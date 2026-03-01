#include <google/protobuf/struct.pb.h>
#include <nats/nats.h>

#include <cstdlib>
#include <cstring>
#include <string>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"
#include "nats_jetstream_source.pb.h"

using namespace flowpipe;

using NatsJetStreamSourceConfig =
    flowpipe::v1::stages::nats::jetstream::source::v1::NatsJetStreamSourceConfig;

namespace {
constexpr int kDefaultPollTimeoutMs = 1000;
const char* kDefaultNatsUrl = "nats://127.0.0.1:4222";

std::string StatusToString(natsStatus status) {
  return std::string(natsStatus_GetText(status));
}

std::string BuildNatsError(natsStatus status, jsErrCode js_error_code = static_cast<jsErrCode>(0)) {
  std::string error = StatusToString(status);

  char stack[512] = {};
  if (nats_GetLastErrorStack(stack, sizeof(stack)) == NATS_OK && stack[0] != '\0') {
    error += " | ";
    error += stack;
  }

  if (js_error_code != 0) {
    error += " | js_err_code=";
    error += std::to_string(js_error_code);
  }

  return error;
}
}  // namespace

// ============================================================
// NatsJetStreamSource
// ============================================================
class NatsJetStreamSource final : public ISourceStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "nats_jetstream_source";
  }

  NatsJetStreamSource() {
    FP_LOG_INFO("nats_jetstream_source constructed");
  }

  ~NatsJetStreamSource() override {
    ShutdownConnection();
    FP_LOG_INFO("nats_jetstream_source destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool configure(const google::protobuf::Struct& config) override {
    NatsJetStreamSourceConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<NatsJetStreamSourceConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("nats_jetstream_source invalid config: " + error);
      return false;
    }

    if (cfg.subject().empty()) {
      FP_LOG_ERROR("nats_jetstream_source requires subject");
      return false;
    }

    const char* env_url = std::getenv("NATS_URL");
    std::string url = cfg.url().empty()
                          ? (env_url ? env_url : kDefaultNatsUrl)
                          : cfg.url();
    int poll_timeout_ms =
        cfg.poll_timeout_ms() > 0 ? static_cast<int>(cfg.poll_timeout_ms()) : kDefaultPollTimeoutMs;

    if (!InitializeConnection(url, cfg.subject(), cfg.stream_name(), cfg.durable_name())) {
      return false;
    }

    config_ = std::move(cfg);
    poll_timeout_ms_ = poll_timeout_ms;

    FP_LOG_INFO("nats_jetstream_source configured");
    return true;
  }

  // ------------------------------------------------------------
  // ISourceStage
  // ------------------------------------------------------------
  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      FP_LOG_DEBUG("nats_jetstream_source stop requested, skipping produce");
      return false;
    }

    if (!subscription_) {
      FP_LOG_ERROR("nats_jetstream_source subscription not initialized");
      return false;
    }

    natsMsgList msg_list{};
    jsErrCode fetch_error_code = static_cast<jsErrCode>(0);
    natsStatus status = natsSubscription_Fetch(&msg_list, subscription_, 1, poll_timeout_ms_,
                                               &fetch_error_code);
    if (status == NATS_TIMEOUT || msg_list.Count == 0) {
      natsMsgList_Destroy(&msg_list);
      return false;
    }

    if (status != NATS_OK) {
      natsMsgList_Destroy(&msg_list);
      FP_LOG_ERROR("nats_jetstream_source fetch failed: " +
                   BuildNatsError(status, fetch_error_code));
      return false;
    }

    natsMsg* msg = msg_list.Msgs[0];
    msg_list.Msgs[0] = nullptr;
    natsMsgList_Destroy(&msg_list);

    if (!msg) {
      FP_LOG_ERROR("nats_jetstream_source fetch returned null message");
      return false;
    }

    const char* data = natsMsg_GetData(msg);
    int data_len = natsMsg_GetDataLength(msg);
    if (data_len < 0) {
      natsMsg_Destroy(msg);
      FP_LOG_ERROR("nats_jetstream_source invalid message length");
      return false;
    }

    auto buffer = AllocatePayloadBuffer(static_cast<size_t>(data_len));
    if (!buffer) {
      natsMsg_Destroy(msg);
      FP_LOG_ERROR("nats_jetstream_source failed to allocate payload");
      return false;
    }

    if (data_len > 0 && data) {
      std::memcpy(buffer.get(), data, static_cast<size_t>(data_len));
    }

    payload = Payload(std::move(buffer), static_cast<size_t>(data_len));

    status = natsMsg_Ack(msg, nullptr);
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_jetstream_source ack failed: " + BuildNatsError(status));
    }

    natsMsg_Destroy(msg);
    return true;
  }

 private:
  bool InitializeConnection(const std::string& url, const std::string& subject,
                            const std::string& stream_name, const std::string& durable_name) {
    ShutdownConnection();

    natsConnection* connection = nullptr;
    natsStatus status = natsConnection_ConnectTo(&connection, url.c_str());
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_jetstream_source connect failed: " + StatusToString(status));
      return false;
    }

    jsCtx* jetstream = nullptr;
    status = natsConnection_JetStream(&jetstream, connection, nullptr);
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_jetstream_source jetstream init failed: " + StatusToString(status));
      natsConnection_Destroy(connection);
      return false;
    }

    jsSubOptions sub_options;
    jsSubOptions* options_ptr = nullptr;
    jsSubOptions_Init(&sub_options);

    if (!stream_name.empty()) {
      sub_options.Stream = stream_name.c_str();
      options_ptr = &sub_options;
    }

    if (!durable_name.empty()) {
      sub_options.Config.Durable = durable_name.c_str();
      options_ptr = &sub_options;
    }

    natsSubscription* subscription = nullptr;
    jsErrCode subscribe_error_code = static_cast<jsErrCode>(0);
    const char* durable = durable_name.empty() ? nullptr : durable_name.c_str();
    status = js_PullSubscribe(&subscription, jetstream, subject.c_str(), durable, nullptr,
                              options_ptr, &subscribe_error_code);
    if (status != NATS_OK) {
      FP_LOG_ERROR("nats_jetstream_source subscribe failed: " +
                   BuildNatsError(status, subscribe_error_code));
      jsCtx_Destroy(jetstream);
      natsConnection_Destroy(connection);
      return false;
    }

    connection_ = connection;
    jetstream_ = jetstream;
    subscription_ = subscription;
    return true;
  }

  void ShutdownConnection() {
    if (subscription_) {
      natsSubscription_Destroy(subscription_);
      subscription_ = nullptr;
    }
    if (jetstream_) {
      jsCtx_Destroy(jetstream_);
      jetstream_ = nullptr;
    }
    if (connection_) {
      natsConnection_Destroy(connection_);
      connection_ = nullptr;
    }
  }

  NatsJetStreamSourceConfig config_{};
  natsConnection* connection_{nullptr};
  jsCtx* jetstream_{nullptr};
  natsSubscription* subscription_{nullptr};
  int poll_timeout_ms_{kDefaultPollTimeoutMs};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating nats_jetstream_source stage");
  return new NatsJetStreamSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying nats_jetstream_source stage");
  delete stage;
}

}  // extern "C"
