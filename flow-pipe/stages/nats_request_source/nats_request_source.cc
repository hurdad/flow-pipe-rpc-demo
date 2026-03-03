#include <google/protobuf/struct.pb.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>

#include <natscpp/connection.hpp>
#include <natscpp/error.hpp>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"
#include "nats_request_source.pb.h"

using namespace flowpipe;

using NatsRequestSourceConfig =
    flowpipe::v1::stages::nats::request::source::v1::NatsRequestSourceConfig;

namespace {
constexpr int kDefaultPollTimeoutMs = 1000;
const char* kDefaultNatsUrl = "nats://127.0.0.1:4222";
}  // namespace

class NatsRequestSource final : public ISourceStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "nats_request_source";
  }

  NatsRequestSource() {
    FP_LOG_INFO("nats_request_source constructed");
  }

  ~NatsRequestSource() override {
    subscription_.reset();
    connection_.reset();
    FP_LOG_INFO("nats_request_source destroyed");
  }

  bool configure(const google::protobuf::Struct& config) override {
    NatsRequestSourceConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<NatsRequestSourceConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("nats_request_source invalid config: " + error);
      return false;
    }

    if (cfg.subject().empty()) {
      FP_LOG_ERROR("nats_request_source requires subject");
      return false;
    }

    const char* env_url = std::getenv("NATS_URL");
    std::string url = cfg.url().empty() ? (env_url ? env_url : kDefaultNatsUrl) : cfg.url();

    try {
      natscpp::connection_options opts;
      opts.url = url;
      connection_ = std::make_unique<natscpp::connection>(opts);
      subscription_ = std::make_unique<natscpp::subscription>(
          connection_->subscribe_sync(cfg.subject()));
    } catch (const natscpp::nats_error& e) {
      FP_LOG_ERROR("nats_request_source setup failed: " + std::string(e.what()));
      return false;
    }

    config_ = std::move(cfg);
    poll_timeout_ms_ =
        config_.poll_timeout_ms() > 0 ? static_cast<int>(config_.poll_timeout_ms()) : kDefaultPollTimeoutMs;

    FP_LOG_INFO("nats_request_source configured");
    return true;
  }

  bool produce(StageContext& ctx, Payload& payload) override {
    if (ctx.stop.stop_requested()) {
      return false;
    }
    if (!subscription_) {
      FP_LOG_ERROR("nats_request_source subscription not initialized");
      return false;
    }

    natscpp::message message;
    try {
      message = subscription_->next_message(std::chrono::milliseconds(poll_timeout_ms_));
    } catch (const natscpp::nats_error& e) {
      if (e.status() == NATS_TIMEOUT) {
        return false;
      }
      FP_LOG_ERROR("nats_request_source receive failed: " + std::string(e.what()));
      return false;
    }

    std::string_view data = message.data_view();
    auto buffer = AllocatePayloadBuffer(data.size());
    if (!buffer) {
      FP_LOG_ERROR("nats_request_source failed to allocate payload");
      return false;
    }

    if (!data.empty()) {
      std::memcpy(buffer.get(), data.data(), data.size());
    }

    payload = Payload(std::move(buffer), data.size());
    return true;
  }

 private:
  NatsRequestSourceConfig config_{};
  std::unique_ptr<natscpp::connection> connection_{};
  std::unique_ptr<natscpp::subscription> subscription_{};
  int poll_timeout_ms_{kDefaultPollTimeoutMs};
};

extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating nats_request_source stage");
  return new NatsRequestSource();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying nats_request_source stage");
  delete stage;
}

}  // extern "C"
