#include <google/protobuf/struct.pb.h>

#include <cstdlib>
#include <string>

#include <natscpp/connection.hpp>
#include <natscpp/error.hpp>

#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"
#include "flowpipe/stage.h"
#include "nats_reply_sink.pb.h"

using namespace flowpipe;

using NatsReplySinkConfig =
    flowpipe::v1::stages::nats::reply::sink::v1::NatsReplySinkConfig;

namespace {
const char* kDefaultNatsUrl = "nats://127.0.0.1:4222";
}  // namespace

class NatsReplySink final : public ISinkStage, public ConfigurableStage {
 public:
  std::string name() const override {
    return "nats_reply_sink";
  }

  NatsReplySink() {
    FP_LOG_INFO("nats_reply_sink constructed");
  }

  ~NatsReplySink() override {
    connection_.reset();
    FP_LOG_INFO("nats_reply_sink destroyed");
  }

  bool configure(const google::protobuf::Struct& config) override {
    NatsReplySinkConfig cfg;
    std::string error;
    if (!ProtobufConfigParser<NatsReplySinkConfig>::Parse(config, &cfg, &error)) {
      FP_LOG_ERROR("nats_reply_sink invalid config: " + error);
      return false;
    }

    if (cfg.subject().empty()) {
      FP_LOG_ERROR("nats_reply_sink requires subject");
      return false;
    }

    const char* env_url = std::getenv("NATS_URL");
    std::string url = cfg.url().empty() ? (env_url ? env_url : kDefaultNatsUrl) : cfg.url();

    try {
      natscpp::connection_options opts;
      opts.url = url;
      connection_ = std::make_unique<natscpp::connection>(opts);
    } catch (const natscpp::nats_error& e) {
      FP_LOG_ERROR("nats_reply_sink setup failed: " + std::string(e.what()));
      return false;
    }

    config_ = std::move(cfg);
    subject_ = config_.subject();

    FP_LOG_INFO("nats_reply_sink configured");
    return true;
  }

  void consume(StageContext& ctx, const Payload& payload) override {
    if (ctx.stop.stop_requested() || payload.empty() || !connection_) {
      return;
    }

    try {
      connection_->publish(subject_,
                           std::string_view(reinterpret_cast<const char*>(payload.data()), payload.size));
    } catch (const natscpp::nats_error& e) {
      FP_LOG_ERROR("nats_reply_sink publish failed: " + std::string(e.what()));
    }
  }

 private:
  NatsReplySinkConfig config_{};
  std::unique_ptr<natscpp::connection> connection_{};
  std::string subject_{};
};

extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating nats_reply_sink stage");
  return new NatsReplySink();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying nats_reply_sink stage");
  delete stage;
}

}  // extern "C"
