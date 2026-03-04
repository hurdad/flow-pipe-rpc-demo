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

static bool hex_nibble(char c, uint8_t& out) noexcept {
  if (c >= '0' && c <= '9') { out = static_cast<uint8_t>(c - '0'); return true; }
  if (c >= 'a' && c <= 'f') { out = static_cast<uint8_t>(c - 'a' + 10); return true; }
  if (c >= 'A' && c <= 'F') { out = static_cast<uint8_t>(c - 'A' + 10); return true; }
  return false;
}

static bool hex_decode(const char* src, uint8_t* dst, size_t n) noexcept {
  for (size_t i = 0; i < n; ++i) {
    uint8_t hi{}, lo{};
    if (!hex_nibble(src[i * 2], hi) || !hex_nibble(src[i * 2 + 1], lo)) return false;
    dst[i] = static_cast<uint8_t>((hi << 4) | lo);
  }
  return true;
}

// Parse W3C traceparent header: "00-<32hex trace-id>-<16hex parent-id>-<2hex flags>"
static flowpipe::PayloadMeta parse_traceparent(const natscpp::message& msg) noexcept {
  flowpipe::PayloadMeta meta;
  std::string tp = msg.header("traceparent");
  // Minimum valid length: 2 + 1 + 32 + 1 + 16 + 1 + 2 = 55
  if (tp.size() < 55 || tp[2] != '-' || tp[35] != '-' || tp[52] != '-') return meta;
  if (!hex_decode(tp.data() + 3, meta.trace_id, 16)) return meta;
  if (!hex_decode(tp.data() + 36, meta.span_id, 8)) return meta;
  uint8_t flags_byte = 0;
  if (hex_decode(tp.data() + 53, &flags_byte, 1)) meta.flags = flags_byte;
  return meta;
}
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
    while (true) {
      if (ctx.stop.stop_requested()) {
        return false;
      }
      try {
        message = subscription_->next_message(std::chrono::milliseconds(poll_timeout_ms_));
        break;
      } catch (const natscpp::nats_error& e) {
        if (e.status() == NATS_TIMEOUT) {
          continue;
        }
        FP_LOG_ERROR("nats_request_source receive failed: " + std::string(e.what()));
        return false;
      }
    }

    std::string_view data = message.data();
    auto buffer = AllocatePayloadBuffer(data.size());
    if (!buffer) {
      FP_LOG_ERROR("nats_request_source failed to allocate payload");
      return false;
    }

    if (!data.empty()) {
      std::memcpy(buffer.get(), data.data(), data.size());
    }

    payload = Payload(std::move(buffer), data.size(), parse_traceparent(message));
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
