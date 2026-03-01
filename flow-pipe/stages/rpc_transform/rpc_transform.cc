#include <thread>
#include <chrono>

#include "flowpipe/stage.h"
#include "flowpipe/configurable_stage.h"
#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"
#include "flowpipe/protobuf_config.h"

#include "rpc_transform.pb.h"

#include <google/protobuf/struct.pb.h>

#include <cstdlib>
#include <cstring>

using namespace flowpipe;

using RPCTransformConfig =
    flowpipe::stages::rpc::v1::RPCTransformConfig;

// ============================================================
// RPCTransform
// ============================================================
class RPCTransform final
    : public ITransformStage,
      public ConfigurableStage {
public:
  std::string name() const override {
    return "rpc";
  }

  RPCTransform() {
    FP_LOG_INFO("rpc_transform constructed");
  }

  ~RPCTransform() override {
    FP_LOG_INFO("rpc_transform destroyed");
  }

  // ------------------------------------------------------------
  // ConfigurableStage
  // ------------------------------------------------------------
  bool configure(const google::protobuf::Struct& config) override {
    RPCTransformConfig cfg;
    std::string error;

    if (!ProtobufConfigParser<RPCTransformConfig>::Parse(config, &cfg,
                                                                &error)) {
      FP_LOG_ERROR("rpc_transform invalid config: " + error);
      return false;
    }

    config_ = std::move(cfg);
    FP_LOG_INFO("rpc_transform configured");
    return true;
  }

  // ------------------------------------------------------------
  // ITransformStage
  // ------------------------------------------------------------
  void process(StageContext& ctx,
               const Payload& input,
               Payload& output) override {
    if (ctx.stop.stop_requested()) {
      return;
    }

    const size_t size = input.size;

    // Simulate work
    std::this_thread::sleep_for(std::chrono::milliseconds(config_.processing_delay_ms()));

    // ----------------------------------------------------------
    // Allocate new payload for output
    // ----------------------------------------------------------
    auto buffer = AllocatePayloadBuffer(size);
    if (!buffer) {
      FP_LOG_ERROR("rpc_transform failed to allocate payload");
      return;
    }

    // get access to input data
    const uint8_t* src = input.data();
    uint8_t* dst = static_cast<uint8_t*>(buffer.get());

    // convert input string uppercase
    for (size_t i = 0; i < size; ++i) {
      dst[i] = static_cast<uint8_t>(
          std::toupper(static_cast<unsigned char>(src[i])));
    }

    // build output
    output = Payload(std::move(buffer), size);
  }

private:
  RPCTransformConfig config_{};
};

// ============================================================
// Plugin entry points
// ============================================================
extern "C" {

FLOWPIPE_PLUGIN_API
IStage* flowpipe_create_stage() {
  FP_LOG_INFO("creating rpc_transform stage");
  return new RPCTransform();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(IStage* stage) {
  FP_LOG_INFO("destroying rpc_transform stage");
  delete stage;
}

}  // extern "C"
