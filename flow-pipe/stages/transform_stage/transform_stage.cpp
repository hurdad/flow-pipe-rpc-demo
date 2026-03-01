#include "transform_stage.h"

#include "flowpipe/observability/logging.h"

#include <google/protobuf/struct.pb.h>
#include <opentelemetry/trace/provider.h>

#include <chrono>
#include <thread>

std::string TransformStage::name() const { return "transform_stage"; }

bool TransformStage::configure(const google::protobuf::Struct &config) {
  auto it = config.fields().find("sleep_ms");
  if (it != config.fields().end() && it->second.kind_case() == google::protobuf::Value::kNumberValue) {
    cfg_.sleep_ms = static_cast<std::int64_t>(it->second.number_value());
  }
  FP_LOG_INFO("transform_stage configured");
  return true;
}

void TransformStage::process(flowpipe::StageContext &ctx, const flowpipe::Payload &input,
                             flowpipe::Payload &output) {
  if (ctx.stop.stop_requested()) {
    return;
  }

  auto tracer = opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("flow-pipe-runtime");
  auto span = tracer->StartSpan("flowpipe.transform");
  auto scope = tracer->WithActiveSpan(span);

  span->SetAttribute("flow.stage", "transform");
  span->SetAttribute("payload.size", static_cast<int64_t>(input.size));

  std::this_thread::sleep_for(std::chrono::milliseconds(cfg_.sleep_ms));
  output = input;
  span->End();
}
