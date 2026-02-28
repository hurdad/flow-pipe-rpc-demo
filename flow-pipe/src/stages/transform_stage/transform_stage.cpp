#include <opentelemetry/trace/scope.h>
#include "transform_stage.h"

#include <chrono>

#include "../../otel.h"

struct MessageContext {
  natsMsg *msg;
};

bool TransformStage::init(const YAML::Node &cfg) {
  if (cfg["sleep_ms"]) {
    sleep_ms_ = cfg["sleep_ms"].as<int>();
  }
  return true;
}

bool TransformStage::process(MessageContext &ctx) {
  auto tracer = flow::otel::GetTracer();
  auto span = tracer->StartSpan("flowpipe.transform");
  opentelemetry::trace::Scope scope(span);

  span->SetAttribute("flow.stage", "transform");
  span->SetAttribute("payload.size", static_cast<int64_t>(natsMsg_GetDataLength(ctx.msg)));

  std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms_));
  return true;
}

void TransformStage::shutdown() {}
