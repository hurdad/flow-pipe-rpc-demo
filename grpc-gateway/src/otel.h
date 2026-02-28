#pragma once

#include <memory>
#include <string>

#include <nats/nats.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/trace/provider.h>

namespace flow::otel {

void InitTracer(const std::string &service_name);
std::shared_ptr<opentelemetry::trace::Tracer> GetTracer();
void InjectToNatsHeaders(natsMsg *msg);
opentelemetry::context::Context ExtractFromNatsHeaders(natsMsg *msg);

}  // namespace flow::otel
