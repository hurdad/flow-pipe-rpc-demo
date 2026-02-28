#pragma once

#include <memory>
#include <string>

#include <opentelemetry/trace/tracer.h>

namespace flow::otel {

void InitTracer(const std::string &service_name);
std::shared_ptr<opentelemetry::trace::Tracer> GetTracer();

}  // namespace flow::otel
