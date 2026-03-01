#pragma once

#include <string>

#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/provider.h>

namespace otel {

void InitTracer(const std::string &service_name);
opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> GetTracer();

} // namespace otel
