#include "otel.h"

#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/resource/semantic_conventions.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/provider.h>

namespace nostd = opentelemetry::nostd;
namespace sdktrace = opentelemetry::sdk::trace;
namespace resource = opentelemetry::sdk::resource;

namespace flow::otel {

void InitTracer(const std::string &service_name) {
  opentelemetry::exporter::otlp::OtlpGrpcExporterOptions options;
  options.endpoint = "otel-collector:4317";
  auto exporter = opentelemetry::exporter::otlp::OtlpGrpcExporterFactory::Create(options);
  auto processor = sdktrace::BatchSpanProcessorFactory::Create(std::move(exporter));
  auto attrs = resource::ResourceAttributes{{resource::SemanticConventions::kServiceName, service_name}};
  auto provider = nostd::shared_ptr<opentelemetry::trace::TracerProvider>(
      new sdktrace::TracerProvider(std::move(processor), resource::Resource::Create(attrs)));
  opentelemetry::trace::Provider::SetTracerProvider(provider);
}

std::shared_ptr<opentelemetry::trace::Tracer> GetTracer() {
  return opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("flow-pipe-rpc-demo");
}

}  // namespace flow::otel
