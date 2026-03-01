#include "otel.h"

#include <cstdlib>

#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>

namespace otel {
namespace trace_sdk = opentelemetry::sdk::trace;
namespace resource = opentelemetry::sdk::resource;
namespace trace = opentelemetry::trace;

void InitTracer(const std::string &service_name) {
  opentelemetry::exporter::otlp::OtlpGrpcExporterOptions opts;
  const char *endpoint = std::getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
  if (endpoint != nullptr) {
    opts.endpoint = endpoint;
  }

  auto exporter = opentelemetry::exporter::otlp::OtlpGrpcExporterFactory::Create(opts);
  auto processor = std::make_unique<trace_sdk::BatchSpanProcessor>(std::move(exporter),
                                                                    trace_sdk::BatchSpanProcessorOptions{});

  auto attrs = resource::ResourceAttributes{{"service.name", service_name}};
  auto provider = opentelemetry::nostd::shared_ptr<trace::TracerProvider>(
      new trace_sdk::TracerProvider(std::move(processor), resource::Resource::Create(attrs)));
  trace::Provider::SetTracerProvider(provider);
}

opentelemetry::nostd::shared_ptr<trace::Tracer> GetTracer() {
  auto provider = trace::Provider::GetTracerProvider();
  return provider->GetTracer("flow-pipe-rpc-demo", OPENTELEMETRY_SDK_VERSION);
}

} // namespace otel
