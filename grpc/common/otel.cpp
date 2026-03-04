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

// Held so ShutdownTracer() can flush and release buffered spans.
static std::shared_ptr<trace_sdk::TracerProvider> g_sdk_provider;

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
  g_sdk_provider = std::make_shared<trace_sdk::TracerProvider>(
      std::move(processor), resource::Resource::Create(attrs));
  trace::Provider::SetTracerProvider(
      opentelemetry::nostd::shared_ptr<trace::TracerProvider>(
          std::static_pointer_cast<trace::TracerProvider>(g_sdk_provider)));
}

opentelemetry::nostd::shared_ptr<trace::Tracer> GetTracer() {
  auto provider = trace::Provider::GetTracerProvider();
  return provider->GetTracer("flow-pipe-rpc-demo", OPENTELEMETRY_SDK_VERSION);
}

void ShutdownTracer() {
  if (g_sdk_provider) {
    g_sdk_provider->Shutdown();
    g_sdk_provider.reset();
  }
}

} // namespace otel
