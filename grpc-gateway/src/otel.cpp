#include "otel.h"

#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/resource/semantic_conventions.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>

namespace nostd = opentelemetry::nostd;
namespace context = opentelemetry::context;
namespace sdktrace = opentelemetry::sdk::trace;
namespace resource = opentelemetry::sdk::resource;

namespace {
class NatsHeaderCarrier : public opentelemetry::context::propagation::TextMapCarrier {
 public:
  explicit NatsHeaderCarrier(natsMsg *msg) : msg_(msg) {}

  nostd::string_view Get(nostd::string_view key) const noexcept override {
    const char *value = natsMsgHeader_Get(msg_, std::string(key).c_str());
    if (value == nullptr) return {};
    return value;
  }

  void Set(nostd::string_view key, nostd::string_view value) noexcept override {
    natsMsgHeader_Set(msg_, std::string(key).c_str(), std::string(value).c_str());
  }

 private:
  natsMsg *msg_;
};
}  // namespace

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
  opentelemetry::context::propagation::GlobalTextMapPropagator::SetGlobalPropagator(
      nostd::shared_ptr<opentelemetry::context::propagation::TextMapPropagator>(
          new opentelemetry::trace::propagation::HttpTraceContext()));
}

std::shared_ptr<opentelemetry::trace::Tracer> GetTracer() {
  return opentelemetry::trace::Provider::GetTracerProvider()->GetTracer("flow-pipe-rpc-demo");
}

void InjectToNatsHeaders(natsMsg *msg) {
  NatsHeaderCarrier carrier(msg);
  auto prop = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
  prop->Inject(carrier, context::RuntimeContext::GetCurrent());
}

context::Context ExtractFromNatsHeaders(natsMsg *msg) {
  NatsHeaderCarrier carrier(msg);
  auto prop = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
  return prop->Extract(carrier, context::RuntimeContext::GetCurrent());
}

}  // namespace flow::otel
