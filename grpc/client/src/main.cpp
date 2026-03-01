#include "otel.h"

#include "service.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/trace/provider.h>

#include <cstdlib>
#include <iostream>
#include <string>

class GrpcMetadataCarrier : public opentelemetry::context::propagation::TextMapCarrier {
public:
  explicit GrpcMetadataCarrier(grpc::ClientContext &ctx) : ctx_(ctx) {}

  opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view) const noexcept override { return ""; }

  void Set(opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) noexcept override {
    ctx_.AddMetadata(std::string(key), std::string(value));
  }

private:
  grpc::ClientContext &ctx_;
};

int main() {
  otel::InitTracer("grpc-client");

  std::string target = std::getenv("GRPC_SERVER") ? std::getenv("GRPC_SERVER") : "localhost:50051";
  auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
  auto stub = flowpipe::rpc::v1::RPCService::NewStub(channel);

  auto tracer = otel::GetTracer();
  auto span = tracer->StartSpan("client.CallRun");
  auto scope = tracer->WithActiveSpan(span);

  grpc::ClientContext ctx;
  auto propagator = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
  GrpcMetadataCarrier carrier(ctx);
  propagator->Inject(carrier, opentelemetry::context::RuntimeContext::GetCurrent());

  flowpipe::rpc::v1::RPCRequest req;
  req.set_payload("hello through flow-pipe");

  flowpipe::rpc::v1::RPCResponse resp;
  auto status = stub->Run(&ctx, req, &resp);

  if (!status.ok()) {
    span->SetStatus(opentelemetry::trace::StatusCode::kError, status.error_message());
    std::cerr << "Run failed: " << status.error_message() << std::endl;
    span->End();
    return 1;
  }

  std::cout << "status=" << resp.status() << " payload='" << resp.payload()
            << "' processed_by=" << resp.processed_by() << std::endl;
  span->End();
  return 0;
}
