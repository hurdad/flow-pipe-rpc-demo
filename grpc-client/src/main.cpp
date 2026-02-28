#include <opentelemetry/trace/scope.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "otel.h"
#include "service.grpc.pb.h"

int main() {
  flow::otel::InitTracer("grpc-client");
  auto tracer = flow::otel::GetTracer();
  auto span = tracer->StartSpan("client.CallRun");
  opentelemetry::trace::Scope scope(span);

  std::string target = std::getenv("GATEWAY_ADDR") ? std::getenv("GATEWAY_ADDR") : "localhost:50051";
  auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
  auto stub = flow::FlowService::NewStub(channel);

  flow::FlowRequest req;
  req.set_payload("hello from grpc-client");
  req.set_timeout_ms(10000);

  grpc::ClientContext ctx;
  flow::FlowResponse resp;
  auto status = stub->Run(&ctx, req, &resp);
  if (!status.ok()) {
    std::cerr << "RPC failed: " << status.error_message() << std::endl;
    return 1;
  }

  std::cout << "Result: " << resp.result() << std::endl;
  span->End();
  return 0;
}
