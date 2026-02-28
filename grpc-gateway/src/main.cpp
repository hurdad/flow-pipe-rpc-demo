#include <opentelemetry/trace/scope.h>
#include <grpcpp/grpcpp.h>
#include <nats/nats.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <random>
#include <string>

#include "otel.h"
#include "service.grpc.pb.h"

namespace trace = opentelemetry::trace;

class FlowServiceImpl final : public flow::FlowService::Service {
 public:
  explicit FlowServiceImpl(jsCtx *js) : js_(js) {}

  grpc::Status Run(grpc::ServerContext *context, const flow::FlowRequest *request,
                   flow::FlowResponse *response) override {
    auto tracer = flow::otel::GetTracer();
    auto span = tracer->StartSpan("grpc.gateway.Run");
    trace::Scope scope(span);

    std::string inbox = "_INBOX.flow." + std::to_string(rd_());
    natsSubscription *sub = nullptr;
    natsConnection *conn = js_GetConnection(js_);
    auto s = natsConnection_SubscribeSync(&sub, conn, inbox.c_str());
    if (s != NATS_OK) {
      span->SetStatus(trace::StatusCode::kError, "subscribe failed");
      return grpc::Status(grpc::StatusCode::INTERNAL, natsStatus_GetText(s));
    }

    natsMsg *msg = nullptr;
    s = natsMsg_Create(&msg, "flow.jobs", inbox.c_str(), request->payload().data(),
                       static_cast<int>(request->payload().size()));
    if (s != NATS_OK) {
      natsSubscription_Destroy(sub);
      span->SetStatus(trace::StatusCode::kError, "message create failed");
      return grpc::Status(grpc::StatusCode::INTERNAL, natsStatus_GetText(s));
    }

    flow::otel::InjectToNatsHeaders(msg);
    jsPubAck *ack = nullptr;
    s = js_PublishMsg(&ack, js_, msg, nullptr, nullptr);
    natsMsg_Destroy(msg);
    if (s != NATS_OK) {
      natsSubscription_Destroy(sub);
      span->SetStatus(trace::StatusCode::kError, "publish failed");
      return grpc::Status(grpc::StatusCode::INTERNAL, natsStatus_GetText(s));
    }

    span->SetAttribute("jetstream.stream", ack->Stream);
    span->SetAttribute("jetstream.sequence", static_cast<int64_t>(ack->Sequence));
    jsPubAck_Destroy(ack);

    natsMsg *reply = nullptr;
    int timeout = request->timeout_ms() > 0 ? request->timeout_ms() : 5000;
    s = natsSubscription_NextMsg(&reply, sub, timeout);
    natsSubscription_Destroy(sub);
    if (s != NATS_OK) {
      span->SetStatus(trace::StatusCode::kError, "timeout waiting reply");
      return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "jetstream reply timeout");
    }

    response->set_result(natsMsg_GetData(reply), natsMsg_GetDataLength(reply));
    natsMsg_Destroy(reply);
    span->End();
    return grpc::Status::OK;
  }

 private:
  jsCtx *js_;
  std::mt19937_64 rd_{std::random_device{}()};
};

int main() {
  flow::otel::InitTracer("grpc-gateway");

  const char *nats_url = std::getenv("NATS_URL");
  natsConnection *conn = nullptr;
  natsOptions *opts = nullptr;
  natsOptions_Create(&opts);
  natsOptions_SetURL(opts, nats_url == nullptr ? NATS_DEFAULT_URL : nats_url);
  auto s = natsConnection_Connect(&conn, opts);
  natsOptions_Destroy(opts);
  if (s != NATS_OK) {
    std::cerr << "NATS connect failed: " << natsStatus_GetText(s) << std::endl;
    return 1;
  }

  jsCtx *js = nullptr;
  jsOptions js_opts;
  jsOptions_Init(&js_opts);
  s = natsConnection_JetStream(&js, conn, &js_opts);
  if (s != NATS_OK) {
    std::cerr << "JetStream init failed: " << natsStatus_GetText(s) << std::endl;
    natsConnection_Destroy(conn);
    return 1;
  }

  FlowServiceImpl service(js);
  grpc::ServerBuilder builder;
  builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  std::cout << "grpc-gateway listening on 50051" << std::endl;
  server->Wait();

  jsCtx_Destroy(js);
  natsConnection_Destroy(conn);
  nats_Close();
  return 0;
}
