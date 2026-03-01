#include "otel.h"

#include "service.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <nats/nats.h>
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/trace/provider.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

using flowpipe::rpc::v1::RPCService;
using flowpipe::rpc::v1::RPCRequest;
using flowpipe::rpc::v1::RPCResponse;

class GatewayService final : public RPCService::Service {
public:
  GatewayService() {
    const char *url = std::getenv("NATS_URL");
    natsOptions_Create(&opts_);
    natsOptions_SetURL(opts_, url == nullptr ? "nats://nats:4222" : url);
    natsConnection_Connect(&nc_, opts_);
    natsConnection_JetStream(&js_, nc_, nullptr);
  }

  ~GatewayService() override {
    jsCtx_Destroy(js_);
    natsConnection_Destroy(nc_);
    natsOptions_Destroy(opts_);
  }

  grpc::Status Run(grpc::ServerContext *context, const RPCRequest *request,
                   RPCResponse *response) override {
    auto tracer = otel::GetTracer();
    auto span = tracer->StartSpan("grpc.gateway.Run");
    auto scope = tracer->WithActiveSpan(span);

    natsStatus s;
    natsInbox *inbox = nullptr;
    s = natsInbox_Create(&inbox);
    if (s != NATS_OK) {
      span->SetStatus(opentelemetry::trace::StatusCode::kError, "inbox create failed");
      return grpc::Status(grpc::StatusCode::INTERNAL, "inbox create failed");
    }

    natsSubscription *sub = nullptr;
    s = natsConnection_SubscribeSync(&sub, nc_, inbox);
    if (s != NATS_OK) {
      natsInbox_Destroy(inbox);
      span->SetStatus(opentelemetry::trace::StatusCode::kError, "subscribe failed");
      return grpc::Status(grpc::StatusCode::INTERNAL, "subscribe failed");
    }

    auto pub_span = tracer->StartSpan("jetstream.publish");

    natsMsg *msg = nullptr;
    natsMsg_Create(&msg, "flow.jobs", inbox, request->payload().data(), request->payload().size());

    auto propagator = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    class NatsCarrier : public opentelemetry::context::propagation::TextMapCarrier {
    public:
      explicit NatsCarrier(natsMsg *msg) : msg_(msg) {}
      opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view key) const noexcept override { return ""; }
      void Set(opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) noexcept override {
        natsMsgHeader_Set(msg_, std::string(key).c_str(), std::string(value).c_str());
      }
    private:
      natsMsg *msg_;
    } carrier(msg);
    propagator->Inject(carrier, opentelemetry::context::RuntimeContext::GetCurrent());

    jsPubAck *ack = nullptr;
    s = js_PublishMsg(&ack, js_, msg, nullptr, nullptr);
    pub_span->End();

    if (s != NATS_OK || ack == nullptr) {
      natsMsg_Destroy(msg);
      natsSubscription_Destroy(sub);
      natsInbox_Destroy(inbox);
      span->SetStatus(opentelemetry::trace::StatusCode::kError, "publish failed");
      return grpc::Status(grpc::StatusCode::INTERNAL, "publish failed");
    }

    span->SetAttribute("js.stream", ack->Stream);
    span->SetAttribute("js.seq", static_cast<int64_t>(ack->Sequence));

    natsMsg *reply = nullptr;
    s = natsSubscription_NextMsg(&reply, sub, 5000);

    jsPubAck_Destroy(ack);
    natsMsg_Destroy(msg);
    natsSubscription_Destroy(sub);
    natsInbox_Destroy(inbox);

    if (s != NATS_OK || reply == nullptr) {
      span->SetStatus(opentelemetry::trace::StatusCode::kError, "timeout waiting reply");
      return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "timeout waiting flow-pipe reply");
    }

    response->set_payload(std::string(natsMsg_GetData(reply), natsMsg_GetDataLength(reply)));
    response->set_status("OK");
    response->set_processed_by("transform_stage");

    natsMsg_Destroy(reply);
    span->End();
    return grpc::Status::OK;
  }

private:
  natsOptions *opts_{};
  natsConnection *nc_{};
  jsCtx *js_{};
};

int main() {
  otel::InitTracer("grpc-gateway");

  GatewayService service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "grpc-gateway listening on 0.0.0.0:50051" << std::endl;
  server->Wait();
  return 0;
}
