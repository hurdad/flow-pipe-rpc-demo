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

    natsStatus s = natsOptions_Create(&opts_);
    if (s != NATS_OK) {
      std::cerr << "GatewayService: natsOptions_Create failed: "
                << natsStatus_GetText(s) << "\n";
      return;
    }

    s = natsOptions_SetURL(opts_, url == nullptr ? "nats://nats:4222" : url);
    if (s != NATS_OK) {
      std::cerr << "GatewayService: natsOptions_SetURL failed: "
                << natsStatus_GetText(s) << "\n";
      return;
    }

    s = natsConnection_Connect(&nc_, opts_);
    if (s != NATS_OK) {
      std::cerr << "GatewayService: natsConnection_Connect failed: "
                << natsStatus_GetText(s) << "\n";
      return;
    }

    s = natsConnection_JetStream(&js_, nc_, nullptr);
    if (s != NATS_OK) {
      std::cerr << "GatewayService: natsConnection_JetStream failed: "
                << natsStatus_GetText(s) << "\n";
    }
  }

  ~GatewayService() override {
    if (js_) jsCtx_Destroy(js_);
    if (nc_) natsConnection_Destroy(nc_);
    if (opts_) natsOptions_Destroy(opts_);
  }

  grpc::Status Run(grpc::ServerContext *context, const RPCRequest *request,
                   RPCResponse *response) override {
    if (!nc_ || !js_) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                          "NATS connection not initialized");
    }

    auto tracer = otel::GetTracer();
    auto span = tracer->StartSpan("grpc.gateway.Run");
    auto scope = tracer->WithActiveSpan(span);

    // Subscribe to the reply subject BEFORE publishing to avoid a race
    // condition where the reply arrives before we start listening.
    natsSubscription *sub = nullptr;
    natsStatus s = natsConnection_SubscribeSync(&sub, nc_, "flow.replies");
    if (s != NATS_OK) {
      span->SetStatus(opentelemetry::trace::StatusCode::kError,
                      "subscribe failed");
      span->End();
      return grpc::Status(grpc::StatusCode::INTERNAL, "subscribe failed");
    }

    auto pub_span = tracer->StartSpan("jetstream.publish");

    natsMsg *msg = nullptr;
    natsMsg_Create(&msg, "flow.jobs", nullptr,
                   request->payload().data(), request->payload().size());

    auto propagator =
        opentelemetry::context::propagation::GlobalTextMapPropagator::
            GetGlobalPropagator();
    class NatsCarrier : public opentelemetry::context::propagation::TextMapCarrier {
    public:
      explicit NatsCarrier(natsMsg *msg) : msg_(msg) {}
      opentelemetry::nostd::string_view Get(
          opentelemetry::nostd::string_view) const noexcept override {
        return "";
      }
      void Set(opentelemetry::nostd::string_view key,
               opentelemetry::nostd::string_view value) noexcept override {
        natsMsgHeader_Set(msg_, std::string(key).c_str(),
                          std::string(value).c_str());
      }
    private:
      natsMsg *msg_;
    } carrier(msg);
    propagator->Inject(carrier,
                       opentelemetry::context::RuntimeContext::GetCurrent());

    jsPubAck *ack = nullptr;
    s = js_PublishMsg(&ack, js_, msg, nullptr, nullptr);
    pub_span->End();

    natsMsg_Destroy(msg);

    if (s != NATS_OK || ack == nullptr) {
      natsSubscription_Destroy(sub);
      span->SetStatus(opentelemetry::trace::StatusCode::kError,
                      "publish failed");
      span->End();
      return grpc::Status(grpc::StatusCode::INTERNAL, "publish failed");
    }

    span->SetAttribute("js.stream", ack->Stream);
    span->SetAttribute("js.seq", static_cast<int64_t>(ack->Sequence));
    jsPubAck_Destroy(ack);

    natsMsg *reply = nullptr;
    s = natsSubscription_NextMsg(&reply, sub, 5000);
    natsSubscription_Destroy(sub);

    if (s != NATS_OK || reply == nullptr) {
      span->SetStatus(opentelemetry::trace::StatusCode::kError,
                      "timeout waiting reply");
      span->End();
      return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
                          "timeout waiting flow-pipe reply");
    }

    int data_len = natsMsg_GetDataLength(reply);
    if (data_len < 0) {
      natsMsg_Destroy(reply);
      span->SetStatus(opentelemetry::trace::StatusCode::kError,
                      "invalid reply length");
      span->End();
      return grpc::Status(grpc::StatusCode::INTERNAL, "invalid reply length");
    }

    response->set_payload(
        std::string(natsMsg_GetData(reply), static_cast<size_t>(data_len)));
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
