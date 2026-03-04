#include "otel.h"

#include "service.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include <natscpp/connection.hpp>
#include <natscpp/error.hpp>
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/trace/context.h>
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
    try {
      natscpp::connection_options opts;
      opts.url = url != nullptr ? url : "nats://nats:4222";
      nc_ = std::make_unique<natscpp::connection>(opts);
    } catch (const natscpp::nats_error &e) {
      std::cerr << "GatewayService: connect failed: " << e.what() << "\n";
    }
  }

  grpc::Status Run(grpc::ServerContext *context, const RPCRequest *request,
                   RPCResponse *response) override {
    if (!nc_) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                          "NATS connection not initialized");
    }

    auto tracer = otel::GetTracer();
    auto propagator =
        opentelemetry::context::propagation::GlobalTextMapPropagator::
            GetGlobalPropagator();

    // Extract incoming trace context from gRPC client metadata so the
    // gateway span is a child of the client span.
    class GrpcServerCarrier : public opentelemetry::context::propagation::TextMapCarrier {
    public:
      explicit GrpcServerCarrier(grpc::ServerContext *ctx) : ctx_(ctx) {}
      opentelemetry::nostd::string_view Get(
          opentelemetry::nostd::string_view key) const noexcept override {
        auto it = ctx_->client_metadata().find(
            grpc::string_ref(key.data(), key.size()));
        if (it != ctx_->client_metadata().end())
          return {it->second.data(), it->second.size()};
        return "";
      }
      void Set(opentelemetry::nostd::string_view,
               opentelemetry::nostd::string_view) noexcept override {}
    private:
      grpc::ServerContext *ctx_;
    } server_carrier(context);
    auto parent_ctx = propagator->Extract(
        server_carrier, opentelemetry::context::RuntimeContext::GetCurrent());
    opentelemetry::trace::StartSpanOptions span_opts;
    span_opts.parent = parent_ctx;
    auto span = tracer->StartSpan("grpc.gateway.Run", span_opts);
    auto scope = tracer->WithActiveSpan(span);

    // Generate a unique per-request reply inbox so concurrent requests
    // don't receive each other's replies.
    std::string inbox;
    try {
      inbox = nc_->new_inbox();
    } catch (const natscpp::nats_error &e) {
      span->SetStatus(opentelemetry::trace::StatusCode::kError,
                      "inbox create failed");
      span->End();
      return grpc::Status(grpc::StatusCode::INTERNAL, "inbox create failed");
    }

    // Subscribe to the unique inbox BEFORE publishing to avoid a race
    // condition where the reply arrives before we start listening.
    natscpp::subscription sub;
    try {
      sub = nc_->subscribe_sync(inbox);
    } catch (const natscpp::nats_error &e) {
      span->SetStatus(opentelemetry::trace::StatusCode::kError,
                      "subscribe failed");
      span->End();
      return grpc::Status(grpc::StatusCode::INTERNAL, "subscribe failed");
    }

    auto pub_span = tracer->StartSpan("nats.publish");
    auto pub_scope = tracer->WithActiveSpan(pub_span);

    // Set the inbox as reply-to so the flow-pipe sink routes the response
    // back to this specific request's inbox.
    auto msg = natscpp::message::create(
        "flow.jobs", inbox,
        std::string_view{request->payload().data(), request->payload().size()});

    class NatsCarrier : public opentelemetry::context::propagation::TextMapCarrier {
    public:
      explicit NatsCarrier(natscpp::message &msg) : msg_(msg) {}
      opentelemetry::nostd::string_view Get(
          opentelemetry::nostd::string_view) const noexcept override {
        return "";
      }
      void Set(opentelemetry::nostd::string_view key,
               opentelemetry::nostd::string_view value) noexcept override {
        try {
          msg_.set_header(std::string_view{key.data(), key.size()},
                          std::string_view{value.data(), value.size()});
        } catch (...) {}
      }
    private:
      natscpp::message &msg_;
    } carrier(msg);
    propagator->Inject(carrier,
                       opentelemetry::context::RuntimeContext::GetCurrent());

    try {
      nc_->publish(std::move(msg));
    } catch (const natscpp::nats_error &e) {
      pub_span->End();
      span->SetStatus(opentelemetry::trace::StatusCode::kError,
                      "publish failed");
      span->End();
      return grpc::Status(grpc::StatusCode::INTERNAL, "publish failed");
    }
    pub_span->End();

    natscpp::message reply;
    try {
      reply = sub.next_message(std::chrono::milliseconds(5000));
    } catch (const natscpp::nats_error &e) {
      span->SetStatus(opentelemetry::trace::StatusCode::kError,
                      "timeout waiting reply");
      span->End();
      return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
                          "timeout waiting flow-pipe reply");
    }

    // Link the flow-pipe span propagated back by nats_reply_sink so
    // backends can correlate the pipeline trace with this gateway span.
    std::string reply_traceparent = reply.header("traceparent");
    if (!reply_traceparent.empty()) {
      class ReplyCarrier : public opentelemetry::context::propagation::TextMapCarrier {
      public:
        explicit ReplyCarrier(const std::string &tp) : tp_(tp) {}
        opentelemetry::nostd::string_view Get(
            opentelemetry::nostd::string_view key) const noexcept override {
          if (key == "traceparent") return {tp_.data(), tp_.size()};
          return "";
        }
        void Set(opentelemetry::nostd::string_view,
                 opentelemetry::nostd::string_view) noexcept override {}
      private:
        const std::string &tp_;
      } reply_carrier(reply_traceparent);
      auto reply_ctx = propagator->Extract(
          reply_carrier, opentelemetry::context::Context{});
      auto reply_span_ctx = opentelemetry::trace::GetSpan(reply_ctx)->GetContext();
      if (reply_span_ctx.IsValid()) {
        span->AddLink(reply_span_ctx);
      }
    }

    std::string_view data = reply.data();
    response->set_payload(std::string(data));
    response->set_status("OK");
    response->set_processed_by("transform_stage");

    span->End();
    return grpc::Status::OK;
  }

private:
  std::unique_ptr<natscpp::connection> nc_;
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
  otel::ShutdownTracer();
  return 0;
}
