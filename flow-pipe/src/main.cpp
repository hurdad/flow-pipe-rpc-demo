#include <opentelemetry/trace/scope.h>
#include <yaml-cpp/yaml.h>

#include <chrono>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <cstring>

#include "otel.h"
#include "stages/transform_stage/transform_stage.h"

struct MessageContext {
  natsMsg *msg{nullptr};
};

class Stage {
 public:
  virtual ~Stage() = default;
  virtual bool init(const YAML::Node &cfg) = 0;
  virtual bool process(MessageContext &ctx) = 0;
  virtual void shutdown() = 0;
};

natsConnection *g_conn = nullptr;

class JetStreamSourceStage final : public Stage {
 public:
  bool init(const YAML::Node &) override {
    natsOptions *opts = nullptr;
    natsOptions_Create(&opts);
    natsOptions_SetURL(opts, std::getenv("NATS_URL") ? std::getenv("NATS_URL") : NATS_DEFAULT_URL);
    if (natsConnection_Connect(&conn_, opts) != NATS_OK) return false;
    g_conn = conn_;
    natsOptions_Destroy(opts);

    jsOptions jso;
    jsOptions_Init(&jso);
    if (natsConnection_JetStream(&js_, conn_, &jso) != NATS_OK) return false;

    jsSubOptions so;
    jsSubOptions_Init(&so);
    so.Stream = "FLOW";
    so.Consumer = "FLOW_PIPE";
    so.ManualAck = true;
    return js_PullSubscribe(&sub_, js_, "flow.jobs", "FLOW_PIPE", &so, nullptr, nullptr) == NATS_OK;
  }

  bool process(MessageContext &ctx) override {
    auto tracer = flow::otel::GetTracer();
    auto span = tracer->StartSpan("flowpipe.source");
    opentelemetry::trace::Scope scope(span);

    natsMsgList list;
    natsMsgList_Init(&list);
    if (natsSubscription_Fetch(&list, sub_, 1, 1000, nullptr) != NATS_OK || list.Count == 0) {
      return false;
    }

    ctx.msg = list.Msgs[0];
    return true;
  }

  void shutdown() override {
    if (sub_ != nullptr) natsSubscription_Destroy(sub_);
    if (js_ != nullptr) jsCtx_Destroy(js_);
    if (conn_ != nullptr) natsConnection_Destroy(conn_);
  }

 private:
  natsConnection *conn_{nullptr};
  jsCtx *js_{nullptr};
  natsSubscription *sub_{nullptr};
};

class JetStreamSinkStage final : public Stage {
 public:
  bool init(const YAML::Node &) override { return true; }

  bool process(MessageContext &ctx) override {
    auto tracer = flow::otel::GetTracer();
    auto span = tracer->StartSpan("flowpipe.sink");
    opentelemetry::trace::Scope scope(span);

    const char *reply = natsMsg_GetReply(ctx.msg);
    if (reply != nullptr && std::strlen(reply) > 0) {
      natsMsg *out = nullptr;
      natsMsg_Create(&out, reply, nullptr, natsMsg_GetData(ctx.msg), natsMsg_GetDataLength(ctx.msg));
      natsConnection_PublishMsg(g_conn, out);
      natsMsg_Destroy(out);
    }
    natsMsg_Ack(ctx.msg, nullptr);
    natsMsg_Destroy(ctx.msg);
    ctx.msg = nullptr;
    return true;
  }

  void shutdown() override {}
};

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cerr << "usage: flow-pipe <pipeline.yaml>" << std::endl;
    return 1;
  }
  flow::otel::InitTracer("flow-pipe");

  YAML::Node cfg = YAML::LoadFile(argv[1]);
  TransformStage transform;
  JetStreamSourceStage source;
  JetStreamSinkStage sink;

  if (!source.init(cfg["stages"][0]["config"]) || !transform.init(cfg["stages"][1]["config"]) ||
      !sink.init(cfg["stages"][2]["config"])) {
    std::cerr << "pipeline init failed" << std::endl;
    return 1;
  }

  while (true) {
    MessageContext ctx;
    if (!source.process(ctx)) continue;
    if (!transform.process(ctx)) continue;
    sink.process(ctx);
  }

  sink.shutdown();
  transform.shutdown();
  source.shutdown();
  return 0;
}
