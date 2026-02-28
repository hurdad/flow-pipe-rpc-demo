#pragma once

#include <yaml-cpp/yaml.h>

#include <thread>

struct MessageContext;

class TransformStage {
 public:
  bool init(const YAML::Node &cfg);
  bool process(MessageContext &ctx);
  void shutdown();

 private:
  int sleep_ms_{1000};
};
