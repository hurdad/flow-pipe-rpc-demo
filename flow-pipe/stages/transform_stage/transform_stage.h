#pragma once

#include "transform_stage_config.h"

#include "flowpipe/configurable_stage.h"
#include "flowpipe/stage.h"

class TransformStage final : public flowpipe::ITransformStage,
                             public flowpipe::ConfigurableStage {
public:
  std::string name() const override;
  bool configure(const google::protobuf::Struct &config) override;
  void process(flowpipe::StageContext &ctx, const flowpipe::Payload &input,
               flowpipe::Payload &output) override;

private:
  TransformStageConfig cfg_{};
};
