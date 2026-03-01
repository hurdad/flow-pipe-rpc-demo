#include "transform_stage.h"

#include "flowpipe/observability/logging.h"
#include "flowpipe/plugin.h"

extern "C" {

FLOWPIPE_PLUGIN_API
flowpipe::IStage *flowpipe_create_stage() {
  FP_LOG_INFO("creating transform_stage");
  return new TransformStage();
}

FLOWPIPE_PLUGIN_API
void flowpipe_destroy_stage(flowpipe::IStage *stage) {
  FP_LOG_INFO("destroying transform_stage");
  delete stage;
}

} // extern "C"
