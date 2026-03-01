list(APPEND CMAKE_PREFIX_PATH "/opt/flow-pipe")

find_package(flowpipe REQUIRED)
find_package(Protobuf REQUIRED)

add_library(stage_transform_stage SHARED
  transform_stage.cpp
  register.cpp)

target_include_directories(stage_transform_stage PRIVATE
  /opt/flow-pipe/include
  ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(stage_transform_stage PRIVATE
  flowpipe::flowpipe_runtime
  opentelemetry_trace
  opentelemetry_sdk)

target_compile_features(stage_transform_stage PRIVATE cxx_std_20)

target_compile_options(stage_transform_stage PRIVATE -Wall -Wextra -Wpedantic)

set_target_properties(stage_transform_stage PROPERTIES
  OUTPUT_NAME stage_transform_stage
  PREFIX "lib"
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN YES
  INSTALL_RPATH "$ORIGIN/../lib")
