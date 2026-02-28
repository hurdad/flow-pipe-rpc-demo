cmake_minimum_required(VERSION 3.20)
project(flow_pipe LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(yaml-cpp REQUIRED)
find_package(nats REQUIRED)
find_package(opentelemetry-cpp CONFIG REQUIRED)

add_subdirectory(src/stages/transform_stage)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/../third_party/flow-pipe-stage-plugins ${CMAKE_BINARY_DIR}/stage-plugins)

add_executable(flow-pipe
  src/main.cpp
  src/otel.cpp)

target_include_directories(flow-pipe PRIVATE src)
target_link_libraries(flow-pipe PRIVATE
  yaml-cpp
  nats
  transform_stage
  flow_pipe_stage_plugins
  opentelemetry_trace
  opentelemetry_otlp_recordable
  opentelemetry_exporter_otlp_grpc)
