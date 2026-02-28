cmake_minimum_required(VERSION 3.20)
project(flow_pipe_transform LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_PREFIX_PATH "/opt/flow-pipe")
find_package(flowpipe REQUIRED)
find_package(Protobuf REQUIRED)
find_package(OpenTelemetry CONFIG REQUIRED)

add_subdirectory(src/stages/transform_stage)
