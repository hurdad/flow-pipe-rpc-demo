cmake_minimum_required(VERSION 3.20)
project(flow_pipe_transform LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(stages/rpc_transform)
add_subdirectory(stages/nats_jetstream_sink)
add_subdirectory(stages/nats_jetstream_source)
