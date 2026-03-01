cmake_minimum_required(VERSION 3.20)
project(flow_pipe_rpc_demo LANGUAGES CXX)

option(BUILD_FLOW_PIPE "Build flow-pipe plugin" ON)
option(BUILD_GRPC "Build grpc-client and grpc-gateway" ON)

# otel-cpp opts
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Dont build Shared Libs" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "Disable tests for dependencies" FORCE)
set(WITH_OTLP_GRPC ON CACHE BOOL "Build OTLP gRPC exporter" FORCE)
set(WITH_OTLP_HTTP OFF CACHE BOOL "Build OTLP HTTP exporter" FORCE)
set(WITH_BENCHMARK OFF CACHE BOOL "Disable Benchmarks build" FORCE)
set(WITH_EXAMPLES OFF CACHE BOOL "Disable Examples build" FORCE)
set(WITH_FUNC_TESTS OFF CACHE BOOL "Disable Func tests" FORCE)

if(BUILD_FLOW_PIPE)
  add_subdirectory(flow-pipe)
endif()

if(BUILD_GRPC)
  add_subdirectory(third_party/opentelemetry-cpp)
  add_subdirectory(grpc/client)
  add_subdirectory(grpc/gateway)
endif()

