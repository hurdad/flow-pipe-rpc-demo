cmake_minimum_required(VERSION 3.20)
project(grpc_client LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "Dont build Shared Libs" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "Disable tests for dependencies" FORCE)
set(WITH_OTLP_GRPC ON CACHE BOOL "Build OTLP gRPC exporter" FORCE)
set(WITH_OTLP_HTTP ON CACHE BOOL "Build OTLP HTTP exporter" FORCE)
set(WITH_BENCHMARK OFF CACHE BOOL "Disable Benchmarks build" FORCE)
set(WITH_EXAMPLES OFF CACHE BOOL "Disable Examples build" FORCE)
set(WITH_FUNC_TESTS OFF CACHE BOOL "Disable Func tests" FORCE)

if(NOT TARGET opentelemetry_trace)
  add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp
                   ${CMAKE_BINARY_DIR}/third_party/opentelemetry-cpp)
endif()

set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/../proto/service.proto)
get_filename_component(PROTO_PATH ${PROTO_FILE} PATH)

set(PROTO_SRCS ${CMAKE_CURRENT_BINARY_DIR}/service.pb.cc)
set(PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}/service.pb.h)
set(GRPC_SRCS ${CMAKE_CURRENT_BINARY_DIR}/service.grpc.pb.cc)
set(GRPC_HDRS ${CMAKE_CURRENT_BINARY_DIR}/service.grpc.pb.h)

add_custom_command(
  OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
  COMMAND protobuf::protoc
  ARGS --grpc_out ${CMAKE_CURRENT_BINARY_DIR}
       --cpp_out ${CMAKE_CURRENT_BINARY_DIR}
       -I ${PROTO_PATH}
       --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
       ${PROTO_FILE}
  DEPENDS ${PROTO_FILE})

add_executable(grpc-client
  src/main.cpp
  src/otel.cpp
  src/otel.h
  ${PROTO_SRCS}
  ${GRPC_SRCS})

target_include_directories(grpc-client PRIVATE ${CMAKE_CURRENT_BINARY_DIR} src)

target_link_libraries(grpc-client PRIVATE
  gRPC::grpc++
  protobuf::libprotobuf
  opentelemetry_trace
  opentelemetry_resources
  opentelemetry_otlp_recordable
  opentelemetry_otlp_grpc_exporter
  opentelemetry_exporter_ostream_span
  opentelemetry_sdk)
