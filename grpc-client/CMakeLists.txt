cmake_minimum_required(VERSION 3.20)
project(grpc_client LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(opentelemetry-cpp CONFIG REQUIRED)

set(PROTO_FILE ${CMAKE_CURRENT_LIST_DIR}/../proto/service.proto)
get_filename_component(PROTO_DIR ${PROTO_FILE} DIRECTORY)
set(GENERATED_DIR ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

add_custom_command(
  OUTPUT ${GENERATED_DIR}/service.pb.cc ${GENERATED_DIR}/service.pb.h
         ${GENERATED_DIR}/service.grpc.pb.cc ${GENERATED_DIR}/service.grpc.pb.h
  COMMAND protobuf::protoc
  ARGS --grpc_out=${GENERATED_DIR}
       --cpp_out=${GENERATED_DIR}
       -I${PROTO_DIR}
       --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
       ${PROTO_FILE}
  DEPENDS ${PROTO_FILE})

add_library(flow_proto
  ${GENERATED_DIR}/service.pb.cc
  ${GENERATED_DIR}/service.grpc.pb.cc)
target_include_directories(flow_proto PUBLIC ${GENERATED_DIR})
target_link_libraries(flow_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

add_executable(grpc-client src/main.cpp src/otel.cpp)
target_include_directories(grpc-client PRIVATE src ${GENERATED_DIR})
target_link_libraries(grpc-client PRIVATE flow_proto opentelemetry_trace opentelemetry_otlp_recordable opentelemetry_exporter_otlp_grpc)
