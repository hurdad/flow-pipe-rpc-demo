cmake_minimum_required(VERSION 3.20)
project(grpc_gateway LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(NATS REQUIRED IMPORTED_TARGET libnats)

set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/../../proto/service.proto)
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

add_executable(grpc-gateway
        src/main.cpp
        ../common/otel.cpp
        ${PROTO_SRCS}
        ${GRPC_SRCS})

target_include_directories(grpc-gateway PRIVATE ${CMAKE_CURRENT_BINARY_DIR} src ../common)

target_link_libraries(grpc-gateway PRIVATE
        gRPC::grpc++
        protobuf::libprotobuf
        PkgConfig::NATS
        opentelemetry_trace
        opentelemetry_exporter_otlp_grpc)
