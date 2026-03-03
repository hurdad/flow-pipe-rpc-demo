cmake_minimum_required(VERSION 3.20)

project(stage_nats_request_source LANGUAGES CXX)

list(APPEND CMAKE_PREFIX_PATH "/opt/flow-pipe")

find_package(flowpipe REQUIRED)
find_package(Protobuf REQUIRED)

set(PROTO_FILES
        nats_request_source.proto
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(stage_nats_request_source_proto STATIC
        ${PROTO_SRCS}
        ${PROTO_HDRS}
)

set_target_properties(stage_nats_request_source_proto PROPERTIES
        POSITION_INDEPENDENT_CODE ON
)

target_link_libraries(stage_nats_request_source_proto
        PUBLIC protobuf::libprotobuf
)

target_include_directories(stage_nats_request_source_proto
        PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_features(stage_nats_request_source_proto
        PUBLIC cxx_std_20
)

add_library(stage_nats_request_source SHARED
        nats_request_source.cc
)

target_include_directories(stage_nats_request_source
        PRIVATE
        /opt/flow-pipe/include
)

target_link_libraries(stage_nats_request_source
        PRIVATE
        flowpipe::flowpipe_runtime
        stage_nats_request_source_proto
        natscpp::natscpp
)

target_compile_features(stage_nats_request_source
        PRIVATE
        cxx_std_20
)

target_compile_options(stage_nats_request_source
        PRIVATE
        -Wall -Wextra -Wpedantic
)

set_target_properties(stage_nats_request_source PROPERTIES
        OUTPUT_NAME stage_nats_request_source
        PREFIX "lib"
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN YES
        INSTALL_RPATH "$ORIGIN/../lib"
)

install(TARGETS stage_nats_request_source
        LIBRARY DESTINATION /opt/flow-pipe/plugins
)
