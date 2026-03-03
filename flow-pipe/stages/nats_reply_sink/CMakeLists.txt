cmake_minimum_required(VERSION 3.20)

project(stage_nats_reply_sink LANGUAGES CXX)

list(APPEND CMAKE_PREFIX_PATH "/opt/flow-pipe")

find_package(flowpipe REQUIRED)
find_package(Protobuf REQUIRED)

set(PROTO_FILES
        nats_reply_sink.proto
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(stage_nats_reply_sink_proto STATIC
        ${PROTO_SRCS}
        ${PROTO_HDRS}
)

set_target_properties(stage_nats_reply_sink_proto PROPERTIES
        POSITION_INDEPENDENT_CODE ON
)

target_link_libraries(stage_nats_reply_sink_proto
        PUBLIC protobuf::libprotobuf
)

target_include_directories(stage_nats_reply_sink_proto
        PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_features(stage_nats_reply_sink_proto
        PUBLIC cxx_std_20
)

add_library(stage_nats_reply_sink SHARED
        nats_reply_sink.cc
)

target_include_directories(stage_nats_reply_sink
        PRIVATE
        /opt/flow-pipe/include
)

target_link_libraries(stage_nats_reply_sink
        PRIVATE
        flowpipe::flowpipe_runtime
        stage_nats_reply_sink_proto
        natscpp::natscpp
)

target_compile_features(stage_nats_reply_sink
        PRIVATE
        cxx_std_20
)

target_compile_options(stage_nats_reply_sink
        PRIVATE
        -Wall -Wextra -Wpedantic
)

set_target_properties(stage_nats_reply_sink PROPERTIES
        OUTPUT_NAME stage_nats_reply_sink
        PREFIX "lib"
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN YES
        INSTALL_RPATH "$ORIGIN/../lib"
)

install(TARGETS stage_nats_reply_sink
        LIBRARY DESTINATION /opt/flow-pipe/plugins
)
