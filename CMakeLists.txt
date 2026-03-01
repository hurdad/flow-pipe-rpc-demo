cmake_minimum_required(VERSION 3.20)
project(flow_pipe_rpc_demo LANGUAGES CXX)

option(BUILD_FLOW_PIPE "Build flow-pipe plugin" ON)
option(BUILD_GRPC_CLIENT "Build grpc-client" ON)
option(BUILD_GRPC_GATEWAY "Build grpc-gateway" ON)

if(BUILD_FLOW_PIPE)
  add_subdirectory(flow-pipe)
endif()

if(BUILD_GRPC_CLIENT)
  add_subdirectory(grpc-client)
endif()

if(BUILD_GRPC_GATEWAY)
  add_subdirectory(grpc-gateway)
endif()
