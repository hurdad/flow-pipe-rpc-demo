FROM ubuntu:24.04 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config ca-certificates \
    libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
    libnats-dev libopentelemetry-cpp-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt /src/CMakeLists.txt
COPY proto /src/proto
COPY grpc /src/grpc

RUN cmake -S /src -B /src/build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_FLOW_PIPE=OFF \
    -DBUILD_GRPC_BUILD=ON && \
    cmake --build /src/build -j --target grpc-client grpc-gateway

FROM ubuntu:24.04 AS client-runtime
WORKDIR /app
COPY --from=build /src/build/grpc/client/grpc-client /app/grpc-client
ENTRYPOINT ["/app/grpc-client"]

FROM ubuntu:24.04 AS gateway-runtime
WORKDIR /app
COPY --from=build /src/build/grpc/gateway/grpc-gateway /app/grpc-gateway
EXPOSE 50051
ENTRYPOINT ["/app/grpc-gateway"]
