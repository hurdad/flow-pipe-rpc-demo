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
COPY grpc-gateway /src/grpc-gateway
RUN cmake -S /src -B /src/build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_FLOW_PIPE=OFF \
    -DBUILD_GRPC_CLIENT=OFF \
    -DBUILD_GRPC_GATEWAY=ON && \
    cmake --build /src/build -j --target grpc-gateway

FROM build AS runtime
WORKDIR /app
COPY --from=build /src/build/grpc-gateway/grpc-gateway /app/grpc-gateway
EXPOSE 50051
ENTRYPOINT ["/app/grpc-gateway"]
