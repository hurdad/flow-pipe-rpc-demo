FROM ubuntu:24.04 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config ca-certificates \
    libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
    libopentelemetry-cpp-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt /src/CMakeLists.txt
COPY proto /src/proto
COPY grpc-client /src/grpc-client
RUN cmake -S /src -B /src/build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_FLOW_PIPE=OFF \
    -DBUILD_GRPC_CLIENT=ON \
    -DBUILD_GRPC_GATEWAY=OFF && \
    cmake --build /src/build -j --target grpc-client

FROM build AS runtime
WORKDIR /app
COPY --from=build /src/build/grpc-client/grpc-client /app/grpc-client
ENTRYPOINT ["/app/grpc-client"]
