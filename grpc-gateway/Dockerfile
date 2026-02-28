FROM ubuntu:24.04 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config ca-certificates \
    libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
    libnats-dev libopentelemetry-cpp-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY proto /src/proto
COPY grpc-gateway /src/grpc-gateway
WORKDIR /src/grpc-gateway
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j

FROM build AS runtime
WORKDIR /app
COPY --from=build /src/grpc-gateway/build/grpc-gateway /app/grpc-gateway
EXPOSE 50051
ENTRYPOINT ["/app/grpc-gateway"]
