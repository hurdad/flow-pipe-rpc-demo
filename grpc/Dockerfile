FROM ubuntu:24.04 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config ca-certificates git \
    libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
    libnats-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt /src/CMakeLists.txt
COPY proto /src/proto
COPY grpc /src/grpc
COPY third_party /src/third_party

RUN cmake -S /src -B /src/build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_FLOW_PIPE=OFF \
    -DBUILD_GRPC_BUILD=ON && \
    cmake --build /src/build -j --target grpc-client grpc-gateway

FROM ubuntu:24.04 AS client-runtime
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++1.51t64 \
    libprotobuf32 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /src/build/grpc/client/grpc-client /app/grpc-client
ENTRYPOINT ["/app/grpc-client"]

FROM ubuntu:24.04 AS gateway-runtime
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++1.51t64 \
    libprotobuf32 \
    libnats3.7t64 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /src/build/grpc/gateway/grpc-gateway /app/grpc-gateway
EXPOSE 50051
ENTRYPOINT ["/app/grpc-gateway"]
