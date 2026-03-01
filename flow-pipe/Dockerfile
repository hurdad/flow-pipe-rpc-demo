FROM ghcr.io/hurdad/flow-pipe-dev:main-ubuntu24.04 AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    libnats-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt /src/CMakeLists.txt
COPY flow-pipe /src/flow-pipe
RUN cmake -S /src -B /src/build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_FLOW_PIPE=ON \
    -DBUILD_GRPC_BUILD=OFF \
    && cmake --build /src/build -j \
    && cmake --install build

FROM ghcr.io/hurdad/flow-pipe-runtime:main-ubuntu24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    libnats3.7t64 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder \
  /opt/flow-pipe/plugins/*.so \
  /opt/flow-pipe/plugins/

COPY flow-pipe/flows/*.yaml /opt/flow-pipe/flows/

