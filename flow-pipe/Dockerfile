FROM ghcr.io/hurdad/flow-pipe-dev:main-ubuntu24.04 AS builder
WORKDIR /src
COPY CMakeLists.txt /src/CMakeLists.txt
COPY flow-pipe /src/flow-pipe
RUN cmake -S /src -B /src/build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_FLOW_PIPE=ON \
    -DBUILD_GRPC_BUILD=OFF && \
    cmake --build /src/build -j

FROM ghcr.io/hurdad/flow-pipe-runtime:main-ubuntu24.04

COPY --from=builder \
  /opt/flow-pipe/plugins/*.so \
  /opt/flow-pipe/plugins/

COPY flow-pipe/flow/pipeline.yaml /opt/flow-pipe/flow/pipeline.yaml
ENTRYPOINT ["/opt/flow-pipe/bin/flow-pipe-runtime", "--config", "/opt/flow-pipe/flow/pipeline.yaml"]
