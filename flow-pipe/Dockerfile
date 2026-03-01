FROM ghcr.io/hurdad/flow-pipe-dev:main-ubuntu24.04 AS builder
WORKDIR /src
COPY CMakeLists.txt /src/CMakeLists.txt
COPY flow-pipe /src/flow-pipe
RUN cmake -S /src -B /src/build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_FLOW_PIPE=ON \
    -DBUILD_GRPC_CLIENT=OFF \
    -DBUILD_GRPC_GATEWAY=OFF && \
    cmake --build /src/build -j

FROM ghcr.io/hurdad/flow-pipe-runtime:main-ubuntu24.04
WORKDIR /opt/flow-pipe
COPY --from=builder /src/build/flow-pipe/src/stages/transform_stage/libstage_transform_stage.so /opt/flow-pipe/plugins/libstage_transform_stage.so
COPY flow-pipe/config/pipeline.yaml /opt/flow-pipe/config/pipeline.yaml
ENTRYPOINT ["/opt/flow-pipe/bin/flow-pipe-runtime", "--config", "/opt/flow-pipe/config/pipeline.yaml"]
