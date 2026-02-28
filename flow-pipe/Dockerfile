FROM ghcr.io/hurdad/flow-pipe-dev:main-ubuntu24.04 AS builder
WORKDIR /src
COPY src/stages/transform_stage /src/stages/transform_stage
COPY CMakeLists.txt /src/CMakeLists.txt
RUN cmake -S /src -B /src/build -DCMAKE_BUILD_TYPE=Release && cmake --build /src/build -j

FROM ghcr.io/hurdad/flow-pipe-runtime:main-ubuntu24.04
WORKDIR /opt/flow-pipe
COPY --from=builder /src/build/libstage_transform_stage.so /opt/flow-pipe/plugins/libstage_transform_stage.so
COPY config/pipeline.yaml /opt/flow-pipe/config/pipeline.yaml
ENTRYPOINT ["/opt/flow-pipe/bin/flow-pipe-runtime", "--config", "/opt/flow-pipe/config/pipeline.yaml"]
