FROM ubuntu:24.04 AS build

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake ninja-build pkg-config ca-certificates git \
    libyaml-cpp-dev libnats-dev curl && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -S third_party/opentelemetry-cpp -B /tmp/otel-build -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DWITH_OTLP=ON -DWITH_OTLP_GRPC=ON -DBUILD_SHARED_LIBS=ON && \
    cmake --build /tmp/otel-build && cmake --install /tmp/otel-build

RUN cmake -S flow-pipe -B /tmp/flow-build -GNinja -DCMAKE_BUILD_TYPE=Release && \
    cmake --build /tmp/flow-build

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    libyaml-cpp0.8 libnats3.4 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local /usr/local
COPY --from=build /tmp/flow-build/flow-pipe /usr/local/bin/flow-pipe
COPY flow-pipe/config/pipeline.yaml /etc/flow-pipe/pipeline.yaml
ENTRYPOINT ["/usr/local/bin/flow-pipe", "/etc/flow-pipe/pipeline.yaml"]
