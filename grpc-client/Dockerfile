FROM ubuntu:24.04 AS build

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake ninja-build pkg-config git ca-certificates \
    libprotobuf-dev protobuf-compiler-grpc libgrpc++-dev libgrpc-dev \
    curl && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -S third_party/opentelemetry-cpp -B /tmp/otel-build -GNinja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DWITH_OTLP=ON -DWITH_OTLP_GRPC=ON -DBUILD_SHARED_LIBS=ON && \
    cmake --build /tmp/otel-build && cmake --install /tmp/otel-build

RUN cmake -S grpc-client -B /tmp/client-build -GNinja -DCMAKE_BUILD_TYPE=Release && \
    cmake --build /tmp/client-build

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++1.51 libprotobuf32 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local /usr/local
COPY --from=build /tmp/client-build/grpc-client /usr/local/bin/grpc-client
ENTRYPOINT ["/usr/local/bin/grpc-client"]
