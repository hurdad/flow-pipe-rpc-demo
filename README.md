# flow-pipe-rpc-demo

Proof-of-concept end-to-end RPC flow:

`gRPC client -> gRPC gateway -> NATS JetStream -> flow-pipe 3-stage pipeline -> gRPC response`

## Included topology

- **gRPC API**: `proto/service.proto`
- **Gateway**: blocking sync gRPC C++20 server publishing JetStream request/reply
- **Worker**: flow-pipe-style pipeline with:
  1. `jetstream_source`
  2. `transform_stage` (`sleep_ms: 1000`)
  3. `jetstream_sink`
- **Tracing**: W3C TraceContext propagation via NATS headers and OTLP export to Jaeger through otel-collector

## Prerequisites

The following local source directories must exist:

- `third_party/opentelemetry-cpp`
- `third_party/flow-pipe-stage-plugins`

No git submodules are used.

## Run

```bash
docker compose up --build
```

Expected one-shot client output includes a processed payload, and Jaeger UI is at:

- http://localhost:16686

Expected trace shape:

- `client.CallRun`
  - `grpc.gateway.Run`
    - `jetstream.publish`
  - `flowpipe.source`
    - `flowpipe.transform`
      - `flowpipe.sink`
