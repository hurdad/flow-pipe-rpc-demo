# flow-pipe-rpc-demo

Proof-of-concept showing synchronous gRPC request/response bridged through NATS core pub/sub and processed by a Flow-Pipe worker.

## Architecture

```text
gRPC client
  -> gRPC gateway (C++20, sync server)
  -> NATS request/reply subjects (no JetStream)
  -> flow-pipe runtime worker (prebuilt runtime image)
       -> nats_request_source
       -> transform_stage (custom plugin)
       -> nats_reply_sink
  -> gRPC response
```

## Key constraints implemented

- C++20 throughout
- gRPC C++ client + sync server
- `nats.c` APIs in gateway
- NATS Flow-Pipe stages implemented with `nats-cpp`
- Flow-Pipe worker uses **prebuilt images only**:
  - Build: `ghcr.io/hurdad/flow-pipe-dev:main-ubuntu24.04`
  - Runtime: `ghcr.io/hurdad/flow-pipe-runtime:main-ubuntu24.04`
- W3C trace propagation over gRPC metadata and NATS headers
- OTLP export to local OpenTelemetry Collector
- Docker Compose runnable stack

## Run

```bash
docker compose build
docker compose up -d
```

Wait for services to become healthy, then run the demo client:

```bash
docker compose run --rm grpc-client
```

Expected response:

- `status: OK`
- payload echoed back from flow-pipe pipeline
- `processed_by: transform_stage`

## Traces

- Jaeger UI: <http://localhost:16686>
- OTLP ingest from services is sent to collector (`otel-collector:4317`) and exported to Jaeger.

Expected trace structure:

```text
client.CallRun
  grpc.gateway.Run
    nats.publish
  flowpipe.source
    flowpipe.transform
      flowpipe.sink
```

## Repo layout

- `proto/service.proto`: RPC contract
- `grpc/gateway/`: sync gRPC server + NATS bridge
- `grpc/client/`: simple caller with trace context injection
- `flow-pipe/`: custom flow-pipe stages + runtime image overlay
- `otel-collector/`: OTLP collector config
