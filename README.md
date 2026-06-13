# Natstroll

Natstroll is a small NATS capability test — a hub-and-spoke joke exchange powered by Ollama.

A **hub** runs an embedded NATS server with JetStream, issues dynamic JWT credentials to connecting spokes, and orchestrates an AI-powered joke conversation. Each **spoke** registers with the hub, receives scoped dynamic credentials, creates its own durable JetStream pull consumer, receives joke requests, asks Ollama for a reply, and publishes the response back.

This is a lab/demo, not a production security profile.

## Table of contents

- [What this tests](#what-this-tests)
- [How it works](#how-it-works)
- [Layout](#layout)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Environment variables](#environment-variables)
- [Building](#building)
- [Running tests](#running-tests)
- [Start the hub](#start-the-hub)
- [Start a spoke](#start-a-spoke)
- [Multiple spokes](#multiple-spokes)
- [Spy on traffic](#spy-on-traffic)
- [Live subject spy](#live-subject-spy)
- [OpenTelemetry](#opentelemetry)
- [Common errors](#common-errors)
- [Security notes](#security-notes)

## What this tests

- Embedded `nats-server`
- JWT operator/account/user authentication
- Account JWT loading through a resolver directory
- Dynamic user credentials issued by the hub
- JetStream stream creation by the hub
- Durable pull consumer creation by the spoke
- NATS request/reply registration
- Scoped heartbeat, request, and response subjects
- Ollama-backed message generation
- Optional OpenTelemetry trace export

## How it works

```
┌──────────────────────────────────────────────────┐
│                      Hub                          │
│  ┌──────────────┐  ┌──────────┐  ┌────────────┐  │
│  │ Embedded     │  │ JWT      │  │ Ollama     │  │
│  │ NATS Server  │  │ Issuer   │  │ Client     │  │
│  │ (JetStream)  │  │          │  │            │  │
│  └──────┬───────┘  └────┬─────┘  └─────┬──────┘  │
│         │               │               │         │
└─────────┼───────────────┼───────────────┼─────────┘
          │               │               │
    ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
    │ NATS      │   │ Dynamic   │   │ Ollama    │
    │ Messages  │   │ Creds     │   │ Replies   │
    └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
          │               │               │
┌─────────┼───────────────┼───────────────┼─────────┐
│         │               │               │         │
│  ┌──────▼───────┐  ┌────▼─────┐  ┌─────▼──────┐  │
│  │ JetStream    │  │ JWT      │  │ Ollama     │  │
│  │ Consumer     │  │ Auth     │  │ Client     │  │
│  └──────────────┘  └──────────┘  └────────────┘  │
│                      Spoke                         │
└───────────────────────────────────────────────────┘
```

1. **Bootstrap**: On first run, the hub generates an account seed and narrow-scoped registrar credentials.
2. **Registration**: Each spoke connects with registrar creds, sends its `SPOKE_ID`, and receives dynamically issued user credentials scoped to its identity.
3. **Credential upgrade**: The spoke drops the registrar connection and reconnects with its new dynamic credentials, proving they work.
4. **Stream setup**: The hub creates `JOKE_STREAM` on JetStream covering both request and response subjects.
5. **Consumer setup**: The spoke creates its own durable pull consumer on `JOKE_STREAM`, filtered to `joke.request.<spokeID>`.
6. **Conversation loop** (with the first registered spoke): The hub generates a joke via Ollama, publishes it via JetStream with a reply subject, waits up to 75s for the spoke's Ollama-generated reply, then generates a follow-up joke incorporating the reply — and repeats.
7. **Heartbeats**: Each spoke publishes a heartbeat every 10 seconds on `heartbeat.<spokeID>`.

## Layout

```text
natstroll/
  shared/
    shared.go          — common types, helpers, OpenTelemetry setup
    shared_test.go     — tests for shared package
    go.mod / go.sum
  hub/
    main.go            — embedded NATS server, JWT issuer, joke orchestrator
    go.mod / go.sum
  spoke/
    main.go            — registration, JetStream consumer, Ollama reply generation
    go.mod / go.sum
  .gitignore
  README.md
```

Each component is an independent Go module. Hub and spoke both reference `shared` via a `replace` directive in their `go.mod` files.

## Requirements

- **Go** 1.26+
- **Ollama** running locally
- An Ollama model pulled:

```bash
ollama pull deepseek-r1:1.5b
```

For faster tests, use a non-thinking model or reduce generation with `num_predict`.

- **NATS CLI** — optional but useful for spying and debugging

## Quick start

In one terminal, bootstrap and start the hub:

```bash
cd hub

# Generate secrets (first time only)
unset NATS_ACCOUNT_SEED REGISTRAR_CREDS_B64
go run main.go
# → copy the two export lines printed

# Start the hub with those secrets
export NATS_ACCOUNT_SEED="..."
export REGISTRAR_CREDS_B64="..."
go run main.go
```

In another terminal, start a spoke:

```bash
cd spoke
export NATS_URL=nats://127.0.0.1:4222
export REGISTRAR_CREDS_B64="..."  # paste the value from the hub
export SPOKE_ID=black-spoke
go run main.go
```

The hub detects the first spoke and starts exchanging jokes. You'll see joke requests and AI-generated replies in both terminals.

## Environment variables

| Variable | Applies to | Default | Description |
|----------|------------|---------|-------------|
| `LOG_LEVEL` | hub, spoke | `info` | Log level: `debug`, `info`, `warn`, `error`. |
| `OLLAMA_HOST` | hub, spoke | `http://localhost:11434` | Ollama API base URL. |
| `NATS_ACCOUNT_SEED` | hub | *(none)* | Account seed for signing dynamic user credentials. |
| `NATSTROLL_WRITE_HUB_CREDS` | hub | *(none)* | If set, write hub credentials to this path for debugging. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | hub, spoke | hub: `localhost:4317` | OTLP collector endpoint. If unset in the spoke, OTel is disabled. |
| `OTEL_EXPORTER_OTLP_INSECURE` | hub, spoke | `true` | Use insecure gRPC when `true`. Set to `false` for production collectors with TLS. |
| `REGISTRAR_CREDS_B64` | spoke | *(none)* | Base64-encoded registrar credentials from the hub's first-run output. |
| `SPOKE_ID` | spoke | hostname | Unique identifier for this spoke. |
| `NATS_URL` | spoke | *(none)* | URL of the NATS server started by the hub. |

## Building

Build standalone binaries from each module directory:

```bash
cd hub && go build -o hub
cd spoke && go build -o spoke
```

The binaries can then be run directly:

```bash
cd hub && ./hub
cd spoke && ./spoke
```

Pre-built binaries may already exist at `hub/hub` and `spoke/spoke`.

## Running tests

```bash
cd shared && go test ./...
```

Tests cover the shared package: log level parsing, spoke ID validation, safe-name generation, consumer name helpers, temporary credential file writing, and JSON round-trips for all message types.

## Start the hub

First generate fresh NATS bootstrap credentials:

```bash
cd hub
unset NATS_ACCOUNT_SEED REGISTRAR_CREDS_B64
go run main.go
```

The hub prints two export lines:

```bash
export NATS_ACCOUNT_SEED="..."
export REGISTRAR_CREDS_B64="..."
```

Paste those exports into the same terminal, then start the hub:

```bash
go run main.go
```

Expected output:

```text
time=... level=INFO component=hub msg="Ollama client initialized for hub"
time=... level=INFO component=hub msg="NATS server started" host=0.0.0.0 port=4222
time=... level=INFO component=hub msg="JOKE_STREAM ready"
Hub is ready and waiting for spokes...
Spokes will automatically register and then the joke exchange will start.
==========================================
```

## Start a spoke

Open another terminal:

```bash
cd spoke
export NATS_URL=nats://127.0.0.1:4222
export REGISTRAR_CREDS_B64="PASTE_THE_VALUE_PRINTED_BY_THE_HUB"
export SPOKE_ID=black-spoke
go run main.go
```

Expected output:

```text
time=... level=INFO component=spoke spoke_id=black-spoke msg="registering with hub"
time=... level=INFO component=spoke spoke_id=black-spoke msg="registered and received dynamic credentials"
time=... level=INFO component=spoke spoke_id=black-spoke consumer=joke_consumer_black_spoke filter="joke.request.black-spoke" msg="consumer ready"
time=... level=INFO component=spoke spoke_id=black-spoke subject="joke.request.black-spoke" consumer=joke_consumer_black_spoke msg="listening for jokes"
time=... level=INFO component=spoke spoke_id=black-spoke msg="ready; waiting for jokes from hub"
time=... level=INFO component=spoke spoke_id=black-spoke msg="heartbeat sent"
time=... level=INFO component=spoke spoke_id=black-spoke request_id=... joke="..." msg="received joke"
time=... level=INFO component=spoke spoke_id=black-spoke request_id=... reply="..." msg="generated reply"
```

## Multiple spokes

You can start additional spokes alongside the first one. Each spoke registers independently, receives its own scoped credentials, creates its own durable consumer, and sends heartbeats. Additional spokes show up in the hub's heartbeat logs.

Only the **first** spoke to register participates in the joke conversation loop. This is by design — the demo focuses on credential issuance and consumer setup for each spoke rather than multi-spoke conversation routing.

To start a second spoke:

```bash
cd spoke
export NATS_URL=nats://127.0.0.1:4222
export REGISTRAR_CREDS_B64="..."  # same registrar creds
export SPOKE_ID=red-spoke          # different ID
go run main.go
```

## Spy on traffic

To inspect traffic from outside, tell the hub to write debug credentials by setting `NATSTROLL_WRITE_HUB_CREDS` to the path you want:

```bash
cd hub
export NATSTROLL_WRITE_HUB_CREDS=/tmp/natstroll-hub.creds
# ... also export NATS_ACCOUNT_SEED and REGISTRAR_CREDS_B64
go run main.go
```

Then use one command to create a separate spy consumer and continuously read stored messages:

```bash
NATS_URL=nats://127.0.0.1:4222 NATS_CREDS=/tmp/natstroll-hub.creds bash -lc 'nats --server "$NATS_URL" --creds "$NATS_CREDS" consumer info JOKE_STREAM spy_all >/dev/null 2>&1 || nats --server "$NATS_URL" --creds "$NATS_CREDS" consumer add JOKE_STREAM spy_all --filter "joke.>" --pull --ack explicit --deliver all --replay instant --defaults; while true; do nats --server "$NATS_URL" --creds "$NATS_CREDS" consumer next JOKE_STREAM spy_all --count 10 --wait 5s || true; sleep 1; done'
```

This does not steal messages from the spoke because it uses a separate consumer named `spy_all`.

## Live subject spy

For live-only traffic:

```bash
nats --server nats://127.0.0.1:4222 --creds /tmp/natstroll-hub.creds sub 'joke.>'
```

For heartbeats:

```bash
nats --server nats://127.0.0.1:4222 --creds /tmp/natstroll-hub.creds sub 'heartbeat.>'
```

## OpenTelemetry

Both binaries can export OTLP traces. The hub defaults to `localhost:4317`; the spoke only initializes OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

Resource attributes include `service.name`, `service.version`, and `host.name`. Trace context is propagated through NATS message headers, linking hub and spoke spans.

If no collector is running, you may see:

```text
time=... level=ERROR component=hub msg="OTel init failed" error="..."
```

That does not break NATS or Ollama. It only means trace export failed.

To run with a local collector:

```bash
# hub
export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317
export OTEL_EXPORTER_OTLP_INSECURE=true
cd hub && go run main.go

# spoke
export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317
export OTEL_EXPORTER_OTLP_INSECURE=true
cd spoke && go run main.go
```

## Common errors

### `nats: jetstream not enabled`

The embedded server started, but JetStream was not enabled in the processed server config or the account JWT did not allow JetStream.

Check:

- The config uses a `jetstream { ... }` block.
- The config file is loaded with `server.ProcessConfigFile`.
- The account JWT has non-zero JetStream limits for storage, streams, and consumers.

### `using nats based account resolver - the system account needs to be specified`

JWT resolver mode requires a system account.

Check:

- The operator JWT has `SystemAccount` set.
- The system account JWT exists in the resolver directory.
- The config includes `system_account: "<system-account-public-key>"`.

### `context deadline exceeded`

The Ollama request timed out (hub: 60s, spoke: 60s). The hub waits an additional 15s for the spoke's reply (75s total).

Usually this means the model is too slow, especially if using a thinking model.

Fixes:

- Use a faster non-thinking model.
- Set `num_predict` to a smaller value in the source.
- Use a shorter prompt.

Default Ollama options used by this demo:

| Component | `temperature` | `num_predict` |
|-----------|---------------|---------------|
| Hub       | 0.9           | 60            |
| Spoke     | 0.7           | 256           |

### `no servers available for connection`

The spoke cannot reach the hub.

Check:

- The hub is still running.
- The hub started NATS on `127.0.0.1:4222`.
- The spoke has `NATS_URL=nats://127.0.0.1:4222`.

## Security notes

This demo intentionally grants the spoke broad `$JS.API.>` publish access so it can create and bind its own durable pull consumer.

That is useful for testing NATS capabilities, but it is not the right default for production.

Production options:

- Let the hub create consumers.
- Give spokes only exact JetStream API subject permissions.
- Avoid writing debug credentials to `/tmp`; only enable `NATSTROLL_WRITE_HUB_CREDS` when actively debugging.
- Add credential rotation.
- Add stricter stream, consumer, and subject policies.
- Add per-spoke quotas.
