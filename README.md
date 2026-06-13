# Natstroll

Natstroll is a small NATS capability test.

It runs a local hub and one or more spokes. The hub starts an embedded NATS server, enables JetStream, creates a stream, issues dynamic NATS JWT credentials to spokes, and starts a joke exchange. Each spoke registers with the hub, receives dynamic credentials, creates its own durable JetStream pull consumer, receives joke requests, asks Ollama for a reply, and publishes the response back to the hub.

This is a lab/demo, not a production security profile.

## What this tests

* Embedded `nats-server`
* JWT operator/account/user authentication
* Account JWT loading through a resolver directory
* Dynamic user credentials issued by the hub
* JetStream stream creation by the hub
* Durable pull consumer creation by the spoke
* NATS request/reply registration
* Scoped heartbeat, request, and response subjects
* Ollama-backed message generation
* Optional OpenTelemetry trace export

## Layout

```text
natstroll/
  hub/
    main.go
    go.mod
    go.sum
  spoke/
    main.go
    go.mod
    go.sum
```

## Requirements

* Go
* Ollama
* NATS CLI, optional but useful for spying/debugging
* An Ollama model pulled locally

Default model used by the code:

```bash
ollama pull deepseek-r1:1.5b
```

For faster tests, use a non-thinking model or reduce generation with `num_predict`.

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
Ollama client initialized for hub
NATS server started
JOKE_STREAM ready
Hub is ready and waiting for spokes...
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
Spoke registering with hub...
Spoke registered and received dynamic credentials
consumer ready
Spoke listening for jokes on joke.request.black-spoke
Spoke ready; waiting for jokes from hub...
Heartbeat sent
Spoke received joke: ...
Spoke generated reply: ...
```

## Spy on traffic

To inspect traffic from outside, make the hub write debug credentials after `hubCreds` is created:

```go
if err := os.WriteFile("/tmp/natstroll-hub.creds", []byte(hubCreds), 0600); err != nil {
	die("failed to write debug hub credentials: %v", err)
}
fmt.Println("Debug hub creds written to /tmp/natstroll-hub.creds")
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

The code can initialize OTLP tracing against `localhost:4317`.

If no collector is running, you may see:

```text
traces export: exporter export timeout
dial tcp [::1]:4317: connect: connection refused
```

That does not break NATS or Ollama. It only means trace export failed.

Run an OpenTelemetry Collector if you want traces, or make tracing optional through an environment variable.

## Common errors

### `nats: jetstream not enabled`

The embedded server started, but JetStream was not enabled in the processed server config or the account JWT did not allow JetStream.

Check:

* The config uses a `jetstream { ... }` block.
* The config file is loaded with `server.ProcessConfigFile`.
* The account JWT has non-zero JetStream limits for storage, streams, and consumers.

### `using nats based account resolver - the system account needs to be specified`

JWT resolver mode requires a system account.

Check:

* The operator JWT has `SystemAccount` set.
* The system account JWT exists in the resolver directory.
* The config includes `system_account: "<system-account-public-key>"`.

### `context deadline exceeded`

The Ollama request timed out.

Usually this means the model is too slow, especially if using a thinking model.

Fixes:

* Use a faster non-thinking model.
* Increase `OllamaTimeout`.
* Set `num_predict` to a small value.
* Use a shorter prompt.

Recommended Ollama options for this demo:

```go
Options: map[string]any{
	"temperature": 0.7,
	"num_predict": 60,
}
```

### `no servers available for connection`

The spoke cannot reach the hub.

Check:

* The hub is still running.
* The hub started NATS on `127.0.0.1:4222`.
* The spoke has `NATS_URL=nats://127.0.0.1:4222`.

## Security notes

This demo intentionally grants the spoke broad `$JS.API.>` publish access so it can create and bind its own durable pull consumer.

That is useful for testing NATS capabilities, but it is not the right default for production.

Production options:

* Let the hub create consumers.
* Give spokes only exact JetStream API subject permissions.
* Avoid writing debug credentials to `/tmp`.
* Add credential rotation.
* Add stricter stream, consumer, and subject policies.
* Add per-spoke quotas.
