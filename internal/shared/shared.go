// Package shared contains common types, helpers, and telemetry utilities used
// by both the Natstroll hub and spoke binaries.
package shared

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// NATS subject and stream constants.
const (
	JokeStream          = "JOKE_STREAM"
	JokeRequestSubject  = "joke.request."
	JokeResponseSubject = "joke.response."

	// ServiceVersion is the demo version used in OpenTelemetry resource attributes.
	ServiceVersion = "0.1.0"
)

// JokeRequest is sent by the hub to a spoke.
type JokeRequest struct {
	RequestID   string `json:"request_id"`
	Joke        string `json:"joke"`
	FromSpokeID string `json:"from_spoke_id,omitempty"`
}

// JokeResponse is sent by a spoke back to the hub.
type JokeResponse struct {
	RequestID     string `json:"request_id"`
	Reply         string `json:"reply"`
	ReplyingSpoke string `json:"replying_spoke"`
}

// RegisterRequest is sent by a spoke to register with the hub.
type RegisterRequest struct {
	SpokeID string `json:"spoke_id"`
}

// RegisterResponse carries dynamically issued NATS credentials back to a spoke.
type RegisterResponse struct {
	CredsData string `json:"creds_data"`
}

// Die prints a formatted message to stderr and exits the process.
func Die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// parseLogLevel converts a string like "debug", "info", "warn"/"warning", or "error"
// into the corresponding slog level. Unrecognized values fall back to info.
func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// InitLogger configures the default structured logger at the level specified by
// LOG_LEVEL (default info) and returns a logger that always includes the given
// component name.
//
// When stdout is a terminal, log lines are colorized by level:
//
//	ERROR  red + bold
//	WARN   yellow
//	INFO   cyan
//	DEBUG  gray
//
// When stdout is piped or redirected, color is disabled automatically.
func InitLogger(component string) *slog.Logger {
	level := parseLogLevel(os.Getenv("LOG_LEVEL"))
	handler := NewColorHandler(os.Stdout, &ColorHandlerOptions{
		Level: level,
	})
	logger := slog.New(handler).With("component", component)
	slog.SetDefault(logger)
	return logger
}

// ValidateSpokeID rejects empty or malformed spoke identifiers that would produce
// invalid NATS subject tokens.
func ValidateSpokeID(id string) error {
	if id == "" {
		return fmt.Errorf("spoke id is empty")
	}

	if strings.HasPrefix(id, ".") || strings.HasSuffix(id, ".") || strings.Contains(id, "..") {
		return fmt.Errorf("spoke id %q would create an empty NATS subject token", id)
	}

	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return fmt.Errorf("spoke id %q contains invalid character %q", id, r)
		}
	}

	return nil
}

// SafeName replaces characters that are unsafe in filenames or consumer names.
func SafeName(s string) string {
	replacer := strings.NewReplacer(".", "_", "-", "_", ":", "_", "/", "_", "\\", "_")
	return replacer.Replace(s)
}

// ConsumerNameForSpoke returns the durable consumer name for a given spoke ID.
func ConsumerNameForSpoke(id string) string {
	return "joke_consumer_" + SafeName(id)
}

// WriteTempCreds writes credential data to a temporary file with restrictive
// permissions and returns the file path.
func WriteTempCreds(prefix string, creds []byte) (string, error) {
	f, err := os.CreateTemp("", SafeName(prefix)+"-*.creds")
	if err != nil {
		return "", err
	}
	defer f.Close()

	if _, err := f.Write(creds); err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	if err := f.Chmod(0600); err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}

	return f.Name(), nil
}

// buildResource constructs an OpenTelemetry resource describing this service.
func buildResource(serviceName string) (*sdkresource.Resource, error) {
	hostname, _ := os.Hostname()
	return sdkresource.Merge(
		sdkresource.Default(),
		sdkresource.NewWithAttributes(
			"",
			// Using string literals to avoid a semconv dependency in this demo.
			// Production code typically imports go.opentelemetry.io/otel/semconv.
			attribute.String("service.name", serviceName),
			attribute.String("service.version", ServiceVersion),
			attribute.String("host.name", hostname),
		),
	)
}

// InitOpenTelemetry initializes an OTLP gRPC trace exporter and returns a tracer
// along with a shutdown function. If initialization fails, the returned tracer is
// the global no-op tracer and the shutdown function is a no-op.
func InitOpenTelemetry(serviceName, collectorEndpoint string, insecure bool) (trace.Tracer, func(context.Context) error, error) {
	tracer := otel.Tracer(serviceName)
	noopShutdown := func(context.Context) error { return nil }

	res, err := buildResource(serviceName)
	if err != nil {
		return tracer, noopShutdown, fmt.Errorf("failed to build OTel resource: %w", err)
	}

	ctx := context.Background()
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(collectorEndpoint),
	}
	if insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return tracer, noopShutdown, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Tracer(serviceName), tp.Shutdown, nil
}

// InjectTraceContext injects the current trace context into a NATS message header.
func InjectTraceContext(ctx context.Context, msg *nats.Msg) {
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(msg.Header))
}

// ExtractTraceContext extracts a trace context from a NATS message header.
func ExtractTraceContext(msg *nats.Msg) context.Context {
	return otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(msg.Header))
}
