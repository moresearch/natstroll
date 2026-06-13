package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ollama/ollama/api"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Spoke registers with the hub, receives dynamic NATS user credentials, creates
// a durable JetStream pull consumer for its own request subject, and replies to
// hub-generated joke requests.
//
// The spoke-side consumer creation is intentional. This lab is testing whether
// a dynamically issued credential can exercise JetStream management APIs.

const (
	JokeStream         = "JOKE_STREAM"
	JokeRequestSubject = "joke.request."
	OllamaTimeout      = 600 * time.Second
)

type JokeRequest struct {
	RequestID   string `json:"request_id"`
	Joke        string `json:"joke"`
	FromSpokeID string `json:"from_spoke_id"`
}

type JokeResponse struct {
	RequestID     string `json:"request_id"`
	Reply         string `json:"reply"`
	ReplyingSpoke string `json:"replying_spoke"`
}

type RegisterRequest struct {
	SpokeID string `json:"spoke_id"`
}

type RegisterResponse struct {
	CredsData string `json:"creds_data"`
}

var tracer trace.Tracer
var ollamaClient *api.Client

// var modelName = "deepseek-r1:1.5b"
var modelName = "qwen3.5:0.8b"

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func initLogger() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
}

func initOpenTelemetry(serviceName, collectorEndpoint string) error {
	tracer = otel.Tracer(serviceName)

	ctx := context.Background()
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(collectorEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return err
	}

	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracer = tp.Tracer(serviceName)

	return nil
}

func injectTraceContext(ctx context.Context, msg *nats.Msg) {
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(msg.Header))
}

func extractTraceContext(msg *nats.Msg) context.Context {
	return otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(msg.Header))
}

func validateSpokeID(id string) error {
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

func safeName(s string) string {
	replacer := strings.NewReplacer(".", "_", "-", "_", ":", "_", "/", "_", "\\", "_")
	return replacer.Replace(s)
}

func consumerNameForSpoke(id string) string {
	return "joke_consumer_" + safeName(id)
}

func writeTempCreds(prefix string, creds []byte) (string, error) {
	f, err := os.CreateTemp("", safeName(prefix)+"-*.creds")
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

func isFatalNATSError(err error) bool {
	if err == nil {
		return false
	}

	s := strings.ToLower(err.Error())
	return strings.Contains(s, "permission") ||
		strings.Contains(s, "authorization") ||
		strings.Contains(s, "authentication") ||
		strings.Contains(s, "credentials")
}

func sleepOrDone(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func generateJokeReply(ctx context.Context, joke string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, OllamaTimeout)
	defer cancel()

	ctx, span := tracer.Start(ctx, "spoke.generate-reply")
	defer span.End()

	span.SetAttributes(attribute.String("joke", joke))

	prompt := fmt.Sprintf(`You are a witty AI. The user told you this joke: "%s"
Your task: Consider the received joke carefully. Then craft a response joke. It can be a follow-up joke, a pun, a twist, or a clever comeback that plays on the original joke. Your reply must be short, max 2 sentences, and funny.`, joke)

	req := &api.GenerateRequest{
		Model:   modelName,
		Prompt:  prompt,
		Options: map[string]any{"temperature": 0.5, "num_predict": 80},
		Stream:  nil,
	}

	var response string
	err := ollamaClient.Generate(ctx, req, func(resp api.GenerateResponse) error {
		response += resp.Response
		return nil
	})

	return strings.TrimSpace(response), err
}

func ensureConsumer(ctx context.Context, js nats.JetStreamContext, filterSubject, consumerName string) error {
	// The spoke creates its durable pull consumer on purpose. The hub provisions
	// the stream, but the spoke proves that dynamic credentials can manage a
	// scoped JetStream consumer through $JS.API permissions.
	cfg := &nats.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: filterSubject,
	}

	var lastErr error

	for attempt := 1; attempt <= 5; attempt++ {
		info, err := js.AddConsumer(JokeStream, cfg)
		if err == nil {
			slog.Info("consumer ready", "consumer", info.Name, "filter", filterSubject)
			return nil
		}

		lastErr = err

		info, infoErr := js.ConsumerInfo(JokeStream, consumerName)
		if infoErr == nil {
			if info.Config.FilterSubject != "" && info.Config.FilterSubject != filterSubject {
				return fmt.Errorf("consumer %s exists with filter %q, expected %q", consumerName, info.Config.FilterSubject, filterSubject)
			}
			slog.Info("consumer already exists", "consumer", consumerName, "filter", filterSubject)
			return nil
		}

		if isFatalNATSError(err) {
			return fmt.Errorf("fatal consumer creation error: %w", err)
		}

		slog.Warn("failed to add consumer; retrying", "attempt", attempt, "error", err)

		if err := sleepOrDone(ctx, time.Duration(attempt)*time.Second); err != nil {
			return err
		}
	}

	return fmt.Errorf("failed to add consumer after bounded retries: %w", lastErr)
}

func bindPullConsumer(ctx context.Context, js nats.JetStreamContext, filterSubject, consumerName string) (*nats.Subscription, error) {
	if err := ensureConsumer(ctx, js, filterSubject, consumerName); err != nil {
		return nil, err
	}

	var lastErr error

	for attempt := 1; attempt <= 5; attempt++ {
		sub, err := js.PullSubscribe(
			filterSubject,
			consumerName,
			nats.BindStream(JokeStream),
			nats.ManualAck(),
		)
		if err == nil {
			return sub, nil
		}

		lastErr = err

		if isFatalNATSError(err) {
			return nil, fmt.Errorf("fatal pull subscribe error: %w", err)
		}

		slog.Warn("failed to bind pull consumer; retrying", "attempt", attempt, "error", err)

		if err := sleepOrDone(ctx, time.Duration(attempt)*time.Second); err != nil {
			return nil, err
		}
	}

	return nil, fmt.Errorf("failed to bind pull consumer after bounded retries: %w", lastErr)
}

func processMessage(nc *nats.Conn, spokeID string, msg *nats.Msg) {
	msgCtx := extractTraceContext(msg)

	var jokeReq JokeRequest
	if err := json.Unmarshal(msg.Data, &jokeReq); err != nil {
		slog.Error("failed to unmarshal joke", "error", err)
		_ = msg.Term()
		return
	}

	if strings.TrimSpace(jokeReq.RequestID) == "" {
		slog.Error("invalid joke request: empty request id")
		_ = msg.Term()
		return
	}

	if strings.TrimSpace(msg.Reply) == "" {
		slog.Error("invalid joke request: empty reply subject", "request_id", jokeReq.RequestID)
		_ = msg.Term()
		return
	}

	fmt.Printf("Spoke received joke: %s\n", jokeReq.Joke)

	reply, err := generateJokeReply(msgCtx, jokeReq.Joke)
	if err != nil {
		reply = fmt.Sprintf("Sorry, couldn't reply: %v", err)
	}

	fmt.Printf("Spoke generated reply: %s\n", reply)

	resp := JokeResponse{
		RequestID:     jokeReq.RequestID,
		Reply:         reply,
		ReplyingSpoke: spokeID,
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		slog.Error("failed to marshal joke response", "error", err)
		_ = msg.Term()
		return
	}

	respMsg := &nats.Msg{
		Subject: msg.Reply,
		Data:    respBytes,
		Header:  nats.Header{},
	}
	injectTraceContext(msgCtx, respMsg)

	if err := nc.PublishMsg(respMsg); err != nil {
		slog.Error("failed to send reply", "error", err)
		_ = msg.Nak()
		return
	}

	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		slog.Error("failed to flush reply", "error", err)
		_ = msg.Nak()
		return
	}

	if err := msg.Ack(); err != nil {
		slog.Error("failed to ack message", "error", err)
	}
}

func runConsumer(ctx context.Context, sub *nats.Subscription, nc *nats.Conn, spokeID string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			if isFatalNATSError(err) {
				return fmt.Errorf("fatal fetch error: %w", err)
			}
			slog.Warn("fetch failed; continuing", "error", err)
			continue
		}

		for _, msg := range msgs {
			processMessage(nc, spokeID, msg)
		}
	}
}

func main() {
	initLogger()

	if err := initOpenTelemetry("joke-spoke", "localhost:4317"); err != nil {
		slog.Error("OTel init failed", "error", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	natsURL := os.Getenv("NATS_URL")
	registrarCredsB64 := os.Getenv("REGISTRAR_CREDS_B64")
	spokeID := strings.TrimSpace(os.Getenv("SPOKE_ID"))

	if spokeID == "" {
		host, _ := os.Hostname()
		spokeID = strings.TrimSpace(host)
	}

	if err := validateSpokeID(spokeID); err != nil {
		die("invalid SPOKE_ID: %v", err)
	}

	ollamaHost := os.Getenv("OLLAMA_HOST")
	if ollamaHost == "" {
		ollamaHost = "http://localhost:11434"
	}

	if natsURL == "" || registrarCredsB64 == "" {
		die("missing NATS_URL or REGISTRAR_CREDS_B64")
	}

	registrarCredsData, err := base64.StdEncoding.DecodeString(registrarCredsB64)
	if err != nil {
		die("failed to decode REGISTRAR_CREDS_B64: %v", err)
	}

	registrarCredsFile, err := writeTempCreds("registrar", registrarCredsData)
	if err != nil {
		die("failed to write registrar credentials file: %v", err)
	}
	defer os.Remove(registrarCredsFile)

	fmt.Println("Spoke registering with hub...")

	ncReg, err := nats.Connect(
		natsURL,
		nats.Name("joke-spoke-registrar-"+spokeID),
		nats.UserCredentials(registrarCredsFile),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			slog.Error("registrar NATS async error", "error", err)
		}),
	)
	if err != nil {
		die("registrar connection failed: %v", err)
	}
	defer ncReg.Close()

	regReq := RegisterRequest{SpokeID: spokeID}
	reqData, err := json.Marshal(regReq)
	if err != nil {
		die("failed to marshal registration request: %v", err)
	}

	respMsg, err := ncReg.Request("reg.request", reqData, 5*time.Second)
	if err != nil {
		die("registration request failed: %v", err)
	}

	var regResp RegisterResponse
	if err := json.Unmarshal(respMsg.Data, &regResp); err != nil {
		die("invalid registration response: %v", err)
	}
	if strings.TrimSpace(regResp.CredsData) == "" {
		die("empty credentials received")
	}

	spokeCredsFile, err := writeTempCreds("spoke-"+safeName(spokeID), []byte(regResp.CredsData))
	if err != nil {
		die("failed to write spoke credentials file: %v", err)
	}
	defer os.Remove(spokeCredsFile)

	fmt.Println("Spoke registered and received dynamic credentials")

	nc, err := nats.Connect(
		natsURL,
		nats.Name("joke-spoke-"+spokeID),
		nats.UserCredentials(spokeCredsFile),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			slog.Error("spoke NATS async error", "error", err)
		}),
	)
	if err != nil {
		die("main connection failed: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		die("JetStream context error: %v", err)
	}

	ollamaURL, err := url.Parse(ollamaHost)
	if err != nil {
		die("invalid OLLAMA_HOST: %v", err)
	}
	ollamaClient = api.NewClient(ollamaURL, http.DefaultClient)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := nc.Publish("heartbeat."+spokeID, []byte(`{"status":"alive"}`)); err != nil {
					slog.Error("heartbeat publish failed", "error", err)
					continue
				}
				if err := nc.FlushTimeout(2 * time.Second); err != nil {
					slog.Error("heartbeat flush failed", "error", err)
					continue
				}
				fmt.Println("Heartbeat sent")
			}
		}
	}()

	filterSubject := JokeRequestSubject + spokeID
	consumerName := consumerNameForSpoke(spokeID)

	consumer, err := bindPullConsumer(ctx, js, filterSubject, consumerName)
	if err != nil {
		die("failed to initialize pull consumer: %v", err)
	}

	fmt.Printf("Spoke listening for jokes on %s with durable consumer %s\n", filterSubject, consumerName)

	errCh := make(chan error, 1)
	go func() {
		errCh <- runConsumer(ctx, consumer, nc, spokeID)
	}()

	fmt.Println("Spoke ready; waiting for jokes from hub...")

	select {
	case <-ctx.Done():
		fmt.Println("Shutting down spoke")
	case err := <-errCh:
		if errors.Is(err, context.Canceled) {
			fmt.Println("Shutting down spoke")
			return
		}
		die("spoke stopped: %v", err)
	}
}
