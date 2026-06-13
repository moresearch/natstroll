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
	"go.opentelemetry.io/otel/trace"

	"github.com/moresearch/natstroll/internal/shared"
)

// Spoke registers with the hub, receives dynamic NATS user credentials, creates
// a durable JetStream pull consumer for its own request subject, and replies to
// hub-generated joke requests.
//
// The spoke-side consumer creation is intentional. This lab is testing whether
// a dynamically issued credential can exercise JetStream management APIs.

const (
	// Keep this below the hub's reply timeout. The hub currently waits
	// OllamaTimeout + 15s, so this spoke should either produce a reply or return
	// a fallback before that window closes.
	OllamaTimeout = 60 * time.Second
)

var logger *slog.Logger
var tracer trace.Tracer = otel.Tracer("joke-spoke")
var ollamaClient *api.Client
var modelName = "deepseek-r1:1.5b"

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

func fallbackReply(joke string) string {
	joke = strings.TrimSpace(joke)
	if joke == "" {
		return "That joke vanished faster than a paper airplane in a storm."
	}

	return "That joke took off about as well as a cat’s paper airplane."
}

func generateJokeReply(ctx context.Context, joke string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, OllamaTimeout)
	defer cancel()

	ctx, span := tracer.Start(ctx, "spoke.generate-reply")
	defer span.End()

	span.SetAttributes(attribute.String("joke", joke))

	// Thinking models may spend tokens on hidden or visible reasoning before
	// producing the final answer. The prompt asks for only final output, and
	// num_predict gives enough budget to reach that final answer.
	prompt := fmt.Sprintf(`Return only the final answer. Do not think out loud.

Reply to this joke with one short funny comeback.

Joke:
%s

Rules:
- max 2 sentences
- no explanation
- no reasoning text
- output only the comeback`, joke)

	req := &api.GenerateRequest{
		Model:  modelName,
		Prompt: prompt,
		Options: map[string]any{
			"temperature": 0.7,
			"num_predict": 256,
		},
		Stream: nil,
	}

	var response string
	err := ollamaClient.Generate(ctx, req, func(resp api.GenerateResponse) error {
		response += resp.Response
		return nil
	})
	if err != nil {
		return "", err
	}

	reply := strings.TrimSpace(response)
	if reply == "" {
		return "", fmt.Errorf("ollama returned an empty reply")
	}

	return reply, nil
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
		info, err := js.AddConsumer(shared.JokeStream, cfg)
		if err == nil {
			logger.Info("consumer ready", "consumer", info.Name, "filter", filterSubject)
			return nil
		}

		lastErr = err

		info, infoErr := js.ConsumerInfo(shared.JokeStream, consumerName)
		if infoErr == nil {
			if info.Config.FilterSubject != "" && info.Config.FilterSubject != filterSubject {
				return fmt.Errorf("consumer %s exists with filter %q, expected %q", consumerName, info.Config.FilterSubject, filterSubject)
			}
			logger.Info("consumer already exists", "consumer", consumerName, "filter", filterSubject)
			return nil
		}

		if isFatalNATSError(err) {
			return fmt.Errorf("fatal consumer creation error: %w", err)
		}

		logger.Warn("failed to add consumer; retrying", "attempt", attempt, "error", err)

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
			nats.BindStream(shared.JokeStream),
			nats.ManualAck(),
		)
		if err == nil {
			return sub, nil
		}

		lastErr = err

		if isFatalNATSError(err) {
			return nil, fmt.Errorf("fatal pull subscribe error: %w", err)
		}

		logger.Warn("failed to bind pull consumer; retrying", "attempt", attempt, "error", err)

		if err := sleepOrDone(ctx, time.Duration(attempt)*time.Second); err != nil {
			return nil, err
		}
	}

	return nil, fmt.Errorf("failed to bind pull consumer after bounded retries: %w", lastErr)
}

func processMessage(nc *nats.Conn, spokeID string, msg *nats.Msg) {
	msgCtx := shared.ExtractTraceContext(msg)

	var jokeReq shared.JokeRequest
	if err := json.Unmarshal(msg.Data, &jokeReq); err != nil {
		logger.Error("failed to unmarshal joke", "error", err)
		_ = msg.Term()
		return
	}

	if strings.TrimSpace(jokeReq.RequestID) == "" {
		logger.Error("invalid joke request: empty request id")
		_ = msg.Term()
		return
	}

	if strings.TrimSpace(msg.Reply) == "" {
		logger.Error("invalid joke request: empty reply subject", "request_id", jokeReq.RequestID)
		_ = msg.Term()
		return
	}

	logger.Info("received joke", "request_id", jokeReq.RequestID, "joke", jokeReq.Joke)

	reply, err := generateJokeReply(msgCtx, jokeReq.Joke)
	if err != nil {
		logger.Warn("ollama reply failed; using deterministic fallback", "request_id", jokeReq.RequestID, "error", err)
		reply = fallbackReply(jokeReq.Joke)
	}

	logger.Info("generated reply", "request_id", jokeReq.RequestID, "reply", reply)

	resp := shared.JokeResponse{
		RequestID:     jokeReq.RequestID,
		Reply:         reply,
		ReplyingSpoke: spokeID,
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		logger.Error("failed to marshal joke response", "request_id", jokeReq.RequestID, "error", err)
		_ = msg.Term()
		return
	}

	respMsg := &nats.Msg{
		Subject: msg.Reply,
		Data:    respBytes,
		Header:  nats.Header{},
	}
	shared.InjectTraceContext(msgCtx, respMsg)

	if err := nc.PublishMsg(respMsg); err != nil {
		logger.Error("failed to send reply", "request_id", jokeReq.RequestID, "error", err)
		_ = msg.Nak()
		return
	}

	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		logger.Error("failed to flush reply", "request_id", jokeReq.RequestID, "error", err)
		_ = msg.Nak()
		return
	}

	if err := msg.Ack(); err != nil {
		logger.Error("failed to ack message", "request_id", jokeReq.RequestID, "error", err)
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
			logger.Warn("fetch failed; continuing", "error", err)
			continue
		}

		for _, msg := range msgs {
			processMessage(nc, spokeID, msg)
		}
	}
}

func main() {
	logger = shared.InitLogger("spoke")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracer := func(context.Context) error { return nil }
	if endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")); endpoint != "" {
		otelInsecure := strings.ToLower(os.Getenv("OTEL_EXPORTER_OTLP_INSECURE")) != "false"
		var err error
		tracer, shutdownTracer, err = shared.InitOpenTelemetry("joke-spoke", endpoint, otelInsecure)
		if err != nil {
			logger.Error("OTel init failed", "error", err)
		}
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := shutdownTracer(shutdownCtx); err != nil {
			logger.Error("OTel shutdown failed", "error", err)
		}
	}()

	natsURL := os.Getenv("NATS_URL")
	registrarCredsB64 := os.Getenv("REGISTRAR_CREDS_B64")
	spokeID := strings.TrimSpace(os.Getenv("SPOKE_ID"))

	if spokeID == "" {
		host, _ := os.Hostname()
		spokeID = strings.TrimSpace(host)
	}

	if err := shared.ValidateSpokeID(spokeID); err != nil {
		shared.Die("invalid SPOKE_ID: %v", err)
	}

	logger = logger.With("spoke_id", spokeID)

	ollamaHost := os.Getenv("OLLAMA_HOST")
	if ollamaHost == "" {
		ollamaHost = "http://localhost:11434"
	}

	if natsURL == "" || registrarCredsB64 == "" {
		shared.Die("missing NATS_URL or REGISTRAR_CREDS_B64")
	}

	registrarCredsData, err := base64.StdEncoding.DecodeString(registrarCredsB64)
	if err != nil {
		shared.Die("failed to decode REGISTRAR_CREDS_B64: %v", err)
	}

	registrarCredsFile, err := shared.WriteTempCreds("registrar", registrarCredsData)
	if err != nil {
		shared.Die("failed to write registrar credentials file: %v", err)
	}
	defer os.Remove(registrarCredsFile)

	logger.Info("registering with hub")

	ncReg, err := nats.Connect(
		natsURL,
		nats.Name("joke-spoke-registrar-"+spokeID),
		nats.UserCredentials(registrarCredsFile),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			logger.Error("registrar NATS async error", "error", err)
		}),
	)
	if err != nil {
		shared.Die("registrar connection failed: %v", err)
	}
	defer ncReg.Close()

	regReq := shared.RegisterRequest{SpokeID: spokeID}
	reqData, err := json.Marshal(regReq)
	if err != nil {
		shared.Die("failed to marshal registration request: %v", err)
	}

	respMsg, err := ncReg.Request("reg.request", reqData, 5*time.Second)
	if err != nil {
		shared.Die("registration request failed: %v", err)
	}

	var regResp shared.RegisterResponse
	if err := json.Unmarshal(respMsg.Data, &regResp); err != nil {
		shared.Die("invalid registration response: %v", err)
	}
	if strings.TrimSpace(regResp.CredsData) == "" {
		shared.Die("empty credentials received")
	}

	spokeCredsFile, err := shared.WriteTempCreds("spoke-"+shared.SafeName(spokeID), []byte(regResp.CredsData))
	if err != nil {
		shared.Die("failed to write spoke credentials file: %v", err)
	}
	defer os.Remove(spokeCredsFile)

	logger.Info("registered and received dynamic credentials")

	nc, err := nats.Connect(
		natsURL,
		nats.Name("joke-spoke-"+spokeID),
		nats.UserCredentials(spokeCredsFile),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			logger.Error("spoke NATS async error", "error", err)
		}),
	)
	if err != nil {
		shared.Die("main connection failed: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		shared.Die("JetStream context error: %v", err)
	}

	ollamaURL, err := url.Parse(ollamaHost)
	if err != nil {
		shared.Die("invalid OLLAMA_HOST: %v", err)
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
					logger.Error("heartbeat publish failed", "error", err)
					continue
				}
				if err := nc.FlushTimeout(2 * time.Second); err != nil {
					logger.Error("heartbeat flush failed", "error", err)
					continue
				}
				logger.Info("heartbeat sent")
			}
		}
	}()

	filterSubject := shared.JokeRequestSubject + spokeID
	consumerName := shared.ConsumerNameForSpoke(spokeID)

	consumer, err := bindPullConsumer(ctx, js, filterSubject, consumerName)
	if err != nil {
		shared.Die("failed to initialize pull consumer: %v", err)
	}

	logger.Info("listening for jokes", "subject", filterSubject, "consumer", consumerName)

	errCh := make(chan error, 1)
	go func() {
		errCh <- runConsumer(ctx, consumer, nc, spokeID)
	}()

	logger.Info("ready; waiting for jokes from hub")

	select {
	case <-ctx.Done():
		logger.Info("shutting down spoke")
	case err := <-errCh:
		if errors.Is(err, context.Canceled) {
			logger.Info("shutting down spoke")
			return
		}
		shared.Die("spoke stopped: %v", err)
	}
}
