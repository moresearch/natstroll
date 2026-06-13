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
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/ollama/ollama/api"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/moresearch/natstroll/internal/shared"
)

// Hub owns the local NATS lab environment.
//
// This program intentionally exercises:
//   - embedded nats-server
//   - JWT operator/account/user authentication
//   - account JWT loading through a resolver directory
//   - hub-issued dynamic user credentials for spokes
//   - hub-created JetStream stream
//   - spoke-created durable pull consumer
//   - scoped heartbeat, request, and response subjects
//
// This is a capability test, not a production security profile. The spoke is
// intentionally granted broad JetStream API access so the demo can verify that
// dynamic credentials can manage JetStream resources.

const (
	OllamaTimeout = 60 * time.Second

	// The hub waits longer than the spoke-side model timeout so a slow but
	// valid local generation does not look like a missing NATS reply.
	SpokeTimeout = OllamaTimeout + 15*time.Second
)

var logger *slog.Logger
var tracer trace.Tracer
var ollamaClient *api.Client
var ollamaModel = "deepseek-r1:1.5b"

var firstSpokeID string
var firstSpokeMu sync.Mutex

func unlimitedJetStreamLimits() jwt.JetStreamLimits {
	// These are account-level JWT limits. Storage, stream count, and consumer
	// count are independent. Setting only memory/disk storage is not enough to
	// make JetStream usable for the account.
	//
	// Use -1 here because this lab tests capabilities, not quotas.
	return jwt.JetStreamLimits{
		MemoryStorage: -1,
		DiskStorage:   -1,
		Streams:       -1,
		Consumer:      -1,
		MaxAckPending: -1,
	}
}

func userCreds(userJWT string, userSeed []byte) (string, error) {
	creds, err := jwt.FormatUserConfig(userJWT, userSeed)
	if err != nil {
		return "", err
	}
	return string(creds), nil
}

func generateAndPrintSecrets() {
	accountKey, err := nkeys.CreateAccount()
	if err != nil {
		shared.Die("failed to create account key: %v", err)
	}

	accountSeed, err := accountKey.Seed()
	if err != nil {
		shared.Die("failed to read account seed: %v", err)
	}

	registrarKey, err := nkeys.CreateUser()
	if err != nil {
		shared.Die("failed to create registrar user key: %v", err)
	}

	registrarPub, err := registrarKey.PublicKey()
	if err != nil {
		shared.Die("failed to read registrar public key: %v", err)
	}

	registrarSeed, err := registrarKey.Seed()
	if err != nil {
		shared.Die("failed to read registrar seed: %v", err)
	}

	// Bootstrap registrar credentials are intentionally narrow. They can
	// publish a registration request and subscribe only to generated _INBOX
	// replies used by NATS request/reply.
	claims := jwt.NewUserClaims(registrarPub)
	claims.Pub.Allow = []string{"reg.request"}
	claims.Sub.Allow = []string{"_INBOX.>"}

	userJWT, err := claims.Encode(accountKey)
	if err != nil {
		shared.Die("failed to encode registrar user JWT: %v", err)
	}

	creds, err := userCreds(userJWT, registrarSeed)
	if err != nil {
		shared.Die("failed to format registrar credentials: %v", err)
	}

	fmt.Println("\n========== COPY THESE EXACTLY ==========")
	fmt.Printf("export NATS_ACCOUNT_SEED=\"%s\"\n", strings.TrimSpace(string(accountSeed)))
	fmt.Printf("export REGISTRAR_CREDS_B64=\"%s\"\n", base64.StdEncoding.EncodeToString([]byte(creds)))
	fmt.Println("========================================")

	os.Exit(0)
}

func startEmbeddedNATS(host string, port int, accountSeed string) (*server.Server, func(), error) {
	dataDir, err := os.MkdirTemp("", "nats-*")
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		_ = os.RemoveAll(dataDir)
	}

	operatorKey, err := nkeys.CreateOperator()
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	operatorPub, err := operatorKey.PublicKey()
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	systemAccountKey, err := nkeys.CreateAccount()
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	systemAccountPub, err := systemAccountKey.PublicKey()
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	operatorClaims := jwt.NewOperatorClaims(operatorPub)
	operatorClaims.Name = "EmbeddedOperator"
	operatorClaims.SystemAccount = systemAccountPub

	operatorJWT, err := operatorClaims.Encode(operatorKey)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	operatorJWTFile := filepath.Join(dataDir, "operator.jwt")
	if err := os.WriteFile(operatorJWTFile, []byte(operatorJWT), 0600); err != nil {
		cleanup()
		return nil, nil, err
	}

	accountKey, err := nkeys.FromSeed([]byte(strings.TrimSpace(accountSeed)))
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("invalid NATS_ACCOUNT_SEED: %w", err)
	}

	accountPub, err := accountKey.PublicKey()
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	// The system account exists only so the resolver can operate correctly.
	// It is preloaded into the same resolver directory as the application
	// account.
	systemAccountClaims := jwt.NewAccountClaims(systemAccountPub)
	systemAccountClaims.Name = "SYS"

	systemAccountJWT, err := systemAccountClaims.Encode(operatorKey)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	// The application account is signed by the operator. User JWTs are signed
	// later by this account key, so the hub keeps NATS_ACCOUNT_SEED.
	accountClaims := jwt.NewAccountClaims(accountPub)
	accountClaims.Name = "Jokes"
	accountClaims.Limits.JetStreamLimits = unlimitedJetStreamLimits()

	accountJWT, err := accountClaims.Encode(operatorKey)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	resolverDir := filepath.Join(dataDir, "jwt")
	if err := os.MkdirAll(resolverDir, 0700); err != nil {
		cleanup()
		return nil, nil, err
	}

	if err := os.WriteFile(filepath.Join(resolverDir, systemAccountPub+".jwt"), []byte(systemAccountJWT), 0600); err != nil {
		cleanup()
		return nil, nil, err
	}

	if err := os.WriteFile(filepath.Join(resolverDir, accountPub+".jwt"), []byte(accountJWT), 0600); err != nil {
		cleanup()
		return nil, nil, err
	}

	// The embedded server runs in JWT/operator mode. The trust chain is:
	//
	//   operator JWT -> account JWT in resolver dir -> user JWT in creds file
	//
	// Full resolver mode requires a system account. JetStream must also be
	// enabled at the server level and allowed at the account level.
	configContent := fmt.Sprintf(`
operator: %q
system_account: %q
resolver: {
  type: full
  dir: %q
}
jetstream {
  store_dir: %q
  max_mem: 512M
  max_file: 2G
}
host: %q
port: %d
`, operatorJWTFile, systemAccountPub, resolverDir, filepath.Join(dataDir, "store"), host, port)

	configFile := filepath.Join(dataDir, "nats.conf")
	if err := os.WriteFile(configFile, []byte(configContent), 0600); err != nil {
		cleanup()
		return nil, nil, err
	}

	opts, err := server.ProcessConfigFile(configFile)
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("failed to process embedded NATS config: %w", err)
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		cleanup()
		return nil, nil, err
	}

	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		cleanup()
		return nil, nil, fmt.Errorf("NATS server not ready on %s:%d; port may already be in use", host, port)
	}

	logger.Info("NATS server started", "host", host, "port", port)
	return ns, cleanup, nil
}

func issueHubCredentials(accountSeed string) (string, error) {
	accountKey, err := nkeys.FromSeed([]byte(strings.TrimSpace(accountSeed)))
	if err != nil {
		return "", err
	}

	userKey, err := nkeys.CreateUser()
	if err != nil {
		return "", err
	}

	userPub, err := userKey.PublicKey()
	if err != nil {
		return "", err
	}

	userSeed, err := userKey.Seed()
	if err != nil {
		return "", err
	}

	// The hub owns this embedded test server, so it receives broad subject
	// access. JetStream capability comes from the account JWT limits; this user
	// JWT only controls subject authorization.
	claims := jwt.NewUserClaims(userPub)
	claims.Expires = time.Now().Add(24 * time.Hour).Unix()
	claims.Pub.Allow = []string{">"}
	claims.Sub.Allow = []string{">"}

	userJWT, err := claims.Encode(accountKey)
	if err != nil {
		return "", err
	}

	return userCreds(userJWT, userSeed)
}

func issueSpokeCredentials(accountSeed, id string) (string, error) {
	if err := shared.ValidateSpokeID(id); err != nil {
		return "", err
	}

	accountKey, err := nkeys.FromSeed([]byte(strings.TrimSpace(accountSeed)))
	if err != nil {
		return "", err
	}

	userKey, err := nkeys.CreateUser()
	if err != nil {
		return "", err
	}

	userPub, err := userKey.PublicKey()
	if err != nil {
		return "", err
	}

	userSeed, err := userKey.Seed()
	if err != nil {
		return "", err
	}

	consumerName := shared.ConsumerNameForSpoke(id)

	// Lab permission: the spoke can publish to $JS.API.> so it can create and
	// bind its own durable pull consumer. That is the capability under test.
	//
	// Production code should narrow this to the exact JetStream API subjects or
	// move consumer provisioning back into the hub.
	//
	// _INBOX.> is required because both NATS request/reply and JetStream API
	// calls receive responses on generated inbox subjects.
	claims := jwt.NewUserClaims(userPub)
	claims.Expires = time.Now().Add(30 * 24 * time.Hour).Unix()
	claims.Pub.Allow = []string{
		"heartbeat." + id,
		shared.JokeResponseSubject + id + ".>",
		"$JS.API.>",
		"$JS.ACK." + shared.JokeStream + "." + consumerName + ".>",
	}
	claims.Sub.Allow = []string{
		"_INBOX.>",
		shared.JokeRequestSubject + id,
	}

	userJWT, err := claims.Encode(accountKey)
	if err != nil {
		return "", err
	}

	return userCreds(userJWT, userSeed)
}

func waitForJetStream(js nats.JetStreamContext) error {
	var lastErr error

	for attempt := 1; attempt <= 20; attempt++ {
		if _, err := js.AccountInfo(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		time.Sleep(250 * time.Millisecond)
	}

	return fmt.Errorf("JetStream did not become ready: %w", lastErr)
}

func ensureJokeStream(js nats.JetStreamContext) error {
	// Store request and response subjects in the same stream so the demo
	// exercises JetStream persistence for the whole exchange. The hub still
	// waits for the immediate reply through a normal core NATS subscription.
	cfg := &nats.StreamConfig{
		Name:     shared.JokeStream,
		Subjects: []string{shared.JokeRequestSubject + ">", shared.JokeResponseSubject + ">"},
		Storage:  nats.FileStorage,
	}

	if _, err := js.StreamInfo(shared.JokeStream); err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			_, err = js.AddStream(cfg)
			return err
		}
		return err
	}

	_, err := js.UpdateStream(cfg)
	return err
}

func generateJoke(ctx context.Context, prompt string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, OllamaTimeout)
	defer cancel()

	ctx, span := tracer.Start(ctx, "hub.generate-joke")
	defer span.End()

	span.SetAttributes(attribute.String("prompt", prompt))

	// Prefix to suppress chain-of-thought from thinking models (deepseek-r1).
	// Without this, the model may spend all num_predict tokens on reasoning
	// and return an empty final answer.
	fullPrompt := fmt.Sprintf("Return only the final answer. Do not think out loud.\n\n%s", prompt)

	req := &api.GenerateRequest{
		Model:  ollamaModel,
		Prompt: fullPrompt,
		Options: map[string]any{
			"temperature": 0.9,
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
		return "", fmt.Errorf("ollama returned an empty reply (model may need more num_predict budget for thinking)")
	}

	return reply, nil
}

func startConversation(ctx context.Context, js nats.JetStreamContext, targetSpokeID string, nc *nats.Conn) {
	logger.Info("starting conversation", "spoke_id", targetSpokeID)

	testResp, err := generateJoke(ctx, "Say hello")
	if err != nil {
		logger.Error("Ollama not working; make sure Ollama is running and model is pulled", "model", ollamaModel, "error", err)
		return
	}
	logger.Info("Ollama test successful", "response", testResp)

	joke, err := generateJoke(ctx, "Tell me a short, funny joke, max 2 sentences.")
	if err != nil {
		logger.Error("failed initial joke", "error", err)
		return
	}
	logger.Info("sending initial joke", "joke", joke)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		requestID := uuid.New().String()

		// Use a spoke-scoped reply subject so the spoke credential only needs
		// access to joke.response.<spokeID>.>, not a global reply namespace.
		replySubject := shared.JokeResponseSubject + targetSpokeID + "." + requestID

		sub, err := nc.SubscribeSync(replySubject)
		if err != nil {
			logger.Error("reply subscribe failed", "error", err)
			return
		}

		if err := nc.Flush(); err != nil {
			_ = sub.Unsubscribe()
			logger.Error("reply subscribe flush failed", "error", err)
			return
		}

		reqData, err := json.Marshal(shared.JokeRequest{RequestID: requestID, Joke: joke})
		if err != nil {
			_ = sub.Unsubscribe()
			logger.Error("failed to marshal joke request", "error", err)
			return
		}

		msg := &nats.Msg{
			Subject: shared.JokeRequestSubject + targetSpokeID,
			Reply:   replySubject,
			Data:    reqData,
			Header:  nats.Header{},
		}
		shared.InjectTraceContext(ctx, msg)

		if _, err := js.PublishMsg(msg); err != nil {
			_ = sub.Unsubscribe()
			logger.Error("publish failed", "error", err)
			return
		}

		replyMsg, err := sub.NextMsg(SpokeTimeout)
		_ = sub.Unsubscribe()
		if err != nil {
			logger.Error("no reply", "error", err)
			return
		}

		replyCtx := shared.ExtractTraceContext(replyMsg)

		var resp shared.JokeResponse
		if err := json.Unmarshal(replyMsg.Data, &resp); err != nil {
			logger.Error("invalid spoke response", "error", err)
			return
		}

		if resp.RequestID != requestID {
			logger.Error("mismatched response request id", "want", requestID, "got", resp.RequestID)
			return
		}

		logger.Info("received reply", "reply", resp.Reply)
		logger.Info("waiting before next joke", "duration", "10s")

		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}

		nextPrompt := fmt.Sprintf("The other AI just said: %q. Now respond with a short, funny comeback or follow-up joke, max 2 sentences.", resp.Reply)

		joke, err = generateJoke(replyCtx, nextPrompt)
		if err != nil {
			logger.Error("failed next joke", "error", err)
			return
		}

		logger.Info("sending next joke", "joke", joke)
	}
}

func main() {
	logger = shared.InitLogger("hub")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	otelEndpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	otelInsecure := strings.ToLower(os.Getenv("OTEL_EXPORTER_OTLP_INSECURE")) != "false"

	var shutdownTracer func(context.Context) error
	var err error
	if otelEndpoint != "" {
		tracer, shutdownTracer, err = shared.InitOpenTelemetry("joke-hub", otelEndpoint, otelInsecure)
		if err != nil {
			logger.Error("OTel init failed", "error", err)
		}
	}
	if tracer == nil {
		// No OTel configured — use the global no-op tracer so calls like
		// tracer.Start() don't panic on a nil interface.
		tracer = otel.Tracer("joke-hub")
		shutdownTracer = func(context.Context) error { return nil }
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := shutdownTracer(shutdownCtx); err != nil {
			logger.Error("OTel shutdown failed", "error", err)
		}
	}()

	accountSeed := os.Getenv("NATS_ACCOUNT_SEED")
	if accountSeed == "" {
		generateAndPrintSecrets()
		return
	}

	ollamaHost := os.Getenv("OLLAMA_HOST")
	if ollamaHost == "" {
		ollamaHost = "http://localhost:11434"
	}

	ollamaURL, err := url.Parse(ollamaHost)
	if err != nil {
		shared.Die("invalid OLLAMA_HOST URL: %v", err)
	}

	ollamaClient = api.NewClient(ollamaURL, http.DefaultClient)
	logger.Info("Ollama client initialized for hub")

	natsServer, cleanupNATS, err := startEmbeddedNATS("0.0.0.0", 4222, accountSeed)
	if err != nil {
		shared.Die("failed to start embedded NATS: %v", err)
	}
	defer cleanupNATS()
	defer natsServer.Shutdown()

	hubCreds, err := issueHubCredentials(accountSeed)
	if err != nil {
		shared.Die("failed to issue hub credentials: %v", err)
	}

	if debugCredsPath := os.Getenv("NATSTROLL_WRITE_HUB_CREDS"); debugCredsPath != "" {
		if err := os.WriteFile(debugCredsPath, []byte(hubCreds), 0600); err != nil {
			shared.Die("failed to write debug hub credentials: %v", err)
		}
		logger.Info("Debug hub creds written", "path", debugCredsPath)
	}

	hubCredsFile, err := shared.WriteTempCreds("hub", []byte(hubCreds))
	if err != nil {
		shared.Die("failed to write hub credentials: %v", err)
	}
	defer os.Remove(hubCredsFile)

	nc, err := nats.Connect(
		"nats://127.0.0.1:4222",
		nats.Name("joke-hub"),
		nats.UserCredentials(hubCredsFile),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			logger.Error("NATS async error", "error", err)
		}),
	)
	if err != nil {
		shared.Die("failed to connect to embedded NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		shared.Die("failed to get JetStream context: %v", err)
	}

	if err := waitForJetStream(js); err != nil {
		shared.Die("%v", err)
	}

	if err := ensureJokeStream(js); err != nil {
		shared.Die("failed to ensure %s: %v", shared.JokeStream, err)
	}
	logger.Info("JOKE_STREAM ready")

	if _, err := nc.Subscribe("heartbeat.>", func(m *nats.Msg) {
		logger.Info("heartbeat received", "subject", m.Subject, "data", string(m.Data))
	}); err != nil {
		shared.Die("failed to subscribe to heartbeats: %v", err)
	}

	var conversationStarted bool

	if _, err := nc.Subscribe("reg.request", func(m *nats.Msg) {
		var req shared.RegisterRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			logger.Error("invalid registration request", "error", err)
			_ = m.Respond([]byte(`{"creds_data":""}`))
			return
		}

		req.SpokeID = strings.TrimSpace(req.SpokeID)
		if err := shared.ValidateSpokeID(req.SpokeID); err != nil {
			logger.Error("invalid registration request", "error", err)
			_ = m.Respond([]byte(`{"creds_data":""}`))
			return
		}

		creds, err := issueSpokeCredentials(accountSeed, req.SpokeID)
		if err != nil {
			logger.Error("failed to issue spoke credentials", "spoke_id", req.SpokeID, "error", err)
			_ = m.Respond([]byte(`{"creds_data":""}`))
			return
		}

		data, err := json.Marshal(shared.RegisterResponse{CredsData: creds})
		if err != nil {
			logger.Error("failed to marshal registration response", "error", err)
			_ = m.Respond([]byte(`{"creds_data":""}`))
			return
		}

		if err := m.Respond(data); err != nil {
			logger.Error("failed to respond to registration", "spoke_id", req.SpokeID, "error", err)
			return
		}

		logger.Info("registered spoke", "spoke_id", req.SpokeID)

		firstSpokeMu.Lock()
		if !conversationStarted {
			conversationStarted = true
			firstSpokeID = req.SpokeID
			logger.Info("first spoke registered; starting conversation", "spoke_id", firstSpokeID)
			go startConversation(ctx, js, firstSpokeID, nc)
		}
		firstSpokeMu.Unlock()
	}); err != nil {
		shared.Die("failed to subscribe to registration requests: %v", err)
	}

	if err := nc.Flush(); err != nil {
		shared.Die("NATS flush failed: %v", err)
	}

	fmt.Println("==========================================")
	fmt.Println("Hub is ready and waiting for spokes...")
	fmt.Println("Spokes will automatically register and then the joke exchange will start.")
	fmt.Println("==========================================")

	<-ctx.Done()
	logger.Info("shutting down hub")
}
