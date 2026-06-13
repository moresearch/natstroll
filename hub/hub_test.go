package main

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func TestUnlimitedJetStreamLimits(t *testing.T) {
	limits := unlimitedJetStreamLimits()

	if limits.MemoryStorage != -1 {
		t.Errorf("MemoryStorage = %d, want -1", limits.MemoryStorage)
	}
	if limits.DiskStorage != -1 {
		t.Errorf("DiskStorage = %d, want -1", limits.DiskStorage)
	}
	if limits.Streams != -1 {
		t.Errorf("Streams = %d, want -1", limits.Streams)
	}
	if limits.Consumer != -1 {
		t.Errorf("Consumer = %d, want -1", limits.Consumer)
	}
	if limits.MaxAckPending != -1 {
		t.Errorf("MaxAckPending = %d, want -1", limits.MaxAckPending)
	}
}

func TestUserCreds(t *testing.T) {
	// User JWTs must be signed by an account key.
	accountKey, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}

	userKey, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}
	userPub, err := userKey.PublicKey()
	if err != nil {
		t.Fatalf("PublicKey failed: %v", err)
	}
	userSeed, err := userKey.Seed()
	if err != nil {
		t.Fatalf("Seed failed: %v", err)
	}

	claims := jwt.NewUserClaims(userPub)
	userJWT, err := claims.Encode(accountKey)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	creds, err := userCreds(userJWT, userSeed)
	if err != nil {
		t.Fatalf("userCreds failed: %v", err)
	}

	if !strings.Contains(creds, "BEGIN NATS USER JWT") {
		t.Error("creds should contain JWT begin marker")
	}
	if !strings.Contains(creds, "BEGIN USER NKEY SEED") {
		t.Error("creds should contain seed begin marker")
	}
}

func TestUserCredsInvalid(t *testing.T) {
	_, err := userCreds("not-a-jwt", []byte("not-a-seed"))
	if err == nil {
		t.Error("userCreds with invalid inputs should return an error")
	}
}

func TestIssueHubCredentials(t *testing.T) {
	accountKey, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	accountSeed, err := accountKey.Seed()
	if err != nil {
		t.Fatalf("account Seed failed: %v", err)
	}

	creds, err := issueHubCredentials(string(accountSeed))
	if err != nil {
		t.Fatalf("issueHubCredentials failed: %v", err)
	}

	if !strings.Contains(creds, "BEGIN NATS USER JWT") {
		t.Error("hub creds should contain JWT begin marker")
	}
	if !strings.Contains(creds, "BEGIN USER NKEY SEED") {
		t.Error("hub creds should contain seed begin marker")
	}

	// Should fail with an invalid seed.
	_, err = issueHubCredentials("not-a-valid-seed")
	if err == nil {
		t.Error("issueHubCredentials with invalid seed should return an error")
	}
}

func TestIssueSpokeCredentials(t *testing.T) {
	accountKey, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	accountSeed, err := accountKey.Seed()
	if err != nil {
		t.Fatalf("account Seed failed: %v", err)
	}

	creds, err := issueSpokeCredentials(string(accountSeed), "test-spoke")
	if err != nil {
		t.Fatalf("issueSpokeCredentials failed: %v", err)
	}

	if !strings.Contains(creds, "BEGIN NATS USER JWT") {
		t.Error("spoke creds should contain JWT begin marker")
	}
	if !strings.Contains(creds, "BEGIN USER NKEY SEED") {
		t.Error("spoke creds should contain seed begin marker")
	}

	// Should fail with invalid spoke ID.
	_, err = issueSpokeCredentials(string(accountSeed), "")
	if err == nil {
		t.Error("issueSpokeCredentials with empty ID should return an error")
	}

	// Should fail with invalid seed.
	_, err = issueSpokeCredentials("bad-seed", "test-spoke")
	if err == nil {
		t.Error("issueSpokeCredentials with invalid seed should return an error")
	}
}

func TestIssueSpokeCredentialsSubjectScoping(t *testing.T) {
	accountKey, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("CreateAccount failed: %v", err)
	}
	accountSeed, err := accountKey.Seed()
	if err != nil {
		t.Fatalf("account Seed failed: %v", err)
	}

	creds, err := issueSpokeCredentials(string(accountSeed), "scoped-spoke")
	if err != nil {
		t.Fatalf("issueSpokeCredentials failed: %v", err)
	}

	// Extract the JWT from the creds file (text between BEGIN/END markers).
	jwtStart := strings.Index(creds, "BEGIN NATS USER JWT")
	jwtEnd := strings.Index(creds, "END NATS USER JWT")
	if jwtStart < 0 || jwtEnd < 0 {
		t.Fatal("creds missing JWT markers")
	}
	jwtBlock := creds[jwtStart:jwtEnd]
	// The JWT is the last line of the begin marker block, before the end marker.
	lines := strings.Split(jwtBlock, "\n")
	var jwtToken string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "---") {
			jwtToken = line
		}
	}
	if jwtToken == "" {
		t.Fatal("could not extract JWT from creds")
	}

	// Decode the JWT payload to verify subject scoping.
	parts := strings.Split(jwtToken, ".")
	if len(parts) != 3 {
		t.Fatalf("JWT has %d parts, want 3", len(parts))
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("failed to decode JWT payload: %v", err)
	}

	var claims struct {
		Nats struct {
			Pub struct {
				Allow []string `json:"allow"`
			} `json:"pub"`
			Sub struct {
				Allow []string `json:"allow"`
			} `json:"sub"`
		} `json:"nats"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("failed to unmarshal JWT claims: %v", err)
	}

	// Verify expected subject permissions are present.
	pubAllows := strings.Join(claims.Nats.Pub.Allow, " ")
	if !strings.Contains(pubAllows, "heartbeat.scoped-spoke") {
		t.Error("pub allow should include heartbeat.scoped-spoke")
	}
	if !strings.Contains(pubAllows, "joke.response.scoped-spoke") {
		t.Error("pub allow should include joke.response.scoped-spoke")
	}

	subAllows := strings.Join(claims.Nats.Sub.Allow, " ")
	if !strings.Contains(subAllows, "joke.request.scoped-spoke") {
		t.Error("sub allow should include joke.request.scoped-spoke")
	}
}
