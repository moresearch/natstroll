package shared

import (
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestParseLogLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"DEBUG":   slog.LevelDebug,
		"  debug ": slog.LevelDebug,
		"info":    slog.LevelInfo,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"WARNING": slog.LevelWarn,
		"error":   slog.LevelError,
		"ERROR":   slog.LevelError,
		"":        slog.LevelInfo,
		"unknown": slog.LevelInfo,
	}
	for in, want := range cases {
		got := parseLogLevel(in)
		if got != want {
			t.Errorf("parseLogLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestInitLogger(t *testing.T) {
	// Save and restore the env var.
	prev := os.Getenv("LOG_LEVEL")
	defer os.Setenv("LOG_LEVEL", prev)

	os.Setenv("LOG_LEVEL", "debug")
	logger := InitLogger("test-component")
	if logger == nil {
		t.Fatal("InitLogger returned nil")
	}

	// The logger should include the component attribute.
	// We can verify it produces output with the expected level set.
	// Write to a buffer? slog.NewTextHandler writes to os.Stdout by default.
	// Here we only verify the function does not panic and returns a non-nil logger.
}

func TestValidateSpokeID(t *testing.T) {
	valid := []string{"a", "spoke-1", "my.spoke", "spoke_2", "ABC-123"}
	for _, id := range valid {
		if err := ValidateSpokeID(id); err != nil {
			t.Errorf("ValidateSpokeID(%q) unexpected error: %v", id, err)
		}
	}

	invalid := []string{"", ".spoke", "spoke.", "spoke..id", "spoke id", "spoke/id", "spoke:id"}
	for _, id := range invalid {
		if err := ValidateSpokeID(id); err == nil {
			t.Errorf("ValidateSpokeID(%q) expected error, got nil", id)
		}
	}
}

func TestSafeName(t *testing.T) {
	cases := map[string]string{
		"spoke-1":      "spoke_1",
		"my.spoke":     "my_spoke",
		"a:b/c\\d":     "a_b_c_d",
		"already_safe": "already_safe",
	}
	for in, want := range cases {
		got := SafeName(in)
		if got != want {
			t.Errorf("SafeName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestConsumerNameForSpoke(t *testing.T) {
	got := ConsumerNameForSpoke("spoke-1")
	want := "joke_consumer_spoke_1"
	if got != want {
		t.Errorf("ConsumerNameForSpoke(\"spoke-1\") = %q, want %q", got, want)
	}
}

func TestWriteTempCreds(t *testing.T) {
	path, err := WriteTempCreds("test-spoke", []byte("test-credentials"))
	if err != nil {
		t.Fatalf("WriteTempCreds failed: %v", err)
	}
	defer os.Remove(path)

	if !strings.HasSuffix(path, ".creds") {
		t.Errorf("temp creds path %q does not end with .creds", path)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat temp creds file failed: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("temp creds file mode = %o, want 0600", info.Mode().Perm())
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read temp creds file failed: %v", err)
	}
	if string(data) != "test-credentials" {
		t.Errorf("temp creds content = %q, want %q", string(data), "test-credentials")
	}
}

func TestJokeRequestRoundTrip(t *testing.T) {
	req := JokeRequest{RequestID: "req-1", Joke: "Why did the chicken cross the road?", FromSpokeID: "spoke-1"}
	bytes, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded JokeRequest
	if err := json.Unmarshal(bytes, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded != req {
		t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, req)
	}
}

func TestJokeResponseRoundTrip(t *testing.T) {
	resp := JokeResponse{RequestID: "req-1", Reply: "To get to the other side.", ReplyingSpoke: "spoke-1"}
	bytes, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded JokeResponse
	if err := json.Unmarshal(bytes, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded != resp {
		t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, resp)
	}
}

func TestRegisterResponseRoundTrip(t *testing.T) {
	resp := RegisterResponse{CredsData: "super-secret-jwt"}
	bytes, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded RegisterResponse
	if err := json.Unmarshal(bytes, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded != resp {
		t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, resp)
	}
}

func TestRegisterRequestRoundTrip(t *testing.T) {
	req := RegisterRequest{SpokeID: "test-spoke-42"}
	bytes, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded RegisterRequest
	if err := json.Unmarshal(bytes, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded != req {
		t.Errorf("round-trip mismatch: got %+v, want %+v", decoded, req)
	}
}
