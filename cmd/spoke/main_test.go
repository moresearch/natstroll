package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestIsFatalNATSError(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("permission denied for subject"), true},
		{errors.New("authorization violation"), true},
		{errors.New("authentication failed"), true},
		{errors.New("invalid credentials"), true},
		{errors.New("PERMISSION VIOLATION"), true},
		{errors.New("AUTHORIZATION error"), true},
		{fmt.Errorf("wrapped: %w", errors.New("credentials expired")), true},
		{errors.New("connection refused"), false},
		{errors.New("timeout"), false},
		{errors.New("no servers available"), false},
		{errors.New(""), false},
	}
	for _, c := range cases {
		got := isFatalNATSError(c.err)
		if got != c.want {
			t.Errorf("isFatalNATSError(%q) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestFallbackReply(t *testing.T) {
	// Non-empty joke.
	reply := fallbackReply("Why did the chicken cross the road?")
	if reply == "" {
		t.Error("fallbackReply returned empty string for a non-empty joke")
	}

	// Empty joke.
	reply = fallbackReply("")
	if reply == "" {
		t.Error("fallbackReply returned empty string for an empty joke")
	}

	// Whitespace-only joke.
	reply = fallbackReply("   ")
	if reply == "" {
		t.Error("fallbackReply returned empty string for a whitespace joke")
	}

	// Verify the two cases produce different fallbacks.
	r1 := fallbackReply("some joke")
	r2 := fallbackReply("")
	if r1 == r2 {
		t.Error("fallbackReply should return different replies for empty vs non-empty jokes")
	}
}

func TestSleepOrDone(t *testing.T) {
	t.Run("completes after duration", func(t *testing.T) {
		ctx := context.Background()
		start := time.Now()
		err := sleepOrDone(ctx, 50*time.Millisecond)
		elapsed := time.Since(start)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if elapsed < 50*time.Millisecond {
			t.Errorf("slept too briefly: %v", elapsed)
		}
	})

	t.Run("returns on context cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := time.Now()
		err := sleepOrDone(ctx, 5*time.Second)
		elapsed := time.Since(start)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
		if elapsed > 100*time.Millisecond {
			t.Errorf("should have returned immediately on canceled ctx, took %v", elapsed)
		}
	})
}
