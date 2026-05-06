package binance

import (
	"testing"
	"time"
)

// runReconnectLoop is a minimal copy of Run's backoff state machine so we can
// drive it deterministically in tests without a real WS server. It mirrors
// stream.go: backoff doubles up to maxBackoff after each failure, resets to
// initialBackoff once `firstMessageInSession` fires for that session.
func runReconnectLoop(t *testing.T, sessions []sessionResult) []time.Duration {
	t.Helper()
	const (
		initialBackoff = time.Second
		maxBackoff     = 30 * time.Second
	)
	backoff := initialBackoff
	delays := make([]time.Duration, 0, len(sessions))
	for _, s := range sessions {
		// Simulate session: if it received at least one message, the reset
		// callback fires (mirroring stream.runOnce's onConnected).
		if s.gotMessage {
			backoff = initialBackoff
		}
		// Record the delay that would precede the *next* reconnect attempt
		// (after this session ends). Skip if it was a clean exit.
		if !s.cleanExit {
			delays = append(delays, backoff)
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
	}
	return delays
}

type sessionResult struct {
	gotMessage bool // first message received before the drop
	cleanExit  bool // ctx cancelled, no reconnect needed
}

func TestReconnectBackoff_GrowsOnRepeatedFailures(t *testing.T) {
	// 5 sessions in a row that fail before any message arrives.
	delays := runReconnectLoop(t, []sessionResult{
		{}, {}, {}, {}, {},
	})
	want := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	if len(delays) != len(want) {
		t.Fatalf("delays len: got %d, want %d (%v)", len(delays), len(want), delays)
	}
	for i, d := range delays {
		if d != want[i] {
			t.Errorf("delay[%d]: got %s, want %s", i, d, want[i])
		}
	}
}

func TestReconnectBackoff_CapsAt30s(t *testing.T) {
	// 10 immediate failures should saturate at maxBackoff.
	const n = 10
	sessions := make([]sessionResult, n)
	delays := runReconnectLoop(t, sessions)
	if last := delays[len(delays)-1]; last != 30*time.Second {
		t.Errorf("last delay: got %s, want 30s", last)
	}
}

func TestReconnectBackoff_ResetsAfterFirstMessage(t *testing.T) {
	// Three failures grow backoff to 4s; a successful session (got a message)
	// then drops, but its reset means the *next* delay restarts at 1s instead
	// of continuing at 8s.
	delays := runReconnectLoop(t, []sessionResult{
		{},                 // delay 1s, then *2 = 2s
		{},                 // delay 2s, then *2 = 4s
		{},                 // delay 4s, then *2 = 8s
		{gotMessage: true}, // reset; this session's eventual drop -> delay 1s
		{},                 // delay 2s (proves growth resumed from reset)
	})
	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		1 * time.Second, // post-reset delay
		2 * time.Second, // grows again from reset
	}
	if len(delays) != len(want) {
		t.Fatalf("delays len: got %d, want %d (%v)", len(delays), len(want), delays)
	}
	for i, d := range delays {
		if d != want[i] {
			t.Errorf("delay[%d]: got %s, want %s", i, d, want[i])
		}
	}
}
