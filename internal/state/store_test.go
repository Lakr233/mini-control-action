package state

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	rec := Record{JobID: 42, RunAttempt: 1, Repo: "acme/widgets", State: StateWaitReady, VMID: "vm-a", QueuedAt: time.Now().UTC()}
	if err := s.Upsert(rec); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	got, ok := s2.Get(Key{42, 1})
	if !ok || got.VMID != "vm-a" || got.State != StateWaitReady {
		t.Fatalf("reload mismatch: %+v ok=%v", got, ok)
	}
	if !s2.HasVM("vm-a") || s2.HasVM("vm-b") {
		t.Fatal("HasVM wrong")
	}
	if err := s2.Delete(Key{42, 1}); err != nil {
		t.Fatal(err)
	}
	if _, ok := s2.Get(Key{42, 1}); ok {
		t.Fatal("record survived delete")
	}
}

func TestNewIdemKey(t *testing.T) {
	k1 := NewIdemKey(7, 2, 1)
	k2 := NewIdemKey(7, 2, 1)
	if !strings.HasPrefix(k1, "mcra1-j7-a2-r1-") || len(k1) > 128 {
		t.Fatalf("bad key format %q", k1)
	}
	if k1 == k2 {
		t.Fatal("keys for the same attempt must differ (random suffix)")
	}
}

func TestCorruptFileMovedAside(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if len(s.List()) != 0 {
		t.Fatal("corrupt store should start empty")
	}
	if _, err := os.Stat(path + ".corrupt"); err != nil {
		t.Fatal("corrupt file not moved aside:", err)
	}
	// Store still works after recovery.
	if err := s.Upsert(Record{JobID: 1, State: StateQueued, QueuedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
}

func TestTerminal(t *testing.T) {
	if StateComplete.Terminal() != true || StateFailed.Terminal() != true || StateJobRunning.Terminal() != false {
		t.Fatal("Terminal() classification wrong")
	}
}
