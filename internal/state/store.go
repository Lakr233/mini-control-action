// Package state persists per-job runner records to a single JSON file with
// atomic writes, so the service can resume VM lifecycles after a restart and
// the reconciler can tell owned VMs from orphans.
package state

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// JobState is the FSM state of one workflow job's runner VM.
type JobState string

const (
	StateQueued        JobState = "queued"
	StateCreating      JobState = "creating"
	StateWaitReady     JobState = "wait_ready"
	StateBootstrapping JobState = "bootstrapping"
	StateRunnerUp      JobState = "runner_up"
	StateJobRunning    JobState = "job_running"
	StateJobDone       JobState = "job_done"
	StateDeleting      JobState = "deleting"
	StateComplete      JobState = "complete"
	StateFailed        JobState = "failed" // terminal failure; VM already deleted
)

// Terminal reports whether s needs no further work.
func (s JobState) Terminal() bool { return s == StateComplete || s == StateFailed }

type Record struct {
	JobID          int64    `json:"job_id"`
	RunAttempt     int      `json:"run_attempt"`
	Retry          int      `json:"retry"`
	Repo           string   `json:"repo"` // "owner/repo"
	Labels         []string `json:"labels"`
	WorkflowJobURL string   `json:"workflow_job_url,omitempty"`

	State          JobState  `json:"state"`
	VMID           string    `json:"vm_id,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	RunnerName     string    `json:"runner_name,omitempty"`
	RunnerID       int64     `json:"runner_id,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
	QueuedAt       time.Time `json:"queued_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// Key identifies a job attempt uniquely.
type Key struct {
	JobID      int64
	RunAttempt int
}

func (r *Record) Key() Key { return Key{r.JobID, r.RunAttempt} }

// NewIdemKey builds the Idempotency-Key for one create attempt. The random
// suffix guarantees a fresh key even if the state file was lost and counters
// reset (a purely deterministic key would replay a stale create response
// pointing at a long-deleted VM). Crash-replay safety comes from persisting
// the key in the record BEFORE the create call, not from determinism.
func NewIdemKey(jobID int64, attempt, retry int) string {
	return fmt.Sprintf("mcra1-j%d-a%d-r%d-%s", jobID, attempt, retry, ShortNonce())
}

// ShortNonce returns 8 hex chars of randomness for names/keys that must not
// collide with earlier incarnations of themselves.
func ShortNonce() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

type Store struct {
	mu   sync.Mutex
	path string
	recs map[Key]Record
	log  *slog.Logger
}

type fileFormat struct {
	Version int      `json:"version"`
	Records []Record `json:"records"`
}

// Open loads (or initializes) the store at path. A corrupt file is moved
// aside to <path>.corrupt and the store starts fresh with a loud error log —
// the reconciler will re-adopt or delete any VMs the lost records tracked.
func Open(path string, log *slog.Logger) (*Store, error) {
	s := &Store{path: path, recs: map[Key]Record{}, log: log}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	var f fileFormat
	if err := json.Unmarshal(raw, &f); err != nil {
		corrupt := path + ".corrupt"
		log.Error("state file is corrupt; moving aside and starting fresh — "+
			"the reconciler will clean up any VMs it tracked", "path", path, "moved_to", corrupt, "error", err)
		if mvErr := os.Rename(path, corrupt); mvErr != nil {
			return nil, fmt.Errorf("state file corrupt (%v) and could not move aside: %w", err, mvErr)
		}
		return s, nil
	}
	for _, r := range f.Records {
		s.recs[r.Key()] = r
	}
	return s, nil
}

// persist writes atomically: temp file in the same directory, then rename.
// Caller must hold mu.
func (s *Store) persist() error {
	f := fileFormat{Version: 1, Records: make([]Record, 0, len(s.recs))}
	for _, r := range s.recs {
		f.Records = append(f.Records, r)
	}
	sort.Slice(f.Records, func(i, j int) bool {
		if f.Records[i].JobID != f.Records[j].JobID {
			return f.Records[i].JobID < f.Records[j].JobID
		}
		return f.Records[i].RunAttempt < f.Records[j].RunAttempt
	})
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".state-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, s.path)
}

// Upsert stores rec (stamping UpdatedAt) and persists.
func (s *Store) Upsert(rec Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec.UpdatedAt = time.Now().UTC()
	s.recs[rec.Key()] = rec
	return s.persist()
}

func (s *Store) Get(k Key) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.recs[k]
	return r, ok
}

// List returns all records, newest-queued first.
func (s *Store) List() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, 0, len(s.recs))
	for _, r := range s.recs {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].QueuedAt.After(out[j].QueuedAt) })
	return out
}

// Delete removes a record and persists.
func (s *Store) Delete(k Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.recs, k)
	return s.persist()
}

// SetUpdatedAtForTest backdates a record's UpdatedAt (test-only hook for
// retention pruning).
func (s *Store) SetUpdatedAtForTest(k Key, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.recs[k]; ok {
		r.UpdatedAt = at
		s.recs[k] = r
	}
}

// HasVM reports whether any record references vmID.
func (s *Store) HasVM(vmID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.recs {
		if r.VMID == vmID && !r.State.Terminal() {
			return true
		}
	}
	return false
}
