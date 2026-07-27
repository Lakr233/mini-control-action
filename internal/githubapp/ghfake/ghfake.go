// Package ghfake is a minimal in-memory GitHub API for offline tests.
package ghfake

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Job struct {
	ID         int64
	RunID      int64
	RunAttempt int
	Status     string // queued | in_progress | completed
	Conclusion string
	RunnerName string
	RunnerID   int64
	Labels     []string
}

type Runner struct {
	ID     int64
	Name   string
	Status string
	Busy   bool
}

type Server struct {
	*httptest.Server

	mu           sync.Mutex
	Jobs         map[int64]*Job // by job ID
	Runners      map[int64]*Runner
	nextRunnerID int64
	JITCalls     int
	JobsFetches  int // GET .../runs/{id}/jobs calls
	RemovedIDs   []int64
	LatestTag    string
	Repos        []string // served by /user/repos
}

func New() *Server {
	s := &Server{
		Jobs:      map[int64]*Job{},
		Runners:   map[int64]*Runner{},
		LatestTag: "v2.325.0",
		Repos:     []string{"acme/widgets"},
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

func (s *Server) SetJob(j *Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Jobs[j.ID] = j
}

// JITCount returns how many JIT configs were generated.
func (s *Server) JITCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.JITCalls
}

// RunnerCount returns the number of registered runners.
func (s *Server) RunnerCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.Runners)
}

// SetRunnerBusy flips the busy flag of the runner with the given name.
func (s *Server) SetRunnerBusy(name string, busy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rn := range s.Runners {
		if rn.Name == name {
			rn.Busy = busy
			if busy {
				rn.Status = "online"
			}
		}
	}
}

// JobsFetchCount returns how many per-run jobs listings were served.
func (s *Server) JobsFetchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.JobsFetches
}

// AddRunner registers a runner directly (for reconciler tests).
func (s *Server) AddRunner(r *Runner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.ID == 0 {
		s.nextRunnerID++
		r.ID = s.nextRunnerID
	}
	s.Runners[r.ID] = r
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := r.URL.Path
	writeJSON := func(status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	switch {
	case strings.HasSuffix(path, "/actions/runners/generate-jitconfig") && r.Method == "POST":
		var req struct {
			Name   string   `json:"name"`
			Labels []string `json:"labels"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		for _, existing := range s.Runners {
			if existing.Name == req.Name {
				writeJSON(409, map[string]string{"message": "runner name already exists"})
				return
			}
		}
		s.nextRunnerID++
		s.JITCalls++
		rn := &Runner{ID: s.nextRunnerID, Name: req.Name, Status: "offline"}
		s.Runners[rn.ID] = rn
		writeJSON(201, map[string]any{
			"runner":             map[string]any{"id": rn.ID, "name": rn.Name},
			"encoded_jit_config": "ZmFrZS1qaXQtY29uZmln", // base64("fake-jit-config")
		})
	case strings.HasSuffix(path, "/actions/runners") && r.Method == "GET":
		// Paginated like the real API: per_page + page + total_count.
		perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
		if perPage <= 0 || perPage > 100 {
			perPage = 30
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page <= 0 {
			page = 1
		}
		all := []map[string]any{}
		ids := make([]int64, 0, len(s.Runners))
		for id := range s.Runners {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			rn := s.Runners[id]
			all = append(all, map[string]any{"id": rn.ID, "name": rn.Name, "status": rn.Status, "busy": rn.Busy})
		}
		lo := min((page-1)*perPage, len(all))
		hi := min(lo+perPage, len(all))
		writeJSON(200, map[string]any{"total_count": len(all), "runners": all[lo:hi]})
	case strings.Contains(path, "/actions/runners/") && r.Method == "DELETE":
		var id int64
		fmt.Sscanf(path[strings.LastIndex(path, "/")+1:], "%d", &id)
		if _, ok := s.Runners[id]; !ok {
			writeJSON(404, map[string]string{"message": "not found"})
			return
		}
		delete(s.Runners, id)
		s.RemovedIDs = append(s.RemovedIDs, id)
		w.WriteHeader(204)
	case strings.Contains(path, "/actions/jobs/") && r.Method == "GET":
		var id int64
		fmt.Sscanf(path[strings.LastIndex(path, "/")+1:], "%d", &id)
		j, ok := s.Jobs[id]
		if !ok {
			writeJSON(404, map[string]string{"message": "job not found"})
			return
		}
		writeJSON(200, map[string]any{
			"id": j.ID, "status": j.Status, "conclusion": j.Conclusion,
			"runner_name": j.RunnerName, "runner_id": j.RunnerID,
		})
	case path == "/repos/actions/runner/releases/latest" && r.Method == "GET":
		v := strings.TrimPrefix(s.LatestTag, "v")
		writeJSON(200, map[string]any{
			"tag_name": s.LatestTag,
			"assets": []map[string]string{
				{"name": "actions-runner-linux-x64-" + v + ".tar.gz", "browser_download_url": "https://github.com/actions/runner/releases/download/" + s.LatestTag + "/actions-runner-linux-x64-" + v + ".tar.gz"},
				{"name": "actions-runner-osx-arm64-" + v + ".tar.gz", "browser_download_url": "https://github.com/actions/runner/releases/download/" + s.LatestTag + "/actions-runner-osx-arm64-" + v + ".tar.gz"},
			},
		})
	case path == "/user/repos" && r.Method == "GET":
		out := []map[string]any{}
		for _, name := range s.Repos {
			out = append(out, map[string]any{"full_name": name, "archived": false,
				"pushed_at": time.Now().UTC().Format(time.RFC3339)})
		}
		writeJSON(200, out)
	case strings.Contains(path, "/actions/runs") && strings.HasSuffix(path, "/jobs") && r.Method == "GET":
		s.JobsFetches++
		var runID int64
		parts := strings.Split(path, "/")
		fmt.Sscanf(parts[len(parts)-2], "%d", &runID)
		jobs := []map[string]any{}
		for _, j := range s.Jobs {
			if j.RunID == runID {
				jobs = append(jobs, map[string]any{
					"id": j.ID, "run_id": j.RunID, "run_attempt": j.RunAttempt,
					"status": j.Status, "labels": j.Labels,
				})
			}
		}
		writeJSON(200, map[string]any{"jobs": jobs})
	case strings.HasSuffix(path, "/actions/runs") && r.Method == "GET":
		want := r.URL.Query().Get("status")
		if want == "" {
			want = "queued"
		}
		// updated_at is derived from the run's job statuses so it moves
		// exactly when something about the run changes (gating fidelity).
		sig := map[int64]string{}
		for _, j := range s.Jobs {
			sig[j.RunID] += fmt.Sprintf("%d=%s;", j.ID, j.Status)
		}
		runs := map[int64]bool{}
		out := []map[string]any{}
		for _, j := range s.Jobs {
			if j.Status == want && !runs[j.RunID] {
				runs[j.RunID] = true
				out = append(out, map[string]any{
					"id": j.RunID, "run_attempt": j.RunAttempt, "updated_at": sig[j.RunID],
				})
			}
		}
		writeJSON(200, map[string]any{"workflow_runs": out})
	default:
		writeJSON(404, map[string]string{"message": "no fake route: " + r.Method + " " + path})
	}
}
