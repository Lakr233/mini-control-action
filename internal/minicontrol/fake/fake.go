// Package fake is an in-memory mini-control Client VM API for offline tests.
// It simulates VM lifecycle (provisioning -> ready after N polls), idempotent
// creates, capacity/429 injection, and scripted ssh/exec responses.
package fake

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
)

type VM struct {
	ID     string
	Status string
	SKU    string
	// Tag is what the create requested; WorkerName/WorkerTags are where the
	// scheduler put it, exactly as the real server reports placement back.
	Tag        string
	WorkerName string
	WorkerTags []string
	Username   string
	Password   string
	// GetsUntilReady counts down on each GET while provisioning; at zero the
	// VM becomes ready.
	GetsUntilReady int
	readyGets      int
}

type ExecFunc func(vmID string, command string) (exitCode int, stdout, stderr string)

type Server struct {
	*httptest.Server

	mu          sync.Mutex
	apiKey      string
	vms         map[string]*VM
	idempotency map[string]idemRecord // key -> stored response
	nextID      int

	// Knobs (set before use; guarded by mu for mid-test changes).
	RejectTag       bool     // 400 on create when body has a tag field
	FleetTag        string   // the tag this fake's only worker carries
	WorkerName      string   // placement reported back; defaults to fake-worker-01
	CapacityReason  string   // when set, creates fail 409 with this reason_code
	Inject429       int      // number of creates to reject with 429 before succeeding
	GetsUntilReady  int      // default provisioning polls before ready
	CredsDelay      int      // ready GETs served without credentials first
	Exec            ExecFunc // scripted exec behavior; nil => exit 0, empty output
	CreateCount     int
	DeleteCount     int
	LastIdempotency string
	IdemKeys        map[string]bool // every Idempotency-Key ever seen
	SKUs            []map[string]any
	FailExecStatus  int // when non-zero, exec responds with this HTTP status
}

type idemRecord struct {
	body   string
	status int
	resp   []byte
}

func New(apiKey string) *Server {
	s := &Server{
		apiKey:      apiKey,
		vms:         map[string]*VM{},
		idempotency: map[string]idemRecord{},
		IdemKeys:    map[string]bool{},
		SKUs: []map[string]any{
			{"id": "m4-big-v1", "hardware_profile": "m4", "cpu_cores": 8, "memory_mib": 8192, "disk_gib": 150, "available": true,
				"price": map[string]any{"cost_factor": 0.1635, "currency": "USD", "configured": true}},
			{"id": "m4-tiny-v1", "hardware_profile": "m4", "cpu_cores": 2, "memory_mib": 4096, "disk_gib": 150, "available": true,
				"price": map[string]any{"cost_factor": 0.0955, "currency": "USD", "configured": true}},
		},
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

// BaseURL is the client-facing API base (ends with /api/client/v1).
func (s *Server) BaseURL() string { return s.URL + "/api/client/v1" }

// VMIDs returns the IDs of all live VMs.
func (s *Server) VMIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.vms))
	for id := range s.vms {
		ids = append(ids, id)
	}
	return ids
}

// SetCapacityReason toggles capacity rejection while the server is live.
func (s *Server) SetCapacityReason(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CapacityReason = reason
}

// IdemKeyCount returns how many distinct Idempotency-Keys were seen.
func (s *Server) IdemKeyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.IdemKeys)
}

// Counts returns (creates, deletes) observed so far.
func (s *Server) Counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.CreateCount, s.DeleteCount
}

// VM returns a copy of a stored VM, including the tag its create requested.
func (s *Server) VM(id string) (VM, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vm, ok := s.vms[id]
	if !ok {
		return VM{}, false
	}
	return *vm, true
}

// AddVM inserts a VM directly (for orphan-reconciler tests).
func (s *Server) AddVM(vm *VM) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if vm.Status == "" {
		vm.Status = "ready"
	}
	s.vms[vm.ID] = vm
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+s.apiKey {
		writeJSON(w, 401, map[string]string{"error": "unauthorized"})
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/client/v1")
	switch {
	case path == "/skus" && r.Method == "GET":
		s.mu.Lock()
		skus := s.SKUs
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"skus": skus})
	case path == "/vms" && r.Method == "POST":
		s.handleCreate(w, r)
	case path == "/vms" && r.Method == "GET":
		s.mu.Lock()
		out := []map[string]any{}
		for _, vm := range s.vms {
			out = append(out, s.vmJSON(vm))
		}
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"vms": out})
	case strings.HasPrefix(path, "/vms/") && strings.HasSuffix(path, "/ssh/exec") && r.Method == "POST":
		s.handleExec(w, r, strings.TrimSuffix(strings.TrimPrefix(path, "/vms/"), "/ssh/exec"))
	case strings.HasPrefix(path, "/vms/") && r.Method == "GET":
		id := strings.TrimPrefix(path, "/vms/")
		s.mu.Lock()
		vm, ok := s.vms[id]
		if ok && vm.Status == "provisioning" {
			vm.GetsUntilReady--
			if vm.GetsUntilReady <= 0 {
				vm.Status = "ready"
			}
		}
		if ok && vm.Status == "ready" {
			vm.readyGets++
		}
		var body map[string]any
		if ok {
			body = s.vmJSON(vm)
		}
		s.mu.Unlock()
		if !ok {
			writeJSON(w, 404, map[string]string{"error": "not found"})
			return
		}
		writeJSON(w, 200, body)
	case strings.HasPrefix(path, "/vms/") && r.Method == "DELETE":
		id := strings.TrimPrefix(path, "/vms/")
		s.mu.Lock()
		_, ok := s.vms[id]
		delete(s.vms, id)
		s.DeleteCount++
		s.mu.Unlock()
		if !ok {
			writeJSON(w, 404, map[string]string{"error": "not found"})
			return
		}
		w.WriteHeader(204)
	default:
		writeJSON(w, 404, map[string]string{"error": "not found"})
	}
}

func (s *Server) vmJSON(vm *VM) map[string]any {
	tags := vm.WorkerTags
	if tags == nil {
		tags = []string{}
	}
	m := map[string]any{
		"vm_id":       vm.ID,
		"status":      vm.Status,
		"sku":         vm.SKU,
		"worker_name": vm.WorkerName,
		"worker_tags": tags,
	}
	if vm.Status == "ready" && vm.readyGets > s.CredsDelay {
		user := vm.Username
		if user == "" {
			user = "admin"
		}
		pass := vm.Password
		if pass == "" {
			pass = "secret"
		}
		m["username"] = user
		m["password"] = pass
		m["ssh"] = map[string]string{"websocket_url": "wss://fake/vms/" + vm.ID + "/ssh/ws"}
	}
	return m
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	idem := r.Header.Get("Idempotency-Key")
	if idem == "" || len(idem) > 128 {
		writeJSON(w, 400, map[string]string{"error": "missing or invalid Idempotency-Key"})
		return
	}
	bodyRaw := new(strings.Builder)
	dec := json.NewDecoder(io2{r: r.Body, w: bodyRaw})
	dec.DisallowUnknownFields()
	var req struct {
		SKU string `json:"sku"`
		Tag string `json:"tag"`
	}
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid request: " + err.Error()})
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastIdempotency = idem
	s.IdemKeys[idem] = true

	if s.RejectTag && req.Tag != "" {
		// Mirror the live server: unknown-field rejections carry a generic
		// message that does NOT mention the offending field.
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}
	if rec, ok := s.idempotency[idem]; ok {
		if rec.body != bodyRaw.String() {
			writeJSON(w, 409, map[string]string{"error": "Idempotency-Key was already used with a different request"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rec.status)
		_, _ = w.Write(rec.resp)
		return
	}
	if s.Inject429 > 0 {
		s.Inject429--
		w.Header().Set("Retry-After", "1")
		writeJSON(w, 429, map[string]string{"error": "rate limited"})
		return
	}
	if s.CapacityReason != "" {
		resp, _ := json.Marshal(map[string]any{"error": "no_matching_candidate", "reason_code": s.CapacityReason})
		// The real server commits the rejection under the key and replays it.
		s.idempotency[idem] = idemRecord{body: bodyRaw.String(), status: 409, resp: resp}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(409)
		_, _ = w.Write(resp)
		return
	}
	// A tag that no worker carries is a structured 409, never a placement
	// somewhere else.
	if req.Tag != "" && !strings.EqualFold(req.Tag, s.FleetTag) {
		resp, _ := json.Marshal(map[string]any{
			"error": "no available worker matches the requested tag", "reason_code": "no_workers_matching_tag",
		})
		s.idempotency[idem] = idemRecord{body: bodyRaw.String(), status: 409, resp: resp}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(409)
		_, _ = w.Write(resp)
		return
	}
	s.nextID++
	s.CreateCount++
	workerName := s.WorkerName
	if workerName == "" {
		workerName = "fake-worker-01"
	}
	var workerTags []string
	if s.FleetTag != "" {
		workerTags = []string{s.FleetTag}
	}
	vm := &VM{
		ID:             pad(s.nextID),
		Status:         "provisioning",
		SKU:            req.SKU,
		Tag:            req.Tag,
		WorkerName:     workerName,
		WorkerTags:     workerTags,
		GetsUntilReady: s.GetsUntilReady,
	}
	if vm.GetsUntilReady <= 0 {
		vm.Status = "ready"
	}
	s.vms[vm.ID] = vm
	resp, _ := json.Marshal(s.vmJSON(vm))
	s.idempotency[idem] = idemRecord{body: bodyRaw.String(), status: 201, resp: resp}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(201)
	_, _ = w.Write(resp)
}

func (s *Server) handleExec(w http.ResponseWriter, r *http.Request, vmID string) {
	var req struct {
		Command string `json:"command"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	s.mu.Lock()
	vm, ok := s.vms[vmID]
	exec := s.Exec
	failStatus := s.FailExecStatus
	s.mu.Unlock()
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	}
	if vm.Status != "ready" {
		writeJSON(w, 409, map[string]string{"error": "vm not ready"})
		return
	}
	if failStatus != 0 {
		writeJSON(w, failStatus, map[string]string{"error": "exec unavailable"})
		return
	}
	code, stdout, stderr := 0, "", ""
	if exec != nil {
		code, stdout, stderr = exec(vmID, req.Command)
	}
	writeJSON(w, 200, map[string]any{
		"exit_code": code, "stdout": stdout, "stderr": stderr,
		"timed_out": false, "duration_ms": 5,
	})
}

func pad(n int) string {
	const alpha = "abcdefghij"
	s := "vm-"
	for _, c := range []byte{byte(n / 100 % 10), byte(n / 10 % 10), byte(n % 10)} {
		s += string(alpha[c])
	}
	return s
}

// io2 tees the request body into a builder so idempotency can compare bodies.
type io2 struct {
	r interface{ Read([]byte) (int, error) }
	w *strings.Builder
}

func (t io2) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 {
		t.w.Write(p[:n])
	}
	return n, err
}
