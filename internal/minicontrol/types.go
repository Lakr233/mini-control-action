package minicontrol

import (
	"fmt"
	"time"
)

// VM statuses reported by the client API.
const (
	StatusProvisioning = "provisioning"
	StatusReady        = "ready"
	StatusStopping     = "stopping"
	StatusStopped      = "stopped"
	StatusError        = "error"
	StatusDeleting     = "deleting"
)

type SKU struct {
	ID              string `json:"id"`
	HardwareProfile string `json:"hardware_profile"`
	CPUCores        int    `json:"cpu_cores"`
	MemoryMiB       int    `json:"memory_mib"`
	DiskGiB         int    `json:"disk_gib"`
	Available       bool   `json:"available"`
	Price           struct {
		CostFactor  float64 `json:"cost_factor"`
		RateVersion string  `json:"rate_version"`
		Currency    string  `json:"currency"`
		Configured  bool    `json:"configured"`
	} `json:"price"`
}

type CreateVMRequest struct {
	SKU string `json:"sku"`
	// Tag is an optional worker placement constraint. Omitted from the JSON
	// body when empty; deployments without tag support reject the unknown
	// field with a 400 and the create fails — the constraint is never
	// silently dropped.
	Tag string `json:"tag,omitempty"`
}

type VM struct {
	ID     string `json:"vm_id"`
	Status string `json:"status"`
	SKU    string `json:"sku"`
	// WorkerName and WorkerTags report where the scheduler actually placed the
	// VM. They are the only fleet visibility the API-key surface offers, and
	// the only way to confirm a requested tag was honoured — always log them.
	WorkerName    string   `json:"worker_name"`
	WorkerTags    []string `json:"worker_tags"`
	BillingActive bool     `json:"billing_active"`
	LastError     string   `json:"last_error"`
	Username      string   `json:"username"`
	Password      string   `json:"password"`
	SSH           struct {
		WebsocketURL string `json:"websocket_url"`
	} `json:"ssh"`
}

type ExecRequest struct {
	Command        string            `json:"command"`
	Cwd            string            `json:"cwd,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	Stream         bool              `json:"stream,omitempty"`
}

type ExecResult struct {
	ExitCode   int    `json:"exit_code"`
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	TimedOut   bool   `json:"timed_out"`
	DurationMs int64  `json:"duration_ms"`
}

// APIError is any non-2xx response from the API.
type APIError struct {
	Status     int
	Message    string        // "error" field
	ReasonCode string        // 409 capacity responses carry a machine-readable reason_code
	RetryAfter time.Duration // parsed from Retry-After on 429
}

func (e *APIError) Error() string {
	if e.ReasonCode != "" {
		return fmt.Sprintf("mini-control API %d: %s (%s)", e.Status, e.Message, e.ReasonCode)
	}
	return fmt.Sprintf("mini-control API %d: %s", e.Status, e.Message)
}

// IsNotFound reports whether err is a 404 APIError.
func IsNotFound(err error) bool {
	ae, ok := err.(*APIError)
	return ok && ae.Status == 404
}
