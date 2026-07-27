// Package minicontrol is a typed client for the mini-control Client VM API
// (https://<worker-host>/api/client/v1), authenticated with a bearer API key.
package minicontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
	log     *slog.Logger

	// Create-side token bucket mirroring the server's per-key limiter
	// (burst 5, refill 2/s) so we rarely see a real 429.
	mu     sync.Mutex
	tokens float64
	last   time.Time
}

const (
	createBurst  = 5.0
	createPerSec = 2.0
)

func New(baseURL, apiKey string, timeout time.Duration, log *slog.Logger) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		http:    &http.Client{Timeout: timeout},
		log:     log,
		tokens:  createBurst,
		last:    time.Now(),
	}
}

func (c *Client) waitCreateToken(ctx context.Context) error {
	for {
		c.mu.Lock()
		now := time.Now()
		c.tokens = min(createBurst, c.tokens+now.Sub(c.last).Seconds()*createPerSec)
		c.last = now
		if c.tokens >= 1 {
			c.tokens--
			c.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - c.tokens) / createPerSec * float64(time.Second))
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (c *Client) do(ctx context.Context, method, path string, body any, headers map[string]string, out any) error {
	return c.doLimited(ctx, method, path, body, headers, out, 4<<20)
}

func (c *Client) doLimited(ctx context.Context, method, path string, body any, headers map[string]string, out any, maxBody int64) error {
	var rd io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > maxBody {
		return fmt.Errorf("response body exceeded %d bytes", maxBody)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out != nil && len(data) > 0 {
			return json.Unmarshal(data, out)
		}
		return nil
	}
	ae := &APIError{Status: resp.StatusCode}
	var e struct {
		Error      string `json:"error"`
		ReasonCode string `json:"reason_code"`
	}
	if json.Unmarshal(data, &e) == nil {
		ae.Message = e.Error
		ae.ReasonCode = e.ReasonCode
	}
	if ae.Message == "" {
		ae.Message = strings.TrimSpace(string(data))
	}
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		if secs, err := strconv.Atoi(ra); err == nil {
			ae.RetryAfter = time.Duration(secs) * time.Second
		}
	}
	return ae
}

func (c *Client) ListSKUs(ctx context.Context) ([]SKU, error) {
	var out struct {
		SKUs []SKU `json:"skus"`
	}
	if err := c.do(ctx, http.MethodGet, "/skus", nil, nil, &out); err != nil {
		return nil, err
	}
	return out.SKUs, nil
}

// CreateVM provisions a VM. idempotencyKey is required by the API (1–128
// bytes); replaying the same key with the same body returns the original
// response — INCLUDING a stored 409 rejection — which makes crash-recovery
// safe but means capacity retries need fresh keys.
//
// A rejected create is a failure, never a downgrade: a deployment that does
// not accept the configured worker tag answers with a generic
// 400 "invalid json", and silently retrying without the tag would place the
// VM on an arbitrary worker while the config still claims a placement
// constraint. The error is returned as-is.
func (c *Client) CreateVM(ctx context.Context, req CreateVMRequest, idempotencyKey string) (*VM, error) {
	if n := len(idempotencyKey); n < 1 || n > 128 {
		return nil, fmt.Errorf("idempotency key must be 1-128 bytes, got %d", n)
	}
	if err := c.waitCreateToken(ctx); err != nil {
		return nil, err
	}
	hdr := map[string]string{"Idempotency-Key": idempotencyKey}
	vm := &VM{}
	if err := c.do(ctx, http.MethodPost, "/vms", req, hdr, vm); err != nil {
		// The unknown-field rejection is generic ("invalid json"), so name the
		// likely culprit here rather than making the operator rediscover it.
		if ae, ok := err.(*APIError); ok && ae.Status == 400 && req.Tag != "" {
			c.log.Error("create rejected; this deployment may not accept a worker tag",
				"tag", req.Tag, "error", ae.Message)
		}
		return nil, err
	}
	return vm, nil
}

func (c *Client) GetVM(ctx context.Context, id string) (*VM, error) {
	vm := &VM{}
	if err := c.do(ctx, http.MethodGet, "/vms/"+id, nil, nil, vm); err != nil {
		return nil, err
	}
	return vm, nil
}

func (c *Client) ListVMs(ctx context.Context) ([]VM, error) {
	var out struct {
		VMs []VM `json:"vms"`
	}
	if err := c.do(ctx, http.MethodGet, "/vms", nil, nil, &out); err != nil {
		return nil, err
	}
	return out.VMs, nil
}

// DeleteVM removes a VM. A 404 (already gone) is treated as success.
func (c *Client) DeleteVM(ctx context.Context, id string) error {
	err := c.do(ctx, http.MethodDelete, "/vms/"+id, nil, nil, nil)
	if err != nil && IsNotFound(err) {
		return nil
	}
	return err
}

// Exec runs a command in the VM guest via the server-mediated SSH channel.
// The command runs under /bin/sh -lc with no stdin; stdout/stderr are each
// truncated by the server at 1 MiB.
func (c *Client) Exec(ctx context.Context, vmID string, req ExecRequest) (*ExecResult, error) {
	req.Stream = false
	res := &ExecResult{}
	// Exec can legitimately run up to req.TimeoutSeconds; bypass the short
	// default client timeout for this call via a per-call deadline.
	timeout := time.Duration(req.TimeoutSeconds)*time.Second + 30*time.Second
	if req.TimeoutSeconds == 0 {
		timeout = 11 * time.Minute // server default exec timeout is 10m
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	execClient := &Client{baseURL: c.baseURL, apiKey: c.apiKey, log: c.log,
		http: &http.Client{Transport: c.http.Transport}}
	// stdout+stderr are truncated server-side at ~1 MiB each, but JSON
	// escaping can inflate control-heavy output several-fold — allow 16 MiB.
	if err := execClient.doLimited(ctx, http.MethodPost, "/vms/"+vmID+"/ssh/exec", req, nil, res, 16<<20); err != nil {
		return nil, err
	}
	return res, nil
}

// RetryDelay returns how long to wait before retrying err, or false when err
// is not retryable this way (only 429s carry a server-directed delay).
func RetryDelay(err error) (time.Duration, bool) {
	ae, ok := err.(*APIError)
	if !ok || ae.Status != 429 {
		return 0, false
	}
	if ae.RetryAfter > 0 {
		return ae.RetryAfter, true
	}
	return 5 * time.Second, true
}

// IsCapacity reports whether err is a 409 scheduling-capacity rejection.
func IsCapacity(err error) bool {
	ae, ok := err.(*APIError)
	return ok && ae.Status == 409 && ae.ReasonCode != ""
}
