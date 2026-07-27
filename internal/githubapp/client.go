// Package githubapp is a minimal GitHub REST client covering exactly what an
// ephemeral-runner autoscaler needs: JIT runner configs, runner inventory,
// workflow-job status, queued-job discovery, and webhook verification. Built
// on the standard library to keep the dependency tree small.
package githubapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Lakr233/mini-control-action/internal/bootstrap"
	"github.com/Lakr233/mini-control-action/internal/config"
)

const userAgent = "mini-control-action/1.0"

// APIError is a non-2xx GitHub response with enough structure for callers to
// branch on (409 on jitconfig, 404 on delete, 403/429 rate limits).
type APIError struct {
	Status     int
	Body       string
	RetryAfter time.Duration // populated on 403/429 when the server says so
}

func (e *APIError) Error() string {
	return fmt.Sprintf("GitHub API %d: %s", e.Status, truncate(e.Body, 300))
}

// JobRef locates a workflow job. Job IDs are per-attempt: a re-run creates
// new job IDs under a higher run_attempt.
type JobRef struct {
	Repo       string // "owner/repo"
	JobID      int64
	RunID      int64
	RunAttempt int
	Labels     []string
}

// JobStatus is the live state of a workflow job. Job status values documented
// by GitHub: queued | in_progress | completed | waiting | requested | pending.
type JobStatus struct {
	Status     string
	Conclusion string // success | failure | cancelled | ...
	RunnerName string
	RunnerID   int64 // authoritative runner identity; 0 until a runner claims
	Steps      []JobStep
}

type JobStep struct {
	Name       string `json:"name"`
	Status     string `json:"status"` // steps really are queued|in_progress|completed
	Conclusion string `json:"conclusion"`
}

// FailedSteps lists steps that did not end well. steps[].conclusion has no
// documented enum, so this is a deny-list: anything other than success /
// skipped / neutral / empty counts as failed.
func (j JobStatus) FailedSteps() []string {
	var out []string
	for _, s := range j.Steps {
		switch s.Conclusion {
		case "", "success", "skipped", "neutral":
		default:
			out = append(out, s.Name)
		}
	}
	return out
}

// Runner is a self-hosted runner registration. Status values are documented
// only by example ("online"/"offline") — treat them as convention, not enum.
type Runner struct {
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Busy   bool   `json:"busy"`
}

// Reapable reports whether a registration is safe to garbage-collect: not
// verifiably online and not running a job. Inverted (!= "online") so an
// unknown future status value fails toward cleanup, not toward leaking.
func (r Runner) Reapable() bool { return r.Status != "online" && !r.Busy }

type Client struct {
	apiBase string
	// releasesBase serves the one public lookup (actions/runner releases),
	// which must not follow a GHE api_base_url. Tests override it.
	releasesBase string
	scope        string // all | repo | org
	owner        string
	repo         string
	groupID      int64
	token        string
	httpc        *http.Client
	log          *slog.Logger

	repoMu    sync.Mutex
	repoCache []RepoInfo
	repoAt    time.Time

	runMu      sync.Mutex
	runUpdated map[int64]string // run ID -> last seen updated_at; gates /jobs fetches
}

// repoCacheTTL bounds how stale the discovered repo list may be. /user/repos
// pagination is the dominant request cost, so this is deliberately much
// longer than the poll interval; webhooks remain the low-latency path.
const repoCacheTTL = 5 * time.Minute

// RepoInfo is one discovered repository.
type RepoInfo struct {
	FullName string
	PushedAt time.Time
}

func New(cfg config.GitHub, log *slog.Logger) (*Client, error) {
	return &Client{
		apiBase:      strings.TrimRight(cfg.APIBaseURL, "/"),
		releasesBase: "https://api.github.com",
		scope:        cfg.Scope,
		owner:        cfg.Owner,
		repo:         cfg.Repo,
		groupID:      cfg.RunnerGroupID,
		token:        cfg.Auth.Token,
		httpc:        &http.Client{Timeout: 30 * time.Second},
		log:          log,
		runUpdated:   map[int64]string{},
	}, nil
}

// SetReleasesBaseForTest points the actions/runner release lookup at a fake
// server.
func (c *Client) SetReleasesBaseForTest(base string) {
	c.releasesBase = strings.TrimRight(base, "/")
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	return c.doAt(ctx, c.apiBase, method, path, body, out)
}

func (c *Client) doAt(ctx context.Context, base, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", userAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode >= 300 {
		ae := &APIError{Status: resp.StatusCode, Body: string(data)}
		if resp.StatusCode == 403 || resp.StatusCode == 429 {
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, err := strconv.Atoi(ra); err == nil {
					ae.RetryAfter = time.Duration(secs) * time.Second
				}
			} else if resp.Header.Get("x-ratelimit-remaining") == "0" {
				if reset, err := strconv.ParseInt(resp.Header.Get("x-ratelimit-reset"), 10, 64); err == nil {
					ae.RetryAfter = time.Until(time.Unix(reset, 0))
				}
			}
		}
		return ae
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// runnersBase returns the actions/runners path for a repo ("owner/repo").
// Org scope registers runners at the org level regardless of repo.
func (c *Client) runnersBase(repo string) string {
	if c.scope == "org" {
		return "/orgs/" + c.owner + "/actions/runners"
	}
	if repo == "" {
		repo = c.owner + "/" + c.repo
	}
	return "/repos/" + repo + "/actions/runners"
}

// ListAccessibleRepos discovers repositories visible to the token, most
// recently pushed first. NOTE: /user/repos reflects the USER's access, which
// for public repos is wider than what the token can administer — treat the
// result as candidates, not an allowlist. Cached briefly. Repos that have
// never been pushed (null pushed_at) are skipped.
func (c *Client) ListAccessibleRepos(ctx context.Context) ([]RepoInfo, error) {
	c.repoMu.Lock()
	defer c.repoMu.Unlock()
	if c.repoCache != nil && time.Since(c.repoAt) < repoCacheTTL {
		return c.repoCache, nil
	}
	var infos []RepoInfo
	for page := 1; page <= 10; page++ {
		var repos []struct {
			FullName string     `json:"full_name"`
			Archived bool       `json:"archived"`
			PushedAt *time.Time `json:"pushed_at"`
		}
		path := fmt.Sprintf("/user/repos?per_page=100&page=%d&sort=pushed&direction=desc", page)
		if err := c.do(ctx, http.MethodGet, path, nil, &repos); err != nil {
			return nil, err
		}
		for _, r := range repos {
			if !r.Archived && r.PushedAt != nil {
				infos = append(infos, RepoInfo{FullName: r.FullName, PushedAt: *r.PushedAt})
			}
		}
		if len(repos) < 100 {
			break
		}
	}
	c.repoCache = infos
	c.repoAt = time.Now()
	return infos, nil
}

// GenerateJITConfig registers a just-in-time ephemeral runner on repo
// ("owner/repo"; ignored for org scope) and returns the encoded JIT blob
// (passed verbatim to `run.sh --jitconfig`) plus runner ID. The registration
// exists as soon as this call succeeds; JIT runners run at most one job and
// are auto-deregistered afterwards.
func (c *Client) GenerateJITConfig(ctx context.Context, repo, name string, labels []string, workDir string) (string, int64, error) {
	body := map[string]any{
		"name":            name,
		"labels":          labels,
		"runner_group_id": c.groupID, // required by the API at BOTH repo and org scope
	}
	if workDir != "" {
		body["work_folder"] = workDir // relative to the runner install directory
	}
	var out struct {
		Runner           Runner `json:"runner"`
		EncodedJITConfig string `json:"encoded_jit_config"`
	}
	if err := c.do(ctx, http.MethodPost, c.runnersBase(repo)+"/generate-jitconfig", body, &out); err != nil {
		return "", 0, err
	}
	return out.EncodedJITConfig, out.Runner.ID, nil
}

// ListRunners returns ALL self-hosted runners registered on repo (org-wide
// for org scope), following pagination. A truncated listing is returned as an
// error, never as a short list — callers infer "runner exited" from absence,
// so silent truncation would tear down live VMs.
func (c *Client) ListRunners(ctx context.Context, repo string) ([]Runner, error) {
	base := c.runnersBase(repo)
	var all []Runner
	for page := 1; page <= 20; page++ {
		var out struct {
			Runners []Runner `json:"runners"`
		}
		path := fmt.Sprintf("%s?per_page=100&page=%d", base, page)
		if err := c.do(ctx, http.MethodGet, path, nil, &out); err != nil {
			return nil, err
		}
		all = append(all, out.Runners...)
		if len(out.Runners) < 100 {
			return all, nil
		}
	}
	return nil, fmt.Errorf("runner listing exceeded page cap")
}

// RemoveRunner deregisters a runner by ID. Documented responses are 204 and
// 422 ("validation failed or spammed") — the API does NOT promise to refuse
// deleting a busy runner, so callers must verify idleness first. A 404
// (already gone) is success.
func (c *Client) RemoveRunner(ctx context.Context, repo string, id int64) error {
	err := c.do(ctx, http.MethodDelete, fmt.Sprintf("%s/%d", c.runnersBase(repo), id), nil, nil)
	if ae, ok := err.(*APIError); ok && ae.Status == 404 {
		return nil
	}
	return err
}

// GetJobStatus fetches the live status of a workflow job.
func (c *Client) GetJobStatus(ctx context.Context, ref JobRef) (JobStatus, error) {
	var out struct {
		Status     string    `json:"status"`
		Conclusion string    `json:"conclusion"`
		RunnerName string    `json:"runner_name"`
		RunnerID   int64     `json:"runner_id"`
		Steps      []JobStep `json:"steps"`
	}
	path := fmt.Sprintf("/repos/%s/actions/jobs/%d", ref.Repo, ref.JobID)
	if err := c.do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return JobStatus{}, err
	}
	return JobStatus{Status: out.Status, Conclusion: out.Conclusion, RunnerName: out.RunnerName, RunnerID: out.RunnerID, Steps: out.Steps}, nil
}

// ListQueuedJobs discovers queued workflow jobs whose labels our runners can
// satisfy — the poller's heal path. Candidate runs are gathered from BOTH
// "queued" and "in_progress" run statuses: run status is an aggregate, and a
// multi-job/matrix run flips to in_progress while sibling jobs still queue.
// (waiting/pending/requested runs are excluded on purpose — those are
// pre-runner gating states.) A run's /jobs listing is only fetched when its
// updated_at moved since the last cycle, so long-lived in_progress runs cost
// nothing in steady state. One broken repo does not abort the sweep.
func (c *Client) ListQueuedJobs(ctx context.Context, repos []string, ourLabels []string) ([]JobRef, error) {
	var refs []JobRef
	liveRuns := map[int64]bool{}
	for _, repo := range repos {
		repoRefs, err := c.listQueuedJobsInRepo(ctx, repo, ourLabels, liveRuns)
		if err != nil {
			if ctx.Err() != nil {
				return nil, err
			}
			c.log.Warn("queued-job discovery failed for repo; skipping it this cycle", "repo", repo, "error", err)
			continue
		}
		refs = append(refs, repoRefs...)
	}
	// Forget runs that are no longer live so the gate map cannot grow forever.
	c.runMu.Lock()
	for id := range c.runUpdated {
		if !liveRuns[id] {
			delete(c.runUpdated, id)
		}
	}
	c.runMu.Unlock()
	return refs, nil
}

func (c *Client) listQueuedJobsInRepo(ctx context.Context, repo string, ourLabels []string, liveRuns map[int64]bool) ([]JobRef, error) {
	var refs []JobRef
	for _, st := range []string{"queued", "in_progress"} {
		for page := 1; page <= 5; page++ {
			var runs struct {
				WorkflowRuns []struct {
					ID        int64  `json:"id"`
					UpdatedAt string `json:"updated_at"`
				} `json:"workflow_runs"`
			}
			path := fmt.Sprintf("/repos/%s/actions/runs?status=%s&per_page=100&page=%d", repo, st, page)
			if err := c.do(ctx, http.MethodGet, path, nil, &runs); err != nil {
				return nil, err
			}
			for _, run := range runs.WorkflowRuns {
				if liveRuns[run.ID] {
					continue // already handled under the other status
				}
				liveRuns[run.ID] = true
				c.runMu.Lock()
				unchanged := c.runUpdated[run.ID] == run.UpdatedAt && run.UpdatedAt != ""
				c.runMu.Unlock()
				if unchanged {
					continue // nothing new since last cycle; skip the /jobs fetch
				}
				jobRefs, err := c.listQueuedJobsInRun(ctx, repo, run.ID, ourLabels)
				if err != nil {
					return nil, err
				}
				refs = append(refs, jobRefs...)
				c.runMu.Lock()
				c.runUpdated[run.ID] = run.UpdatedAt
				c.runMu.Unlock()
			}
			if len(runs.WorkflowRuns) < 100 {
				break
			}
		}
	}
	return refs, nil
}

func (c *Client) listQueuedJobsInRun(ctx context.Context, repo string, runID int64, ourLabels []string) ([]JobRef, error) {
	var refs []JobRef
	for page := 1; page <= 3; page++ {
		var jobs struct {
			Jobs []struct {
				ID         int64    `json:"id"`
				RunID      int64    `json:"run_id"`
				RunAttempt int      `json:"run_attempt"`
				Status     string   `json:"status"`
				Labels     []string `json:"labels"`
			} `json:"jobs"`
		}
		path := fmt.Sprintf("/repos/%s/actions/runs/%d/jobs?filter=latest&per_page=100&page=%d", repo, runID, page)
		if err := c.do(ctx, http.MethodGet, path, nil, &jobs); err != nil {
			return nil, err
		}
		for _, j := range jobs.Jobs {
			if j.Status != "queued" || !LabelsSatisfiable(j.Labels, ourLabels) {
				continue
			}
			refs = append(refs, JobRef{
				Repo: repo, JobID: j.ID, RunID: j.RunID, RunAttempt: j.RunAttempt, Labels: j.Labels,
			})
		}
		if len(jobs.Jobs) < 100 {
			break
		}
	}
	return refs, nil
}

var runnerVersionRe = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

// ResolveRunnerDownload resolves the newest actions/runner release and the
// target tarball URL from the release's own asset list (never by string
// convention — releases can exist before assets finish uploading). Always
// queries the public GitHub API regardless of api_base_url. Platform naming
// belongs to the bootstrap package.
func (c *Client) ResolveRunnerDownload(ctx context.Context) (version, url string, err error) {
	var out struct {
		TagName string `json:"tag_name"`
		Assets  []struct {
			Name               string `json:"name"`
			BrowserDownloadURL string `json:"browser_download_url"`
		} `json:"assets"`
	}
	if err := c.doAt(ctx, c.releasesBase, http.MethodGet, "/repos/actions/runner/releases/latest", nil, &out); err != nil {
		return "", "", err
	}
	version = strings.TrimPrefix(out.TagName, "v")
	if !runnerVersionRe.MatchString(version) {
		return "", "", fmt.Errorf("unexpected actions/runner release tag %q", out.TagName)
	}
	want := bootstrap.AssetName(version)
	for _, a := range out.Assets {
		if a.Name == want {
			return version, a.BrowserDownloadURL, nil
		}
	}
	return "", "", fmt.Errorf("release v%s has no %s asset yet (assets may still be uploading)", version, want)
}

// LabelsSatisfiable reports whether every label the job demands is offered by
// our runners (job labels ⊆ ours, case-insensitive) and the job actually
// targets self-hosted. NOTE: GitHub routes on labels AND runner groups; we
// assume all watched repos route to the configured group.
func LabelsSatisfiable(jobLabels, ourLabels []string) bool {
	if len(jobLabels) == 0 {
		return false
	}
	ours := map[string]bool{}
	for _, l := range ourLabels {
		ours[strings.ToLower(l)] = true
	}
	for _, l := range jobLabels {
		if !ours[strings.ToLower(l)] {
			return false
		}
	}
	return true
}
