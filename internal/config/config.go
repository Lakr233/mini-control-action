// Package config loads and validates the service configuration from a single
// YAML file. Secrets may be referenced as ${ENV_VAR}; only the ${...} form is
// expanded so plain $ characters in values survive.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration so YAML values like "90s" or "12m" parse.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) D() time.Duration { return time.Duration(d) }

type Config struct {
	Log         Log         `yaml:"log"`
	Server      Server      `yaml:"server"`
	GitHub      GitHub      `yaml:"github"`
	MiniControl MiniControl `yaml:"minicontrol"`
	Runner      Runner      `yaml:"runner"`
	Limits      Limits      `yaml:"limits"`
	Reconciler  Reconciler  `yaml:"reconciler"`
	State       State       `yaml:"state"`
	Debug       Debug       `yaml:"debug"`
}

type Log struct {
	Level  string `yaml:"level"`  // debug|info|warn|error
	Format string `yaml:"format"` // json|text
}

type Server struct {
	Listen string `yaml:"listen"` // /healthz and /status only
}

type GitHub struct {
	APIBaseURL string `yaml:"api_base_url"` // default https://api.github.com; override for GHE/tests
	// Scope selects where runners are registered and which repos are watched:
	//   all  - every repo the token can access (discovered via /user/repos);
	//          runners are registered per-job on the job's own repo
	//   repo - a single owner/repo
	//   org  - org-level runners shared across the org (poll.repos lists what
	//          to watch)
	Scope         string     `yaml:"scope"`
	Owner         string     `yaml:"owner"`
	Repo          string     `yaml:"repo"`
	RunnerGroupID int64      `yaml:"runner_group_id"`
	Auth          GitHubAuth `yaml:"auth"`
	Poll          Poll       `yaml:"poll"`
}

type GitHubAuth struct {
	// Token is a GitHub personal access token. Classic: `repo` scope (repo
	// runners) or `admin:org` (org runners); fine-grained: Administration
	// read/write + Actions read. The owning account needs admin permission
	// on the target repo/org.
	Token string `yaml:"token"`
}

type Poll struct {
	Interval Duration `yaml:"interval"`
	// Repos lists "owner/repo" names to poll for queued jobs when scope is
	// org; ignored for repo scope (the configured repo is polled).
	Repos []string `yaml:"repos"`
	// ActiveWindow/MaxRepos bound polling for scope "all": only repos pushed
	// within the window (newest first, capped) are polled each cycle, so a
	// broad token doesn't blow the API rate budget.
	ActiveWindow Duration `yaml:"active_window"`
	MaxRepos     int      `yaml:"max_repos"`
}

type MiniControl struct {
	BaseURL          string   `yaml:"base_url"`
	APIKey           string   `yaml:"api_key"`
	SKU              string   `yaml:"sku"`
	WorkerTag        string   `yaml:"worker_tag"`
	PollInterval     Duration `yaml:"poll_interval"`
	ProvisionTimeout Duration `yaml:"provision_timeout"`
	RequestTimeout   Duration `yaml:"request_timeout"`
}

type Runner struct {
	Labels             []string `yaml:"labels"`
	Version            string   `yaml:"version"`      // "latest" or pinned "2.325.0"
	DownloadURL        string   `yaml:"download_url"` // optional mirror template with {version}
	PreinstalledPath   string   `yaml:"preinstalled_path"`
	NamePrefix         string   `yaml:"name_prefix"`
	WorkDir            string   `yaml:"work_dir"`
	StatusPollInterval Duration `yaml:"status_poll_interval"`
	PickupTimeout      Duration `yaml:"pickup_timeout"`
	MaxJobDuration     Duration `yaml:"max_job_duration"`
}

type Limits struct {
	MaxConcurrentVMs int     `yaml:"max_concurrent_vms"`
	MaxRetriesPerJob int     `yaml:"max_retries_per_job"`
	CapacityBackoff  Backoff `yaml:"capacity_backoff"`
}

type Backoff struct {
	Initial Duration `yaml:"initial"`
	Max     Duration `yaml:"max"`
}

type Reconciler struct {
	Interval    Duration `yaml:"interval"`
	OrphanGrace Duration `yaml:"orphan_grace"`
}

type State struct {
	Path string `yaml:"path"`
}

type Debug struct {
	StreamBootstrapLogs bool `yaml:"stream_bootstrap_logs"`
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

func expandEnv(data []byte) []byte {
	return envRef.ReplaceAllFunc(data, func(m []byte) []byte {
		name := envRef.FindSubmatch(m)[1]
		return []byte(os.Getenv(string(name)))
	})
}

// Load reads, expands, defaults, and validates the config at path.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	dec := yaml.NewDecoder(strings.NewReader(string(expandEnv(raw))))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return cfg, nil
}

func (c *Config) applyDefaults() {
	def := func(d *Duration, v time.Duration) {
		if *d == 0 {
			*d = Duration(v)
		}
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "json"
	}
	if c.Server.Listen == "" {
		c.Server.Listen = ":8080"
	}
	if c.GitHub.APIBaseURL == "" {
		c.GitHub.APIBaseURL = "https://api.github.com"
	}
	if c.GitHub.Scope == "" {
		c.GitHub.Scope = "all"
	}
	if c.GitHub.RunnerGroupID == 0 {
		c.GitHub.RunnerGroupID = 1
	}
	def(&c.GitHub.Poll.Interval, 60*time.Second)
	def(&c.GitHub.Poll.ActiveWindow, 2*time.Hour)
	if c.GitHub.Poll.MaxRepos == 0 {
		c.GitHub.Poll.MaxRepos = 10
	}
	def(&c.MiniControl.PollInterval, 10*time.Second)
	def(&c.MiniControl.ProvisionTimeout, 12*time.Minute)
	def(&c.MiniControl.RequestTimeout, 30*time.Second)
	if len(c.Runner.Labels) == 0 {
		c.Runner.Labels = []string{"self-hosted", "macos", "arm64", "mini-control"}
	}
	if c.Runner.Version == "" {
		c.Runner.Version = "latest"
	}
	if c.Runner.NamePrefix == "" {
		c.Runner.NamePrefix = "mc"
	}
	if c.Runner.WorkDir == "" {
		// work_folder is documented as relative to the runner install dir.
		c.Runner.WorkDir = "_work"
	}
	def(&c.Runner.StatusPollInterval, 20*time.Second)
	def(&c.Runner.PickupTimeout, 10*time.Minute)
	def(&c.Runner.MaxJobDuration, 2*time.Hour)
	if c.Limits.MaxConcurrentVMs == 0 {
		c.Limits.MaxConcurrentVMs = 4
	}
	if c.Limits.MaxRetriesPerJob == 0 {
		c.Limits.MaxRetriesPerJob = 2
	}
	def(&c.Limits.CapacityBackoff.Initial, 30*time.Second)
	def(&c.Limits.CapacityBackoff.Max, 5*time.Minute)
	def(&c.Reconciler.Interval, 60*time.Second)
	def(&c.Reconciler.OrphanGrace, 3*time.Minute)
	if c.State.Path == "" {
		c.State.Path = "/var/lib/minicontrol-runner/state.json"
	}
}

func (c *Config) Validate() error {
	var errs []error
	bad := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		bad("log.level must be debug|info|warn|error, got %q", c.Log.Level)
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		bad("log.format must be json|text, got %q", c.Log.Format)
	}
	switch c.GitHub.Scope {
	case "all":
	case "repo":
		if c.GitHub.Owner == "" || c.GitHub.Repo == "" {
			bad("github.owner and github.repo are required when github.scope is repo")
		}
	case "org":
		if c.GitHub.Owner == "" {
			bad("github.owner is required when github.scope is org")
		}
	default:
		bad("github.scope must be all|repo|org, got %q", c.GitHub.Scope)
	}
	if c.GitHub.Auth.Token == "" {
		bad("github.auth.token is required")
	}
	if c.GitHub.Scope == "org" && len(c.GitHub.Poll.Repos) == 0 {
		bad("github.poll.repos is required for org scope")
	}
	if c.MiniControl.BaseURL == "" {
		bad("minicontrol.base_url is required")
	}
	if !strings.HasPrefix(c.MiniControl.APIKey, "mck_") {
		bad("minicontrol.api_key must be a mck_-prefixed key (got %d bytes)", len(c.MiniControl.APIKey))
	}
	if c.MiniControl.SKU == "" {
		bad("minicontrol.sku is required")
	}
	hasSelfHosted := false
	for _, l := range c.Runner.Labels {
		if l == "self-hosted" {
			hasSelfHosted = true
		}
	}
	if !hasSelfHosted {
		bad("runner.labels must include \"self-hosted\"")
	}
	if len(c.Runner.Labels) > 100 {
		bad("runner.labels must contain at most 100 labels (got %d)", len(c.Runner.Labels))
	}
	if c.Limits.MaxConcurrentVMs < 1 {
		bad("limits.max_concurrent_vms must be >= 1")
	}
	return errors.Join(errs...)
}

// RepoFullName returns "owner/repo" for repo scope, or "" for org scope.
func (g GitHub) RepoFullName() string {
	if g.Scope == "repo" {
		return g.Owner + "/" + g.Repo
	}
	return ""
}
