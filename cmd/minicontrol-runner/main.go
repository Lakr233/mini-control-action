// minicontrol-runner is a headless service that provisions ephemeral GitHub
// Actions runners on mini-control macOS VMs: one VM per queued job, deleted
// as soon as the job completes.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Lakr233/mini-control-action/internal/config"
	"github.com/Lakr233/mini-control-action/internal/githubapp"
	"github.com/Lakr233/mini-control-action/internal/httpserver"
	"github.com/Lakr233/mini-control-action/internal/logging"
	"github.com/Lakr233/mini-control-action/internal/minicontrol"
	"github.com/Lakr233/mini-control-action/internal/reconciler"
	"github.com/Lakr233/mini-control-action/internal/scaler"
	"github.com/Lakr233/mini-control-action/internal/state"
)

func main() {
	var (
		configPath = flag.String("config", "/etc/minicontrol-runner/config.yaml", "path to config.yaml")
		dryRun     = flag.Bool("dry-run", false, "validate config and connectivity, then exit (no VM is created)")
		smoke      = flag.Bool("smoke", false, "create one VM, run 'uname -a', delete it, then exit (BILLS REAL MONEY)")
		smokeSKU   = flag.String("smoke-sku", "m4-tiny-v1", "SKU used by --smoke")
		healthURL  = flag.String("healthcheck", "", "probe the given /healthz URL and exit (container healthcheck)")
	)
	flag.Parse()

	if *healthURL != "" {
		resp, err := http.Get(*healthURL)
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		os.Exit(0)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(1)
	}
	log := logging.New(cfg.Log.Level, cfg.Log.Format)

	mc := minicontrol.New(cfg.MiniControl.BaseURL, cfg.MiniControl.APIKey, cfg.MiniControl.RequestTimeout.D(), log)
	gh, err := githubapp.New(cfg.GitHub, log)
	if err != nil {
		log.Error("github client init failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *dryRun {
		os.Exit(runDryRun(ctx, cfg, mc, gh))
	}
	if *smoke {
		os.Exit(runSmoke(ctx, cfg, mc, *smokeSKU))
	}

	store, err := state.Open(cfg.State.Path, log)
	if err != nil {
		log.Error("open state store", "path", cfg.State.Path, "error", err)
		os.Exit(1)
	}

	sc := scaler.New(ctx, cfg, mc, gh, store, log)
	rec := &reconciler.Reconciler{Cfg: cfg, MC: mc, GH: gh, Store: store, Scaler: sc, Log: log}
	srv := httpserver.New(cfg, store, log)

	log.Info("minicontrol-runner starting",
		"listen", cfg.Server.Listen,
		"sku", cfg.MiniControl.SKU,
		"worker_tag", cfg.MiniControl.WorkerTag,
		"labels", cfg.Runner.Labels,
		"max_vms", cfg.Limits.MaxConcurrentVMs)

	// Resume any interrupted lifecycles before accepting new work.
	sc.Resume()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		rec.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		if err := httpserver.Run(ctx, srv, log); err != nil && ctx.Err() == nil {
			log.Error("http server failed", "error", err)
			stop()
		}
	}()
	repos := cfg.GitHub.Poll.Repos
	if cfg.GitHub.Scope == "repo" {
		repos = []string{cfg.GitHub.RepoFullName()}
	}
	p := &githubapp.Poller{
		Client: gh, Repos: repos, DiscoverRepos: cfg.GitHub.Scope == "all",
		ActiveWindow: cfg.GitHub.Poll.ActiveWindow.D(), MaxRepos: cfg.GitHub.Poll.MaxRepos,
		OurLabels: cfg.Runner.Labels,
		Interval:  cfg.GitHub.Poll.Interval.D(), Submit: sc.Submit, Log: log,
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Run(ctx)
	}()

	<-ctx.Done()
	log.Info("shutting down: waiting for job workers to reach a safe point " +
		"(running VMs are kept; they resume on next start)")
	sc.Wait()
	wg.Wait()
	log.Info("bye")
}

func runDryRun(ctx context.Context, cfg *config.Config, mc *minicontrol.Client, gh *githubapp.Client) int {
	fmt.Println("== dry run ==")
	skus, err := mc.ListSKUs(ctx)
	if err != nil {
		fmt.Println("mini-control: FAILED:", err)
		return 1
	}
	found := false
	for _, s := range skus {
		mark := " "
		if s.ID == cfg.MiniControl.SKU {
			mark = "*"
			found = true
			if !s.Available {
				fmt.Printf("mini-control: WARNING: configured sku %s is currently unavailable\n", s.ID)
				// Availability mirrors UNTAGGED placement, which excludes
				// workers the server fences off by tag suffix (e.g. "*-test").
				// A tagged create can still succeed here.
				if cfg.MiniControl.WorkerTag != "" {
					fmt.Printf("mini-control: NOTE: availability ignores worker_tag %q; "+
						"tagged creates may still be placed\n", cfg.MiniControl.WorkerTag)
				}
			}
		}
		fmt.Printf("  %s %-14s %d cpu / %5d MiB / %d GiB  available=%v  %.4f %s/h\n",
			mark, s.ID, s.CPUCores, s.MemoryMiB, s.DiskGiB, s.Available, s.Price.CostFactor, s.Price.Currency)
	}
	if !found {
		fmt.Printf("mini-control: FAILED: configured sku %q not offered by server\n", cfg.MiniControl.SKU)
		return 1
	}
	fmt.Println("mini-control: OK")

	if cfg.GitHub.Scope == "all" {
		repos, err := gh.ListAccessibleRepos(ctx)
		if err != nil {
			fmt.Println("github: FAILED:", err)
			return 1
		}
		if len(repos) == 0 {
			fmt.Println("github: WARNING: token sees zero repositories — a fine-grained PAT covers one owner; check its repository selection")
			return 1
		}
		cutoff := time.Now().Add(-cfg.GitHub.Poll.ActiveWindow.D())
		active := 0
		for _, r := range repos {
			if r.PushedAt.After(cutoff) {
				active++
			}
		}
		fmt.Printf("github: OK — token sees %d repo(s), %d pushed within %s (polled set, newest first):\n",
			len(repos), active, cfg.GitHub.Poll.ActiveWindow.D())
		for i, r := range repos {
			if i >= 15 {
				fmt.Printf("    ... and %d more\n", len(repos)-i)
				break
			}
			mark := " "
			if r.PushedAt.After(cutoff) {
				mark = "*"
			}
			fmt.Printf("  %s %s (pushed %s)\n", mark, r.FullName, r.PushedAt.Format("2006-01-02 15:04"))
		}
	} else {
		runners, err := gh.ListRunners(ctx, cfg.GitHub.RepoFullName())
		if err != nil {
			fmt.Println("github: FAILED:", err)
			return 1
		}
		fmt.Printf("github: OK (%d self-hosted runners registered in scope)\n", len(runners))
	}
	if v, u, err := gh.ResolveRunnerDownload(ctx); err == nil {
		fmt.Printf("github: latest actions/runner release: %s (%s)\n", v, u)
	}
	fmt.Println("dry run passed")
	return 0
}

// runSmoke exercises the real create path, including the configured worker
// tag — a smoke test that skipped the placement constraint would not smoke
// out the thing most likely to be misconfigured.
func runSmoke(ctx context.Context, cfg *config.Config, mc *minicontrol.Client, sku string) int {
	tag := cfg.MiniControl.WorkerTag
	fmt.Printf("== smoke test: create %s (tag %q), exec, delete (bills real money) ==\n", sku, tag)
	idem := fmt.Sprintf("mcra-smoke-%d", time.Now().UnixNano())
	vm, err := mc.CreateVM(ctx, minicontrol.CreateVMRequest{SKU: sku, Tag: tag}, idem)
	if err != nil {
		fmt.Println("create: FAILED:", err)
		return 1
	}
	fmt.Printf("created vm %s (%s) on worker %q tags=%v\n", vm.ID, vm.Status, vm.WorkerName, vm.WorkerTags)
	// Registered before any other check: from here on every exit must delete
	// the VM, or it bills until someone notices.
	defer func() {
		dctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := mc.DeleteVM(dctx, vm.ID); err != nil {
			fmt.Println("delete: FAILED (clean up manually!):", err)
		} else {
			fmt.Println("deleted vm", vm.ID)
		}
	}()
	if tag != "" && !slices.ContainsFunc(vm.WorkerTags, func(t string) bool { return strings.EqualFold(t, tag) }) {
		fmt.Printf("placement: FAILED: worker %q does not carry requested tag %q\n", vm.WorkerName, tag)
		return 1
	}
	for {
		cur, err := mc.GetVM(ctx, vm.ID)
		if err != nil {
			fmt.Println("poll: FAILED:", err)
			return 1
		}
		fmt.Println("status:", cur.Status)
		if cur.Status == minicontrol.StatusReady {
			break
		}
		if cur.Status == minicontrol.StatusError {
			fmt.Println("vm entered error state:", cur.LastError)
			return 1
		}
		select {
		case <-ctx.Done():
			return 1
		case <-time.After(10 * time.Second):
		}
	}
	res, err := mc.Exec(ctx, vm.ID, minicontrol.ExecRequest{Command: "uname -a && sw_vers", TimeoutSeconds: 60})
	if err != nil {
		fmt.Println("exec: FAILED:", err)
		return 1
	}
	fmt.Printf("exec exit=%d\n%s%s\n", res.ExitCode, res.Stdout, res.Stderr)
	if res.ExitCode != 0 {
		return 1
	}
	fmt.Println("smoke test passed")
	return 0
}
