package githubapp

import (
	"context"
	"log/slog"
	"time"
)

// JobEvent is a normalized workflow_job signal. The poller emits "queued";
// "in_progress"/"completed" exist for internal signaling and tests.
// ("waiting" — environment protection gating — is dropped at Submit.)
type JobEvent struct {
	Action     string
	Job        JobRef
	Conclusion string
	RunnerName string
}

// Poller periodically lists queued jobs matching our labels and submits
// them as queued events — the sole discovery path (pull-only, no ingress).
type Poller struct {
	Client *Client
	// Repos to watch ("owner/repo"). Ignored when DiscoverRepos is set.
	Repos []string
	// DiscoverRepos watches repos the token can access. To stay inside API
	// rate limits with broad tokens, only repos pushed within ActiveWindow
	// are polled (capped at MaxRepos) — push-triggered jobs always land in
	// that set; manually dispatched runs on long-quiet repos are caught by
	// the webhook instead.
	DiscoverRepos bool
	ActiveWindow  time.Duration
	MaxRepos      int
	OurLabels     []string
	Interval      time.Duration
	Submit        func(JobEvent)
	Log           *slog.Logger
}

func (p *Poller) Run(ctx context.Context) {
	t := time.NewTicker(p.Interval)
	defer t.Stop()
	for {
		p.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (p *Poller) poll(ctx context.Context) {
	repos := p.Repos
	if p.DiscoverRepos {
		infos, err := p.Client.ListAccessibleRepos(ctx)
		if err != nil {
			if ctx.Err() == nil {
				p.Log.Warn("repo discovery failed", "error", err)
			}
			return
		}
		cutoff := time.Now().Add(-p.ActiveWindow)
		repos = repos[:0:0]
		for _, ri := range infos { // already sorted most-recently-pushed first
			if ri.PushedAt.Before(cutoff) {
				break
			}
			repos = append(repos, ri.FullName)
			if len(repos) >= p.MaxRepos {
				break
			}
		}
	}
	refs, err := p.Client.ListQueuedJobs(ctx, repos, p.OurLabels)
	if err != nil {
		if ctx.Err() == nil {
			p.Log.Warn("github poll failed", "error", err)
		}
		return
	}
	for _, ref := range refs {
		p.Submit(JobEvent{Action: "queued", Job: ref})
	}
}
