// Package reconciler is the safety net that guarantees no mini-control VM
// outlives its job (billing runs until deletion is confirmed). It sweeps
// periodically and at startup:
//
//  1. Orphan VMs — visible via GET /vms but unknown to the state file —
//     are deleted after a grace period. This assumes the API key is
//     DEDICATED to this service, which the docs require.
//  2. Records stuck in deleting have their DELETE re-issued.
//  3. Unowned non-terminal records are re-adopted by the scaler.
//  4. Offline runner registrations matching our name prefix with no live VM
//     are deregistered from GitHub.
//  5. Terminal records older than the retention window are pruned.
package reconciler

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/Lakr233/mini-control-action/internal/config"
	"github.com/Lakr233/mini-control-action/internal/githubapp"
	"github.com/Lakr233/mini-control-action/internal/minicontrol"
	"github.com/Lakr233/mini-control-action/internal/scaler"
	"github.com/Lakr233/mini-control-action/internal/state"
)

const terminalRetention = 24 * time.Hour

type Reconciler struct {
	Cfg    *config.Config
	MC     *minicontrol.Client
	GH     *githubapp.Client
	Store  *state.Store
	Scaler *scaler.Scaler
	Log    *slog.Logger

	firstSeen map[string]time.Time // orphan candidate -> first sighting
}

func (r *Reconciler) Run(ctx context.Context) {
	t := time.NewTicker(r.Cfg.Reconciler.Interval.D())
	defer t.Stop()
	for {
		r.Sweep(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Sweep runs one reconciliation pass.
func (r *Reconciler) Sweep(ctx context.Context) {
	if r.firstSeen == nil {
		r.firstSeen = map[string]time.Time{}
	}
	vms, err := r.MC.ListVMs(ctx)
	if err != nil {
		if ctx.Err() == nil {
			r.Log.Warn("reconciler: list vms failed", "error", err)
		}
		return
	}
	live := map[string]bool{}
	for _, vm := range vms {
		live[vm.ID] = true
	}

	// 1. Orphan VMs.
	now := time.Now()
	for _, vm := range vms {
		if vm.Status == minicontrol.StatusDeleting || r.Store.HasVM(vm.ID) {
			delete(r.firstSeen, vm.ID)
			continue
		}
		seen, ok := r.firstSeen[vm.ID]
		if !ok {
			r.firstSeen[vm.ID] = now
			continue
		}
		if now.Sub(seen) >= r.Cfg.Reconciler.OrphanGrace.D() {
			r.Log.Warn("deleting orphan vm (no state record; is the API key really dedicated to this service?)",
				"vm_id", vm.ID, "sku", vm.SKU, "first_seen", seen)
			if err := r.MC.DeleteVM(ctx, vm.ID); err != nil {
				r.Log.Warn("orphan delete failed", "vm_id", vm.ID, "error", err)
			} else {
				delete(r.firstSeen, vm.ID)
			}
		}
	}
	// Forget orphan candidates that disappeared on their own.
	for id := range r.firstSeen {
		if !live[id] {
			delete(r.firstSeen, id)
		}
	}

	// 2–3. Walk records.
	for _, rec := range r.Store.List() {
		switch {
		case rec.State.Terminal():
			if now.Sub(rec.UpdatedAt) > terminalRetention {
				_ = r.Store.Delete(rec.Key())
			}
		case rec.State == state.StateDeleting || rec.State == state.StateJobDone:
			if r.Scaler.Active(rec.Key()) {
				continue // a live worker owns this record; don't race it
			}
			if rec.VMID != "" && live[rec.VMID] {
				r.Log.Info("reconciler: re-issuing delete for stuck vm", "vm_id", rec.VMID, "job_id", rec.JobID)
				if err := r.MC.DeleteVM(ctx, rec.VMID); err != nil {
					r.Log.Warn("reconciler: delete failed", "vm_id", rec.VMID, "error", err)
					continue
				}
			}
			rec.State = state.StateComplete
			_ = r.Store.Upsert(rec)
		default:
			if !r.Scaler.Active(rec.Key()) {
				r.Log.Info("reconciler: re-adopting unowned job", "job_id", rec.JobID, "state", rec.State)
				r.Scaler.ResumeRecord(rec)
			}
		}
	}

	// 4. Stale runner registrations.
	r.cleanRunners(ctx, live)
}

func (r *Reconciler) cleanRunners(ctx context.Context, liveVMs map[string]bool) {
	for _, repo := range r.runnerScopes(ctx) {
		runners, err := r.GH.ListRunners(ctx, repo)
		if err != nil {
			if ctx.Err() == nil {
				r.Log.Warn("reconciler: list runners failed", "repo", repo, "error", err)
			}
			continue
		}
		prefix := r.Cfg.Runner.NamePrefix + "-"
		for _, rn := range runners {
			// Reapable is non-exhaustive on purpose: runner status has no
			// documented enum, so anything not verifiably online and not
			// busy fails toward cleanup rather than leaking.
			if !strings.HasPrefix(rn.Name, prefix) || !rn.Reapable() {
				continue
			}
			// Name format: <prefix>-<vmid8>-j<jobid>; keep it if its VM is alive.
			rest := strings.TrimPrefix(rn.Name, prefix)
			vmid8, _, ok := strings.Cut(rest, "-j")
			if !ok {
				continue
			}
			alive := false
			for id := range liveVMs {
				if strings.HasPrefix(id, vmid8) {
					alive = true
					break
				}
			}
			if alive {
				continue
			}
			r.Log.Info("reconciler: removing stale runner registration", "repo", repo, "runner", rn.Name, "id", rn.ID)
			if err := r.GH.RemoveRunner(ctx, repo, rn.ID); err != nil {
				r.Log.Warn("reconciler: remove runner failed", "runner", rn.Name, "error", err)
			}
		}
	}
}

// runnerScopes returns the repo list to sweep for stale runner registrations.
func (r *Reconciler) runnerScopes(ctx context.Context) []string {
	switch r.Cfg.GitHub.Scope {
	case "org":
		return []string{""} // one org-level listing
	case "repo":
		return []string{r.Cfg.GitHub.RepoFullName()}
	default: // all
		// Stale registrations can only exist on repos we registered runners
		// on, and those are exactly the repos in the state file — no need to
		// sweep every repo the token can see.
		seen := map[string]bool{}
		var repos []string
		for _, rec := range r.Store.List() {
			if rec.Repo != "" && !seen[rec.Repo] {
				seen[rec.Repo] = true
				repos = append(repos, rec.Repo)
			}
		}
		return repos
	}
}
