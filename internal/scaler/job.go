package scaler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Lakr233/mini-control-action/internal/bootstrap"
	"github.com/Lakr233/mini-control-action/internal/githubapp"
	"github.com/Lakr233/mini-control-action/internal/minicontrol"
	"github.com/Lakr233/mini-control-action/internal/state"
)

func nowUTC() time.Time { return time.Now().UTC() }

// runJob drives one job attempt through the FSM:
//
//	QUEUED -> CREATING -> WAIT_READY -> BOOTSTRAPPING -> RUNNER_UP
//	       -> JOB_RUNNING -> JOB_DONE -> DELETING -> COMPLETE
//
// Any failure path converges on DELETING; a retryable failure re-enters the
// loop with a bumped retry counter (fresh idempotency key).
func (s *Scaler) runJob(ctx context.Context, rec state.Record, events chan githubapp.JobEvent) {
	log := s.log.With("repo", rec.Repo, "job_id", rec.JobID, "attempt", rec.RunAttempt)

	// Bound total VM concurrency. Deletion-only resumes skip the queue wait
	// by checking if there is anything left to run.
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-s.sem }()

	for {
		retryable, err := s.attempt(ctx, &rec, events, log)
		if err == nil || ctx.Err() != nil {
			return
		}
		log.Warn("job attempt failed", "retry", rec.Retry, "error", err)
		if !retryable || rec.Retry >= s.cfg.Limits.MaxRetriesPerJob {
			rec.State = state.StateFailed
			rec.LastError = err.Error()
			s.persist(&rec, log)
			log.Error("job permanently failed", "error", err)
			return
		}
		// Re-check the job is still worth a VM before burning another one.
		if done, _ := s.jobAlreadyFinished(ctx, rec); done {
			rec.State = state.StateComplete
			s.persist(&rec, log)
			return
		}
		rec.Retry++
		rec.State = state.StateQueued
		rec.VMID = ""
		rec.IdempotencyKey = ""
		rec.RunnerName = ""
		rec.RunnerID = 0
		s.persist(&rec, log)
		// Pace retries so a systemic failure doesn't burn the budget in
		// seconds; reuse the capacity backoff base as the unit.
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(rec.Retry) * s.cfg.Limits.CapacityBackoff.Initial.D()):
		}
	}
}

// attempt runs the FSM from the record's current state to a terminal state.
// It returns (retryable, err); err == nil means a terminal state was reached
// and persisted. VM deletion is guaranteed via deferred cleanup whenever a VM
// exists and the attempt is not surviving into JOB state monitoring.
func (s *Scaler) attempt(ctx context.Context, rec *state.Record, events chan githubapp.JobEvent, log logger) (bool, error) {
	// CREATING ------------------------------------------------------------
	if rec.State == state.StateQueued || rec.State == state.StateCreating {
		// Resuming an interrupted create reuses the persisted key so the
		// server replays the original response (same VM). Fresh attempts
		// mint a new key.
		if rec.IdempotencyKey == "" || rec.State == state.StateQueued {
			rec.IdempotencyKey = state.NewIdemKey(rec.JobID, rec.RunAttempt, rec.Retry)
		}
		rec.State = state.StateCreating
		// The key MUST be durable before the server sees it — creating a VM
		// whose key we might forget is how VMs leak past a crash.
		if err := s.store.Upsert(*rec); err != nil {
			return true, fmt.Errorf("persist idempotency key before create: %w", err)
		}
		vm, retryable, err := s.createVM(ctx, rec, log)
		if err != nil {
			return retryable, err
		}
		if vm == nil { // job vanished while we waited for capacity
			rec.State = state.StateComplete
			s.persist(rec, log)
			return false, nil
		}
		rec.VMID = vm.ID
		rec.State = state.StateWaitReady
		s.persist(rec, log)
		log.Info("vm created", "vm_id", vm.ID, "status", vm.Status,
			"worker", vm.WorkerName, "worker_tags", vm.WorkerTags,
			"requested_tag", s.cfg.MiniControl.WorkerTag)
	}

	// failCleanup deletes the VM after a genuine failure — but NOT on
	// shutdown: a cancelled context means the service is stopping and the
	// VM must survive to be resumed by the next start.
	failCleanup := func(err error) (bool, error) {
		if ctx.Err() != nil {
			log.Info("shutting down mid-attempt; keeping vm for resume", "vm_id", rec.VMID)
			return false, err
		}
		s.deleteVM(ctx, rec, log)
		return true, err
	}

	// WAIT_READY ----------------------------------------------------------
	if rec.State == state.StateWaitReady {
		if err := s.waitReady(ctx, rec, log); err != nil {
			return failCleanup(err)
		}
		rec.State = state.StateBootstrapping
		s.persist(rec, log)
	}

	// BOOTSTRAPPING -------------------------------------------------------
	if rec.State == state.StateBootstrapping {
		if err := s.bootstrapRunner(ctx, rec, log); err != nil {
			return failCleanup(err)
		}
		rec.State = state.StateRunnerUp
		s.persist(rec, log)
		log.Info("runner launched", "vm_id", rec.VMID, "runner", rec.RunnerName)
	}

	// RUNNER_UP / JOB_RUNNING / JOB_DONE ----------------------------------
	if rec.State == state.StateRunnerUp || rec.State == state.StateJobRunning {
		outcome, err := s.monitor(ctx, rec, events, log)
		if err != nil {
			return failCleanup(err)
		}
		switch outcome {
		case outcomeCompleted:
			rec.State = state.StateJobDone
			s.persist(rec, log)
		case outcomeNotOurs:
			// Job got picked up elsewhere or was cancelled: nothing ran here.
			log.Info("job finished without our runner; releasing vm", "vm_id", rec.VMID)
			rec.State = state.StateJobDone
			s.persist(rec, log)
		}
	}

	// DELETING ------------------------------------------------------------
	if rec.State == state.StateJobDone || rec.State == state.StateDeleting {
		rec.State = state.StateDeleting
		s.persist(rec, log)
		s.deleteVM(ctx, rec, log)
		s.removeRunnerRegistration(ctx, rec, log)
		rec.State = state.StateComplete
		s.persist(rec, log)
		log.Info("job complete, vm deleted", "vm_id", rec.VMID)
	}
	return false, nil
}

type logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
}

func (s *Scaler) persist(rec *state.Record, log logger) {
	if err := s.store.Upsert(*rec); err != nil {
		log.Error("persist state", "state", rec.State, "error", err)
	}
}

// createVM handles capacity backoff and rate limits. A nil VM with nil error
// means the job no longer needs one.
func (s *Scaler) createVM(ctx context.Context, rec *state.Record, log logger) (*minicontrol.VM, bool, error) {
	backoff := s.cfg.Limits.CapacityBackoff.Initial.D()
	deadline := time.Now().Add(s.cfg.Runner.MaxJobDuration.D()) // capacity waits can't exceed the job budget
	for {
		vm, err := s.mc.CreateVM(ctx, minicontrol.CreateVMRequest{
			SKU: s.cfg.MiniControl.SKU,
			Tag: s.cfg.MiniControl.WorkerTag,
		}, rec.IdempotencyKey)
		if err == nil {
			return vm, false, nil
		}
		var wait time.Duration
		switch {
		case minicontrol.IsCapacity(err):
			log.Warn("no capacity, backing off", "error", err, "backoff", backoff)
			wait = backoff
			backoff = min(backoff*2, s.cfg.Limits.CapacityBackoff.Max.D())
			// The server commits the 409 rejection under this idempotency key
			// and would replay it forever — every capacity retry needs a
			// fresh key, persisted before use.
			rec.IdempotencyKey = state.NewIdemKey(rec.JobID, rec.RunAttempt, rec.Retry)
			if perr := s.store.Upsert(*rec); perr != nil {
				return nil, true, fmt.Errorf("persist fresh idempotency key: %w", perr)
			}
		default:
			if d, ok := minicontrol.RetryDelay(err); ok {
				wait = d + time.Second
			} else {
				return nil, true, fmt.Errorf("create vm: %w", err)
			}
		}
		if time.Now().Add(wait).After(deadline) {
			return nil, false, fmt.Errorf("gave up waiting for capacity: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-time.After(wait):
		}
		// While backing off, drop the VM request if the job got handled.
		if done, err2 := s.jobAlreadyFinished(ctx, *rec); err2 == nil && done {
			return nil, false, nil
		}
	}
}

func (s *Scaler) jobAlreadyFinished(ctx context.Context, rec state.Record) (bool, error) {
	st, err := s.gh.GetJobStatus(ctx, githubapp.JobRef{Repo: rec.Repo, JobID: rec.JobID})
	if err != nil {
		return false, err
	}
	return st.Status == "completed", nil
}

func (s *Scaler) waitReady(ctx context.Context, rec *state.Record, log logger) error {
	deadline := time.Now().Add(s.cfg.MiniControl.ProvisionTimeout.D())
	for {
		vm, err := s.mc.GetVM(ctx, rec.VMID)
		if err != nil {
			if minicontrol.IsNotFound(err) {
				return fmt.Errorf("vm %s disappeared while provisioning", rec.VMID)
			}
			log.Warn("poll vm", "vm_id", rec.VMID, "error", err)
		} else {
			switch vm.Status {
			case minicontrol.StatusReady:
				// "ready" alone is not enough: a recovery-mode VM reports
				// ready without SSH credentials. Wait for the full contract.
				if vm.Username != "" && vm.SSH.WebsocketURL != "" {
					return nil
				}
				log.Debug("vm ready but ssh credentials not published yet", "vm_id", rec.VMID)
			case minicontrol.StatusError:
				return fmt.Errorf("vm %s entered error state: %s", rec.VMID, vm.LastError)
			case minicontrol.StatusDeleting:
				return fmt.Errorf("vm %s unexpectedly deleting", rec.VMID)
			case minicontrol.StatusStopped, minicontrol.StatusStopping:
				// Freshly created VMs can transiently report stopped before
				// the worker boots them; keep polling — provision_timeout
				// bounds how long we tolerate it.
				log.Debug("vm not booted yet", "vm_id", rec.VMID, "status", vm.Status)
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("vm %s not ready within %s", rec.VMID, s.cfg.MiniControl.ProvisionTimeout.D())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.cfg.MiniControl.PollInterval.D()):
		}
	}
}

// bootstrapRunner stages and launches the runner unless a previous attempt
// (pre-crash) already did — the in-VM status file is the truth.
func (s *Scaler) bootstrapRunner(ctx context.Context, rec *state.Record, log logger) error {
	status, err := s.execStatus(ctx, rec.VMID)
	if err == nil && status.Launched() {
		log.Info("bootstrap already launched in vm, re-attaching", "vm_id", rec.VMID, "phase", status.Phase)
		return nil
	}

	url, err := s.downloadURL(ctx)
	if err != nil {
		return fmt.Errorf("resolve runner download: %w", err)
	}
	// A resumed attempt may hold a previous registration whose name is still
	// taken (generate-jitconfig registers immediately; a name reuse 409s).
	// Best-effort remove the old registration, then always mint a fresh name.
	if rec.RunnerID != 0 {
		if err := s.gh.RemoveRunner(ctx, rec.Repo, rec.RunnerID); err != nil {
			log.Warn("could not remove stale runner registration", "runner_id", rec.RunnerID, "error", err)
		}
		rec.RunnerID = 0
	}
	vmid8 := rec.VMID
	if len(vmid8) > 8 {
		vmid8 = vmid8[:8]
	}
	rec.RunnerName = fmt.Sprintf("%s-%s-j%d-%s", s.cfg.Runner.NamePrefix, vmid8, rec.JobID, state.ShortNonce())
	jit, runnerID, err := s.gh.GenerateJITConfig(ctx, rec.Repo, rec.RunnerName, s.cfg.Runner.Labels, s.cfg.Runner.WorkDir)
	if err != nil {
		return fmt.Errorf("generate JIT config: %w", err)
	}
	rec.RunnerID = runnerID
	s.persist(rec, log)

	script, err := bootstrap.RenderScript(bootstrap.Params{
		DownloadURL:      url,
		PreinstalledPath: s.cfg.Runner.PreinstalledPath,
	})
	if err != nil {
		return err
	}
	if err := s.execOK(ctx, rec.VMID, bootstrap.StageCommand(script, jit), "staged"); err != nil {
		return fmt.Errorf("stage bootstrap: %w", err)
	}
	if err := s.execOK(ctx, rec.VMID, bootstrap.LaunchCommand(), "launched"); err != nil {
		return fmt.Errorf("launch bootstrap: %w", err)
	}
	return nil
}

// execOK runs a short command and checks its sentinel output.
func (s *Scaler) execOK(ctx context.Context, vmID, cmd, sentinel string) error {
	res, err := s.mc.Exec(ctx, vmID, minicontrol.ExecRequest{Command: cmd, TimeoutSeconds: 60})
	if err != nil {
		return err
	}
	if res.ExitCode != 0 || !strings.Contains(res.Stdout, sentinel) {
		return fmt.Errorf("exec exit=%d stdout=%q stderr=%q", res.ExitCode, truncate(res.Stdout, 200), truncate(res.Stderr, 200))
	}
	return nil
}

func (s *Scaler) execStatus(ctx context.Context, vmID string) (bootstrap.Status, error) {
	res, err := s.mc.Exec(ctx, vmID, minicontrol.ExecRequest{Command: bootstrap.StatusCommand(), TimeoutSeconds: 30})
	if err != nil {
		return bootstrap.Status{}, err
	}
	return bootstrap.ParseStatus(res.Stdout), nil
}

type monitorOutcome int

const (
	outcomeCompleted monitorOutcome = iota // job ran (here) and finished
	outcomeNotOurs                         // job finished elsewhere / cancelled
)

// monitor waits for the job to finish, fed by webhook events, GitHub job
// polls, and the in-VM status marker.
//
// IMPORTANT: ephemeral runners claim jobs by LABEL, not by name — the runner
// we created for job A may legally pick up job B, and A may be run by a
// sibling runner. Therefore the VM's lifetime is governed ONLY by its own
// runner (the in-VM marker), and when this record's job finishes elsewhere
// the VM is released only once our runner is verifiably idle.
func (s *Scaler) monitor(ctx context.Context, rec *state.Record, events chan githubapp.JobEvent, log logger) (monitorOutcome, error) {
	pickupDeadline := time.Now().Add(s.cfg.Runner.PickupTimeout.D())
	jobDeadline := time.Now().Add(s.cfg.Runner.MaxJobDuration.D())
	tick := time.NewTicker(s.cfg.Runner.StatusPollInterval.D())
	defer tick.Stop()

	// Poll GitHub at a slower cadence than the in-VM marker.
	ghEvery := 3
	tickCount := 0
	jobDone := false                    // this record's GitHub job finished (on any runner)
	var lastStatus *githubapp.JobStatus // most recent job status, for release-time logging

	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()

		case ev := <-events:
			switch ev.Action {
			case "in_progress":
				if ev.RunnerName == "" || ev.RunnerName == rec.RunnerName {
					s.markRunning(rec, log)
				}
			case "completed":
				log.Info("job completed (webhook)", "conclusion", ev.Conclusion)
				lastStatus = &githubapp.JobStatus{Status: "completed", Conclusion: ev.Conclusion, RunnerName: ev.RunnerName}
				jobDone = true
			}

		case <-tick.C:
			tickCount++
			status, err := s.execStatus(ctx, rec.VMID)
			if err != nil {
				if minicontrol.IsNotFound(err) {
					return 0, fmt.Errorf("vm %s no longer exists", rec.VMID)
				}
				log.Warn("status poll failed", "vm_id", rec.VMID, "error", err)
			} else {
				if status.Failed() {
					return 0, fmt.Errorf("bootstrap failed in vm: phase=%s log=%q", status.Phase, truncate(status.LogTail, 400))
				}
				if status.Done() {
					// Our runner exited — but by label-claiming it may have
					// run a SIBLING's job. Only this record's actual GitHub
					// job status decides the outcome.
					log.Info("runner exited (marker)", "exit", status.Exit)
					st, jerr := s.gh.GetJobStatus(ctx, githubapp.JobRef{Repo: rec.Repo, JobID: rec.JobID})
					if jerr != nil {
						// GitHub unknown — trust the exit code.
						if status.Exit == 0 {
							return outcomeCompleted, nil
						}
						return 0, fmt.Errorf("runner exited %d before job completion", status.Exit)
					}
					switch st.Status {
					case "completed":
						logJobFinished(log, st)
						return outcomeCompleted, nil
					case "in_progress":
						// A sibling runner is on it; its own VM covers it.
						return outcomeNotOurs, nil
					case "queued", "waiting", "requested", "pending":
						// Our runner served another job and exited; this job
						// still needs a VM eventually. Retry.
						return 0, fmt.Errorf("runner exited (served another job) while our job is still %s", st.Status)
					default:
						log.Warn("unexpected job status from GitHub; trusting runner exit code", "status", st.Status)
						if status.Exit == 0 {
							return outcomeCompleted, nil
						}
						return 0, fmt.Errorf("runner exited %d before job completion", status.Exit)
					}
				}
			}

			if tickCount%ghEvery == 0 {
				// Periodic machine-status heartbeat while the job is claimed.
				if vm, err := s.mc.GetVM(ctx, rec.VMID); err == nil {
					lastLine := ""
					if tail := strings.TrimSpace(status.LogTail); tail != "" {
						lines := strings.Split(tail, "\n")
						lastLine = strings.TrimSpace(lines[len(lines)-1])
					}
					log.Info("machine status",
						"vm_id", rec.VMID,
						"vm_status", vm.Status,
						"billing_active", vm.BillingActive,
						"bootstrap_phase", status.Phase,
						"job_state", rec.State,
						"runner_log", truncate(lastLine, 160))
				}

				st, err := s.gh.GetJobStatus(ctx, githubapp.JobRef{Repo: rec.Repo, JobID: rec.JobID})
				if err != nil {
					log.Warn("github job poll failed", "error", err)
				} else {
					lastStatus = &st
					switch st.Status {
					case "completed":
						jobDone = true
					case "in_progress":
						// Prefer the authoritative runner ID; fall back to
						// our invented name only when the ID is absent.
						ours := (st.RunnerID != 0 && st.RunnerID == rec.RunnerID) ||
							(st.RunnerID == 0 && st.RunnerName == rec.RunnerName)
						if ours {
							s.markRunning(rec, log)
						} else if st.RunnerName != "" || st.RunnerID != 0 {
							// A sibling runner claimed it; ours may be busy
							// with other queued work. NEVER delete here.
							log.Debug("job claimed by another runner", "runner", st.RunnerName, "runner_id", st.RunnerID)
						}
					}
				}
			}

			if jobDone {
				// Release the VM only when our runner is provably idle —
				// it may be running a different job right now.
				if busy := s.runnerBusy(ctx, rec, log); !busy {
					if lastStatus != nil {
						logJobFinished(log, *lastStatus)
					}
					log.Info("releasing idle vm", "vm_id", rec.VMID)
					return outcomeNotOurs, nil
				}
				log.Info("job finished but our runner is busy with other work; waiting for it to exit",
					"runner", rec.RunnerName)
			}

			if rec.State == state.StateRunnerUp && time.Now().After(pickupDeadline) {
				if busy := s.runnerBusy(ctx, rec, log); busy {
					// Busy with someone else's job — not idle, extend.
					pickupDeadline = time.Now().Add(s.cfg.Runner.PickupTimeout.D())
				} else if done, err := s.jobAlreadyFinished(ctx, *rec); err == nil && done {
					return outcomeNotOurs, nil
				} else {
					return 0, fmt.Errorf("runner idle: no job assigned within %s", s.cfg.Runner.PickupTimeout.D())
				}
			}
			if time.Now().After(jobDeadline) {
				return 0, fmt.Errorf("job exceeded max duration %s", s.cfg.Runner.MaxJobDuration.D())
			}
		}
	}
}

// runnerBusy reports whether this record's runner is currently executing a
// job according to GitHub. Unknown (API error) counts as busy — never delete
// a VM on missing information.
func (s *Scaler) runnerBusy(ctx context.Context, rec *state.Record, log logger) bool {
	runners, err := s.gh.ListRunners(ctx, rec.Repo)
	if err != nil {
		log.Warn("runner busy-check failed; assuming busy", "error", err)
		return true
	}
	for _, r := range runners {
		if r.ID == rec.RunnerID {
			return r.Busy
		}
	}
	return false // registration gone: ephemeral runner already exited
}

func (s *Scaler) markRunning(rec *state.Record, log logger) {
	if rec.State != state.StateJobRunning {
		rec.State = state.StateJobRunning
		s.persist(rec, log)
		log.Info("job running on our runner", "runner", rec.RunnerName)
	}
}

// deleteVM removes the VM with bounded retries; on persistent failure the
// record stays in DELETING and the reconciler keeps trying. Billing stops
// only when deletion is confirmed, so this must not silently give up.
func (s *Scaler) deleteVM(ctx context.Context, rec *state.Record, log logger) {
	if rec.VMID == "" {
		return
	}
	for i := 0; i < 3; i++ {
		// Use a fresh context so shutdown doesn't abandon a delete mid-flight.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		err := s.mc.DeleteVM(dctx, rec.VMID)
		cancel()
		if err == nil {
			log.Info("vm delete issued", "vm_id", rec.VMID)
			return
		}
		log.Warn("vm delete failed", "vm_id", rec.VMID, "try", i+1, "error", err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
		}
	}
	log.Error("vm delete still failing; reconciler will keep retrying", "vm_id", rec.VMID)
}

// removeRunnerRegistration cleans up a leftover offline runner registration.
// JIT runners usually vanish on their own; this is best-effort.
func (s *Scaler) removeRunnerRegistration(ctx context.Context, rec *state.Record, log logger) {
	if rec.RunnerID == 0 {
		return
	}
	runners, err := s.gh.ListRunners(ctx, rec.Repo)
	if err != nil {
		return
	}
	for _, r := range runners {
		if r.ID == rec.RunnerID && r.Reapable() {
			if err := s.gh.RemoveRunner(ctx, rec.Repo, r.ID); err != nil {
				log.Warn("remove runner registration", "runner_id", r.ID, "error", err)
			}
			return
		}
	}
}

func logJobFinished(log logger, st githubapp.JobStatus) {
	log.Info("job finished",
		"conclusion", st.Conclusion,
		"ran_on", st.RunnerName,
		"failed_steps", strings.Join(st.FailedSteps(), "; "))
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
