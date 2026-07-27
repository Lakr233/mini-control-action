# minicontrol-runner — agent guide

Headless Go service that turns queued GitHub Actions jobs into ephemeral
mini-control macOS VMs (one VM = one JIT runner = one job, deleted after).
README.md has the architecture and lifecycle diagrams; this file is the
non-obvious knowledge.

## Commands

```sh
make test          # go test ./...  — fully offline, no network ever
make race          # the suite must also pass -race
make lint          # vet + staticcheck + shellcheck (if installed)
docker compose up -d --build   # deploy; config.yaml + .env are local-only (gitignored)
go run ./cmd/minicontrol-runner --config config.yaml --dry-run   # safe: creates nothing
```

`--smoke` creates a real VM and bills real money — never run it casually.

## Invariants (violating these has bitten us in production)

- **Persist the idempotency key BEFORE calling CreateVM.** Keys carry a
  random suffix (`state.NewIdemKey`); deterministic keys replay stale server
  responses (including stored 409 rejections) after state loss. Capacity
  retries must mint a fresh key each time.
- **A VM's lifetime follows its OWN runner, never its job's runner_name.**
  Ephemeral runners claim jobs by label — the runner created for job A may
  legally run job B. Release a VM only when its runner exited (in-VM marker)
  or is provably idle (GitHub busy flag); a record completes only when
  GitHub says *its* job completed.
- **Graceful shutdown must not delete VMs** (`failCleanup` checks
  `ctx.Err()`); startup `Resume()` re-adopts them.
- **Never let a truncated listing look like absence** — `ListRunners` errors
  on page-cap overflow because "runner absent" is read as "runner exited"
  and tears down VMs.
- **The mini-control API key is dedicated to this service.** The reconciler
  deletes any VM under the key that has no state record (orphan GC).
- Job statuses are six-valued (`queued|in_progress|completed|waiting|
  requested|pending`); runner `status` has NO documented enum — reap on
  `!= "online" && !busy`, never on `== "offline"`.
- **A rejected create never degrades into a weaker one.** mini-control
  rejects unknown create fields with a generic `400 "invalid json"`. When the
  API-key surface still lacked `tag`, retrying without it "worked" — and
  silently placed VMs on arbitrary workers while config promised a placement.
  Failure is failure; the error propagates to the job.
- **Worker tags are the only placement control, and they are exact.** The
  API-key create takes `{"sku", "tag"}`; the tag is matched
  case-insensitively and exactly (no zones, no regions — mini-control has no
  such concept). An unmatched tag is a `409 no_workers_matching_tag`, so it
  lands in the capacity path and retries within the job budget rather than
  failing instantly — a typo'd tag looks like a capacity stall, so read the
  reason code in the warning. Untagged creates skip workers whose tags end in
  a server-configured suffix (`client_api.exclude_tag_suffixes`, live default
  `["test"]`); an explicit tag bypasses that fence, which is why a
  `*-test` worker is only reachable with the tag set.
- **Always log `worker_name`/`worker_tags` from the create response.** They
  are the only fleet visibility an API key gets and the only proof a
  requested placement was honoured; a whole debugging session was spent
  answering "which worker did this land on".
- Fresh VMs transiently report `stopped` before booting: not an error.
  `ready` without `username`+`ssh.websocket_url` is not ready either
  (recovery-mode shape).

## Testing conventions

- All tests run against `internal/minicontrol/fake` and
  `internal/githubapp/ghfake`. Keep the fakes protocol-faithful (they store
  and replay idempotent 409s, paginate runner listings, 409 duplicate runner
  names, move run `updated_at` when job statuses change) — a fake that is
  more forgiving than the real server hides real bugs, which is how several
  production incidents got past the original suite.
- Behavior fixes get behavior tests in `internal/scaler/scaler_test.go`
  driving the full FSM through the fakes; pure contract details go in the
  client tests.

## API quirks worth knowing before touching clients

- GitHub: JIT registration is `generate-jitconfig` (runner_group_id required
  at BOTH repo and org scope; `work_folder` is relative to the runner install
  dir). The release download URL must come from the release's `assets[]`,
  fetched from `releasesBase` (always api.github.com, never a GHE base).
  `/user/repos` reflects the USER's access, not a token allowlist.
- mini-control: only the worker hostname serves API keys; `Idempotency-Key`
  is required (1–128 bytes); billing runs until VM deletion is
  worker-confirmed; exec is `/bin/sh -lc`, no stdin, ~1 MiB output caps —
  hence the 3-exec bootstrap protocol (stage base64 → nohup → poll marker).

## Layout

- `internal/scaler/job.go` — per-job FSM; the heart. Read it before any
  lifecycle change.
- `internal/reconciler` — the safety net; every delete path must stay
  redundant with it.
- `internal/githubapp`, `internal/minicontrol` — stdlib-only API clients
  (no go-github; keep the dependency tree at yaml.v3).
- `internal/bootstrap` — owns platform naming and the in-VM script template.
