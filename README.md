# minicontrol-runner

Ephemeral macOS GitHub Actions runners on a [mini-control](https://github.com/Lakr233/mini-control) fleet:

> job queued → grab a VM → install a JIT runner over ssh-exec → job runs → **VM deleted**.

Every job gets a factory-fresh VM; no VM outlives its job (billing runs until
deletion, so cleanup is the top design goal). Headless: one binary, one
`config.yaml`, one JSON state file.

```
                    ┌──────────────────────────────────────────────────────┐
                    │                  minicontrol-runner                  │
  ┌────────┐  poll  │ ┌──────────┐     ┌─────────────┐   ┌──────────────┐  │
  │ GitHub │◄───────│ │  poller  │────►│ dedup queue │──►│    scaler    │  │
  └────────┘        │ └──────────┘     └─────────────┘   │ 1 FSM / job  │  │
      ▲             │ ┌──────────┐     ┌─────────────┐   └──────┬───────┘  │
      │ JIT config, │ │reconciler│◄───►│ state.json  │◄─────────┤          │
      │ job status  │ └──────────┘     └─────────────┘          │          │
      └─────────────│  httpsrv: /healthz /status (no ingress)   │          │
                    └───────────────────────────────────────────┼──────────┘
                                                                ▼
                                 mini-control API ───► macOS VM (JIT runner,
                                 Bearer mck_…, worker host)     one job, exits)
```

## Per-job lifecycle

```
QUEUED → CREATING → WAIT_READY → BOOTSTRAPPING → RUNNER_UP → JOB_RUNNING → JOB_DONE
   └────────┴───────────┴─── any failure/timeout ───┴────────────┴──► DELETING → COMPLETE
```

Cleanup is quadruple-redundant: the FSM's terminal delete, a reconciler
re-issuing stuck deletes, startup resume from `state.json`, and orphan GC
(any VM under the API key with no record is deleted after a grace period —
**the mini-control API key must be dedicated to this service**).

Runners claim jobs by *label*, not name, so a runner may serve a sibling
job; the VM's lifetime follows its own runner (in-VM marker + GitHub busy
flag), and a job record only completes when GitHub says its job completed.

## Quick start

```sh
cp config.example.yaml config.yaml     # edit: base_url, scope/repos, sku, labels
export MINICONTROL_API_KEY=mck_...     # dedicated key!
export GITHUB_PAT=...                  # fine-grained: Administration RW + Actions R;
                                       # account needs admin on the target repos
go run ./cmd/minicontrol-runner --config config.yaml --dry-run
docker compose up -d --build
```

Label your workflow `runs-on: [self-hosted, macos, arm64, mini-control]`.
Discovery is pull-only — no public ingress, no webhooks; jobs are picked up
within one poll interval.

## Configuration

`config.example.yaml` documents every key. The load-bearing ones:

| Key | Meaning |
|---|---|
| `github.scope` | `all` = serve every repo the token grants; `repo` = one repo; `org` = org runners |
| `minicontrol.base_url` | the deployment's **worker** hostname (`https://…/api/client/v1`) |
| `minicontrol.sku` | fixed hardware preset, e.g. `m4-big-v1` |
| `runner.labels` | job runs-on must be a subset of these |
| `limits.max_concurrent_vms` | fleet/bill bound; excess jobs wait in GitHub's queue |
| `state.path` | volume-mounted; losing it is survivable (orphan GC) but don't |

Secrets are referenced as `${ENV_VAR}`; unknown keys fail at startup.

## In-VM bootstrap (3 cheap execs)

```
stage:   base64 script ─► ~/mcra-bootstrap.sh, JIT blob ─► ~/.mcra-jit.b64
launch:  nohup ~/mcra-bootstrap.sh &      (no exec ever waits on the job)
poll:    cat ~/.mcra-status               (PHASE=download|extract|run|done EXIT=n)
```

## Operations

| | |
|---|---|
| `--dry-run` | validate config + both APIs; creates nothing |
| `--smoke` | one VM → `uname -a` → delete (bills real money) |
| `GET /healthz`, `GET /status` | liveness; JSON of all job records |
| SIGTERM | keeps running VMs; next start resumes them |

## Development

```sh
make test    # fully offline: protocol-faithful fakes for both APIs
make race
make lint
```

## License

[MIT](LICENSE)
