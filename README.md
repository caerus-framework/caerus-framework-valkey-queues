# caerus-framework-valkey-queues

[![CI](https://github.com/caerus-framework/caerus-framework-valkey-queues/actions/workflows/ci.yml/badge.svg)](https://github.com/caerus-framework/caerus-framework-valkey-queues/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/caerus-framework/caerus-framework-valkey-queues/graph/badge.svg)](https://codecov.io/gh/caerus-framework/caerus-framework-valkey-queues)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Caerus Framework **Valkey queue machines**. One module, several claim/ack
components that share a valkey peer (`Client()` per use, `Key()` for names,
logs, soft-init). The fridge is [`caerus-framework-valkey`](https://github.com/caerus-framework/caerus-framework-valkey).

This is **not** valkey-state (sessions / cache / counters). Queues and
state are siblings that both use the valkey peer; neither owns the other.
It is **not** a River/asynq wrap (`caerus-framework-jobs` when a product
needs that).

| Package | Machine |
|---|---|
| [`vpq`](vpq/) | Weighted priority queue (hottest id wins) |
| [`jobs`](jobs/) | Delayed / retry / dead-letter jobs (`ComponentName` `"valkey-jobs"`). Old repo `caerus-framework-valkey-jobs` is not tagged anymore |
| this module (`queues`) | Optional **parent** `CFValkeyQueues`: groups machines you pass in. Not a shared `Queue` type. Omitting a machine means it does not start |

The **app** still constructs the queue it needs in `New` and returns it from
`Subcomponents()` (demoapp: VPQ only). Use the parent when a binary wants
one `AddComponent` for several machines. There is no default that starts
every machine.

---

## `vpq` — weighted priority queue

Atomic Lua: add / claim / ack / requeue / recover. The app owns what the
payload means. The component owns fairness, claim, deadlock recover, depth
Health, metrics.

Keys go through the valkey peer’s `Key()` (`squeue`, `zqueue`, `pqdeadlocks`,
…). Put the instance prefix on **valkey** (`WithKeyPrefix`), not on VPQ.

`Handler` is `func(context.Context, *BGetObject) error`. Honour `ctx` for
shutdown. A failed handler requeues (weight +1).

Not a general job queue (no DLQ/cron/dashboard). For retries and scheduling
use the **`jobs`** package in this module, or River/asynq — not VPQ.

## `jobs` — run-at / retry / dead letter

Same fridge and chassis as VPQ. This is still **valkey-jobs**: delayed
work, retries, dead letter. The **module** is `valkey-queues`; the **package
and registry name** stay jobs (`import …/valkey-queues/jobs`, `Name()`
`"valkey-jobs"`). Different Lua: ready / inflight / dead ZSETs.
`Handler` is `func(context.Context, Job) error`. Keys go through valkey
`Key("jobs", …)`. Construct in the app’s `New` and return from
`Subcomponents()` when the product enqueues; do not start it because VPQ
exists.

```go
import cf_jobs "github.com/caerus-framework/caerus-framework-valkey-queues/jobs"

q := cf_jobs.New(
	cf_jobs.WithConfigSource("jobs", "config/jobs.json"),
	cf_jobs.WithJobHandler("email.send", sendEmail),
)
```

`WithConfigSource` Init used to deadlock (mutex locked twice). That is fixed;
the regression is `TestWithConfigSourceInitializeDoesNotDeadlock`.

`worker_enabled` and `retry_jitter` are pointers in the file: omit keeps the
construct default; explicit `false` / `0` is how you turn the worker off or
disable jitter. The poll loop reads `worker_enabled` every tick.

Default **visibility** is 1 minute. Set `WithVisibility` (per enqueue) well
above the handler’s runtime or a slow job is reaped as hung and retried.
Do not Info-log `job.Payload` if it can hold PII.

Dead letters sit in a ZSET until retention expires. Operators call
`ListDead`, `Replay` (same id, attempts reset, due now), `PurgeDead`, or
`PurgeDeadAll`. There is no HTTP admin; an app job or CLI is enough.
Ack/release use the valkey peer’s live `Client()` (not the claim-time
snapshot).

Depth gauges: `valkey_jobs_ready`, `valkey_jobs_inflight`, `valkey_jobs_dead`.

Delivery stays **at-least-once**. `WithID` makes enqueue unique while the job
hash exists (`ErrAlreadyEnqueued`); a visibility timeout can still run the
handler twice. `WithRepeat(type, every, payload)` is interval cron, not a
calendar: one fire per interval across replicas (valkey NX lock). Missed
ticks are not replayed. There is no jobs **dashboard** (no HTML UI); use
`/metrics`, `ListDead`, and logs.

## Wiring

Two wiring shapes. Prefer the **app-owned** shape.

### Golden path (app-owned consumer, demoapp pattern)

`main` declares **valkey** (and postgres/http as needed) plus the app class.
The app constructs the interest (or orders) queue in `New` and exposes it via
`Subcomponents()` so the framework registers it. The app does **not** list
`"vpq"` as a chassis peer it `Get`s unless some other component consumes the
same queue instance.

```go
fw := cf.New(&cf.FrameworkOptions{
	Logs:          &cf.LogsSettings{Format: "json", Level: "info", ConfigSource: "logs"},
	Observability: &cf.ObservabilitySettings{Address: ":9090", ConfigSource: "observability"},
	Components: []cf.CaerusComponent{
		cf_valkey.New(
			cf_valkey.WithConfigSource("valkey", "config/valkey.json"),
			cf_valkey.WithKeyPrefix("demo:"),
		),
		app.New(),
	},
})
```

```go
func New() *App {
	a := &App{}
	a.interest = vpq.New(
		vpq.WithName("interest"),
		vpq.WithQueueName("interest"),
		vpq.WithHandler(a.InterestHandler),
	)
	return a
}

func (a *App) Subcomponents() []cf.CaerusComponent {
	return []cf.CaerusComponent{a.interest}
}
```

A process with more than one valkey uses `vpq.WithValkeyName("valkey-cache")`.
`GetDependencies` reports that **component** `Name()`, not a config source
nickname.

### Simple path

Bare `fw.AddComponent(valkey)` + `fw.AddComponent(queue)` for a one-off
binary:

```go
fw := cf.New()
fw.AddComponent(cf_logs.New(cf_logs.WithWriter(os.Stdout)))
fw.AddComponent(cf_valkey.New(cf_valkey.WithAddress("127.0.0.1:6379")))
queue := vpq.New(
	vpq.WithQueueName("orders"),
	vpq.WithHandler(func(ctx context.Context, item *vpq.BGetObject) error {
		return processOrder(ctx, item.ObjectID, item.ObjectValue)
	}),
)
fw.AddComponent(queue) // GetDependencies: valkey, logs
```

The queue is a `cf.Runnable`: with a handler, `Run` consumes until cancel.
Default recover of abandoned in-flight items is 30s (`WithRecoverInterval(0)`
to disable).

### Optional parent (`CFValkeyQueues`)

This is **not** a single `Queue` API. VPQ and jobs stay separate types.
The parent is a bag: pass only the machines this process should run.

```go
import (
	cf_jobs "github.com/caerus-framework/caerus-framework-valkey-queues/jobs"
	cf_queues "github.com/caerus-framework/caerus-framework-valkey-queues"
	"github.com/caerus-framework/caerus-framework-valkey-queues/vpq"
)

bag := cf_queues.New(
	cf_queues.WithVPQ(vpq.New(vpq.WithQueueName("orders"), vpq.WithHandler(handleOrder))),
	cf_queues.WithJobs(cf_jobs.New(cf_jobs.WithJobHandler("email.send", sendEmail))),
)
fw.AddComponent(bag) // registers bag, then each child
```

`WithJobs` omitted → jobs is not registered. Same for `WithVPQ`. Children
keep `Name()` `"vpq"` / `"valkey-jobs"` (or their `WithName`). The parent
does not Init, Run, or Shutdown them.

## Usage

```go
queue := cf.MustGet[*vpq.PriorityQueue](fw)
added, err := queue.Add(ctx, "order-1", `{"amount": 42}`)
// added false → id already queued; weight +1; payload kept
item, err := queue.BlockingBGet(ctx)
if item != nil {
	_ = queue.Ack(ctx, item.ObjectID)
}
```

## Options

| Option | Description |
|---|---|
| `WithConfig(PQConfig)` | static snapshot; non-zero fields override option defaults |
| `WithConfigSource(name, path, …)` | bind a configuration source (`ConfigSourceRegistrar`) |
| `WithQueueName(name)` | required; key segment and identity (frozen after Init) |
| `WithValkeyName(name)` | valkey component `Name()` (default `"valkey"`) |
| `WithBlockDuration(d)` | blocking pop wait (default `1s`) |
| `WithPublishWatermarkDelay(d)` | min interval between pub/sub on Add (default `0` = off) |
| `WithCacheTimeout(d)` | max queue residence (default `0` = unlimited) |
| `WithPollInterval(d)` | consumer poll (default `1s`) |
| `WithHandler(Handler)` | auto-consumer; default 30s recover + Health thresholds |
| `WithWorkers(n)` | concurrent consumers (default `1`). Reload of `workers` logs restart-required; the running pool size does not change |
| `WithRecoverInterval(d)` / `WithRecoverMaxAge(d)` | deadlock recover tick / min age |
| `WithMaxDepth(n)` / `WithMaxInFlight(n)` | Health ceilings |
| `WithName(name)` | component `Name()` for multiple queues (default `"vpq"`) |
| `WithLogger(*slog.Logger)` | explicit logger; else framework `logs` via `OnReconfigureFor` |

Reload updates tunables only. Queue name is frozen after Init. Valkey
reconnect is the valkey owner’s job.

## Health / metrics

`Health` pings valkey and checks depth / in-flight. A nil `Client()`
(before Init, after Shutdown, or degraded peer) is **not ready**. Init may
succeed when the peer is degraded (`Client()` nil); `/readyz` stays red until
the fridge answers.

Metrics (`vpq_info`, `vpq_depth`, `vpq_in_flight`, `vpq_recoveries_total`)
use copied label maps (`queue`, `component`).

## Tests

Unit tests need no Valkey. Integration tests skip unless `VALKEY_ADDR` is set.

```bash
docker run -d --rm -p 6379:6379 --name v valkey/valkey:8
VALKEY_ADDR=127.0.0.1:6379 go test -race ./...
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
