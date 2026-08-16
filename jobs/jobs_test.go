package jobs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
)

func TestComponentContract(t *testing.T) {
	j := New()
	if j.Name() != ComponentName {
		t.Fatalf("Name() = %q, want %q", j.Name(), ComponentName)
	}
	if j.GetInitOrderStage() != ComponentStage {
		t.Fatalf("GetInitOrderStage() = %q, want %q", j.GetInitOrderStage(), ComponentStage)
	}
	var _ cf.CaerusComponent = j
	var _ cf.Runnable = j

	if c := j.Client(); c != nil {
		t.Fatal("Client() should be nil before Init")
	}
	if err := j.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Init: %v", err)
	}
}

func TestHealthAndMetricsBeforeInit(t *testing.T) {
	j := New()
	if err := j.Health(context.Background()); err == nil {
		t.Fatal("Health before Init should fail")
	}
	if ms := j.Metrics(); ms != nil {
		t.Fatalf("Metrics before Init = %+v, want nil", ms)
	}
	var _ cf.HealthProvider = j
	var _ cf_observability.MetricsProvider = j
}

func TestNewDefaults(t *testing.T) {
	j := New()
	if j.pollInterval != defaultPollInterval {
		t.Fatalf("pollInterval = %v, want %v", j.pollInterval, defaultPollInterval)
	}
	if j.batchSize != defaultBatchSize {
		t.Fatalf("batchSize = %d, want %d", j.batchSize, defaultBatchSize)
	}
	if j.concurrency != defaultConcurrency {
		t.Fatalf("concurrency = %d, want %d", j.concurrency, defaultConcurrency)
	}
	if j.retryFixedD != defaultRetryFixedDelay || j.retryFixedP != defaultRetryFixedPhase || j.retryMaxD != defaultRetryMaxDelay {
		t.Fatalf("retry policy = %v/%v/%v, want 5s/30s/5m", j.retryFixedD, j.retryFixedP, j.retryMaxD)
	}
	if j.retryJitter != defaultRetryJitter {
		t.Fatalf("retryJitter = %v, want %v", j.retryJitter, defaultRetryJitter)
	}
	if cap(j.semCh) != defaultConcurrency {
		t.Fatalf("semCh cap = %d, want %d", cap(j.semCh), defaultConcurrency)
	}
	if j.shutdownDrain != 10*time.Second {
		t.Fatalf("shutdownDrain = %v, want 10s", j.shutdownDrain)
	}
}

func TestNewWithName(t *testing.T) {
	j := New(WithName("audit"))
	if j.Name() != "audit" {
		t.Fatalf("Name() = %q, want audit", j.Name())
	}
}

func TestWithConfigOverridesOptions(t *testing.T) {
	j := New(
		WithPollInterval(2*time.Second),
		WithBatchSize(99),
		WithConcurrency(32),
		WithConfig(JobsConfig{PollIntervalMs: 250, BatchSize: 4, Concurrency: 2, RetryJitter: floatPtr(0)}),
	)
	if j.pollInterval != 250*time.Millisecond {
		t.Fatalf("pollInterval = %v, want 250ms", j.pollInterval)
	}
	if j.batchSize != 4 {
		t.Fatalf("batchSize = %d, want 4", j.batchSize)
	}
	if j.concurrency != 2 || cap(j.semCh) != 2 {
		t.Fatalf("concurrency = %d semCap=%d, want 2/2", j.concurrency, cap(j.semCh))
	}
	if j.retryJitter != 0 {
		t.Fatalf("retryJitter = %v, want 0", j.retryJitter)
	}
}

func TestGetDependencies(t *testing.T) {
	j := New()
	deps := j.GetDependencies()
	if len(deps) != 2 || deps[0] != cf_valkey.ComponentName || deps[1] != "logs" {
		t.Fatalf("GetDependencies() = %v, want [valkey logs]", deps)
	}
	var _ cf.Dependencies = j

	named := New(WithValkeyName("cache"))
	deps = named.GetDependencies()
	if len(deps) != 2 || deps[0] != "cache" {
		t.Fatalf("GetDependencies() with named peer = %v, want [cache logs]", deps)
	}

	withSrc := New(WithConfigSource("jobs", "config/jobs.json"))
	deps = withSrc.GetDependencies()
	if len(deps) != 3 || deps[2] != "configuration" {
		t.Fatalf("GetDependencies() with source = %v, want [valkey logs configuration]", deps)
	}
}

func TestInitRequiresValkey(t *testing.T) {
	j := New()
	err := j.Init(context.Background(), cf.New())
	if err == nil {
		t.Fatal("Init without a valkey component should fail")
	}
	if !strings.Contains(err.Error(), `valkey component "valkey" is not registered`) {
		t.Fatalf("Init error = %v, want a valkey-not-registered error", err)
	}
}

func TestInitWithNamedValkeyMissing(t *testing.T) {
	j := New(WithValkeyName("cache"))
	err := j.Init(context.Background(), cf.New())
	if err == nil {
		t.Fatal("Init with a missing named valkey should fail")
	}
	if !strings.Contains(err.Error(), `valkey component "cache" is not registered`) {
		t.Fatalf("Init error = %v, want a cache-not-registered error", err)
	}
}

func TestInitAllowsNilClient(t *testing.T) {
	fw := cf.New()
	if err := fw.AddComponent(cf_valkey.New()); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
	j := New()
	if err := j.Init(context.Background(), fw); err != nil {
		t.Fatalf("Init with nil Client() should soft-succeed: %v", err)
	}
	if err := j.Health(context.Background()); err == nil {
		t.Fatal("Health with nil Client() should fail")
	}
}

func TestInitResolvesValkeyByName(t *testing.T) {
	fw := cf.New()
	if err := fw.AddComponent(cf_valkey.New(cf_valkey.WithName("cache"))); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
	j := New(WithValkeyName("cache"))
	if err := j.Init(context.Background(), fw); err != nil {
		t.Fatalf("Init with named peer and nil Client() should soft-succeed: %v", err)
	}
}

func TestWorkerEnabled(t *testing.T) {
	if New().workerEnabled() {
		t.Fatal("worker should be disabled without handlers")
	}
	noop := func(ctx context.Context, job Job) error { return nil }
	if !New(WithJobHandler("t", noop)).workerEnabled() {
		t.Fatal("worker should be enabled with a handler")
	}
	if New(WithJobHandler("t", noop), WithWorkerEnabled(false)).workerEnabled() {
		t.Fatal("worker should be disabled by WithWorkerEnabled(false)")
	}
}

func TestEnqueueErrorsBeforeInit(t *testing.T) {
	j := New()
	if _, err := j.Enqueue(context.Background(), "t", nil); err == nil {
		t.Fatal("Enqueue before Init should fail")
	}
}

func TestRetryPolicyFixedPhase(t *testing.T) {
	j := New(WithConfig(JobsConfig{RetryJitter: floatPtr(0)}))
	now := time.Now()

	// First failure: no retry epoch recorded yet -> fixed delay, epoch set now.
	delay, newStart, newBase := j.retryDelay(1, 0, 0, now)
	if delay != 5*time.Second {
		t.Fatalf("first failure delay = %v, want 5s", delay)
	}
	if newStart == 0 || newStart != now.UnixMilli() {
		t.Fatalf("newStart = %d, want %d", newStart, now.UnixMilli())
	}
	if newBase != 0 {
		t.Fatalf("newBase = %d, want 0", newBase)
	}

	// Still inside the fixed phase (5s elapsed of a 30s phase).
	delay, newStart, newBase = j.retryDelay(3, now.Add(-5*time.Second).UnixMilli(), 0, now)
	if delay != 5*time.Second || newBase != 0 {
		t.Fatalf("in-phase delay = %v base=%d, want 5s/0", delay, newBase)
	}
}

func TestRetryPolicyJitterPhase(t *testing.T) {
	j := New(WithConfig(JobsConfig{RetryJitter: floatPtr(0)}))
	now := time.Now()
	start := now.Add(-40 * time.Second).UnixMilli() // past the 30s phase

	// First jitter-phase failure: base set to current attempts, delay = 5s.
	delay, _, newBase := j.retryDelay(3, start, 0, now)
	if newBase != 3 {
		t.Fatalf("newBase = %d, want 3", newBase)
	}
	if delay != 5*time.Second {
		t.Fatalf("jitter-phase first delay = %v, want 5s", delay)
	}

	// Exponential counting from the jitter start.
	for attempt, want := range map[int64]time.Duration{
		4: 10 * time.Second,
		5: 20 * time.Second,
		6: 40 * time.Second,
	} {
		delay, _, base := j.retryDelay(attempt, start, 3, now)
		if base != 3 {
			t.Fatalf("attempt %d base = %d, want 3", attempt, base)
		}
		if delay != want {
			t.Fatalf("attempt %d delay = %v, want %v", attempt, delay, want)
		}
	}

	// Cap at the max delay.
	delay, _, _ = j.retryDelay(100, start, 3, now)
	if delay != 5*time.Minute {
		t.Fatalf("capped delay = %v, want 5m", delay)
	}
}

func TestRetryPolicyJitterRange(t *testing.T) {
	j := New() // default jitter 0.5
	now := time.Now()
	start := now.Add(-40 * time.Second).UnixMilli()
	for i := 0; i < 50; i++ {
		delay, _, _ := j.retryDelay(4, start, 3, now)
		if delay < 5*time.Second || delay > 15*time.Second {
			t.Fatalf("jittered delay = %v, want within [5s, 15s] (10s base, 0.5 jitter)", delay)
		}
	}
}

func TestValidateJobID(t *testing.T) {
	ok, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{ok, "stable-id-1"} {
		if err := validateJobID(id); err != nil {
			t.Fatalf("validateJobID(%q) = %v", id, err)
		}
	}
	for _, id := range []string{"", "ready", "inflight", "dead", "cron", "a:b", "jobs:ready"} {
		if err := validateJobID(id); !errors.Is(err, ErrInvalidJobID) {
			t.Fatalf("validateJobID(%q) = %v, want ErrInvalidJobID", id, err)
		}
	}
}

func TestEnqueueRejectsReservedID(t *testing.T) {
	j := New()
	_, err := j.Enqueue(context.Background(), "t", nil, WithID("ready"))
	if !errors.Is(err, ErrInvalidJobID) {
		t.Fatalf("Enqueue WithID ready = %v, want ErrInvalidJobID", err)
	}
}

func TestNewID(t *testing.T) {
	a, err := newID()
	if err != nil {
		t.Fatalf("newID: %v", err)
	}
	b, _ := newID()
	if a == b {
		t.Fatal("ids should be unique")
	}
	if len(a) != 32 {
		t.Fatalf("id %q length = %d, want 32", a, len(a))
	}
}

func TestParseClaim(t *testing.T) {
	job, retryStart, jitterBase, err := parseClaim([]string{
		"id1", "2", "email.send", "hello", "5", "1700000000000", "1700000100000", "1",
	})
	if err != nil {
		t.Fatalf("parseClaim: %v", err)
	}
	if job.ID != "id1" || job.Type != "email.send" || string(job.Payload) != "hello" {
		t.Fatalf("job = %+v", job)
	}
	if job.Attempts != 2 || job.MaxAttempts != 5 {
		t.Fatalf("attempts = %d/%d, want 2/5", job.Attempts, job.MaxAttempts)
	}
	if retryStart != 1700000100000 || jitterBase != 1 {
		t.Fatalf("retryStart=%d jitterBase=%d, want 1700000100000/1", retryStart, jitterBase)
	}

	if _, _, _, err := parseClaim([]string{"a", "b"}); err == nil {
		t.Fatal("short claim should error")
	}
	if _, _, _, err := parseClaim([]string{"a", "x", "y", "z", "1", "1", "0", "0"}); err == nil {
		t.Fatal("non-numeric attempts should error")
	}
}

func floatPtr(f float64) *float64 { return &f }

func boolPtr(b bool) *bool { return &b }

func TestApplyConfigWorkerEnabledPointer(t *testing.T) {
	noop := func(ctx context.Context, job Job) error { return nil }
	j := New(WithJobHandler("t", noop), WithWorkerEnabled(true))
	if !j.workerEnabled() {
		t.Fatal("want worker on")
	}
	j.applyConfig(JobsConfig{}) // omitted: keep construct default
	if !j.workerEnabled() {
		t.Fatal("omitted WorkerEnabled must not disable the worker")
	}
	j.applyConfig(JobsConfig{WorkerEnabled: boolPtr(false)})
	if j.workerEnabled() {
		t.Fatal("explicit false must disable the worker")
	}
	j.applyConfig(JobsConfig{WorkerEnabled: boolPtr(true)})
	if !j.workerEnabled() {
		t.Fatal("explicit true must enable the worker")
	}
}

func TestApplyConfigRetryJitterOmitKeepsDefault(t *testing.T) {
	j := New()
	j.applyConfig(JobsConfig{PollIntervalMs: 100})
	if j.retryJitter != defaultRetryJitter {
		t.Fatalf("omitted jitter = %v, want default %v", j.retryJitter, defaultRetryJitter)
	}
	j.applyConfig(JobsConfig{RetryJitter: floatPtr(0)})
	if j.retryJitter != 0 {
		t.Fatalf("explicit 0 jitter = %v, want 0", j.retryJitter)
	}
}

func TestDeadLetterOpsBeforeInit(t *testing.T) {
	j := New()
	ctx := context.Background()
	if _, err := j.ListDead(ctx, 0, 10); err == nil {
		t.Fatal("ListDead before Init should fail")
	}
	if err := j.Replay(ctx, "x"); err == nil {
		t.Fatal("Replay before Init should fail")
	}
	if err := j.PurgeDead(ctx, "x"); err == nil {
		t.Fatal("PurgeDead before Init should fail")
	}
	if _, err := j.PurgeDeadAll(ctx); err == nil {
		t.Fatal("PurgeDeadAll before Init should fail")
	}
	if err := j.Replay(ctx, ""); err == nil {
		t.Fatal("Replay empty id should fail")
	}
}

func TestWithRepeatClampsBelowOneSecond(t *testing.T) {
	j := New(WithRepeat("t", 50*time.Millisecond, nil))
	if len(j.repeats) != 1 || j.repeats[0].every != time.Second {
		t.Fatalf("repeats = %+v, want every=1s", j.repeats)
	}
}
