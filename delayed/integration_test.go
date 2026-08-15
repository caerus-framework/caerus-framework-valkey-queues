package delayed

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
	"github.com/valkey-io/valkey-go"
)

func addComponent(t *testing.T, fw *cf.CaerusFramework, c cf.CaerusComponent) {
	t.Helper()
	if err := fw.AddComponent(c); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
}

// setupJobs boots a framework with a real valkey (VALKEY_ADDR), the jobs
// component, and a running worker. It flushes the server, and returns the jobs
// component, a raw client for assertions, and a cancel that stops the worker
// (which drains claimed jobs before Run returns).
func setupJobs(t *testing.T, opts ...Option) (*CFValkeyJobs, valkey.Client, context.CancelFunc) {
	t.Helper()
	return setupJobsNamed(t, "valkey", opts...)
}

func setupJobsNamed(t *testing.T, valkeyName string, opts ...Option) (*CFValkeyJobs, valkey.Client, context.CancelFunc) {
	t.Helper()
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}
	fw := cf.New()
	addComponent(t, fw, cf_logs.New(cf_logs.WithWriter(io.Discard)))
	vk := cf_valkey.New(
		cf_valkey.WithName(valkeyName),
		cf_valkey.WithAddress(addr),
		cf_valkey.WithKeyPrefix("valkey-jobs-test"),
	)
	addComponent(t, fw, vk)
	jobs := New(opts...)
	addComponent(t, fw, jobs)
	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	raw := vk.Client()
	if err := raw.Do(context.Background(), raw.B().Flushdb().Build()).Error(); err != nil {
		t.Fatalf("Flushdb: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = jobs.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		_ = fw.Shutdown(context.Background())
	})
	return jobs, raw, cancel
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

func inZset(raw valkey.Client, key, member string) bool {
	resp := raw.Do(context.Background(), raw.B().Zscore().Key(key).Member(member).Build())
	return resp.Error() == nil
}

func zsetEmpty(raw valkey.Client, key string) bool {
	resp := raw.Do(context.Background(), raw.B().Zcount().Key(key).Min("-inf").Max("+inf").Build())
	if resp.Error() != nil {
		return false
	}
	n, err := resp.AsInt64()
	if err != nil {
		return false
	}
	return n == 0
}

func metricValue(ms []cf_observability.Metric, name string) float64 {
	for _, m := range ms {
		if m.Name == name && m.Value > 0 {
			return m.Value
		}
	}
	return 0
}

func TestIntegrationEnqueueAndRun(t *testing.T) {
	var mu sync.Mutex
	got := make(map[string]string)
	jobs, raw, _ := setupJobs(t,
		WithPollInterval(30*time.Millisecond),
		WithJobHandler("echo", func(ctx context.Context, job Job) error {
			mu.Lock()
			got[job.ID] = string(job.Payload)
			mu.Unlock()
			return nil
		}),
	)

	for i := 0; i < 3; i++ {
		if _, err := jobs.Enqueue(context.Background(), "echo", []byte("payload-"+string(rune('a'+i)))); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 3
	})
	for _, payload := range got {
		if !strings.HasPrefix(payload, "payload-") {
			t.Fatalf("unexpected payload %q", payload)
		}
	}
	waitFor(t, 3*time.Second, func() bool {
		return zsetEmpty(raw, jobs.readyKey()) && zsetEmpty(raw, jobs.inflightKey()) && zsetEmpty(raw, jobs.deadKey())
	})
	if ms := jobs.Metrics(); metricValue(ms, "valkey_jobs_enqueued_total") != 3 || metricValue(ms, "valkey_jobs_run_total") != 3 {
		t.Fatalf("metrics mismatch: %+v", ms)
	}
	if err := jobs.Health(context.Background()); err != nil {
		t.Fatalf("Health after run: %v", err)
	}
}

func TestIntegrationDelayedSchedule(t *testing.T) {
	var runs atomic.Int64
	jobs, _, _ := setupJobs(t,
		WithPollInterval(30*time.Millisecond),
		WithJobHandler("late", func(ctx context.Context, job Job) error {
			runs.Add(1)
			return nil
		}),
	)
	if _, err := jobs.Enqueue(context.Background(), "late", nil, WithDelay(400*time.Millisecond)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := jobs.Enqueue(context.Background(), "late", nil, WithRunAt(time.Now().Add(-time.Minute))); err != nil {
		t.Fatalf("Enqueue past: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if runs.Load() != 1 {
		t.Fatalf("runs after 150ms = %d, want 1 (only the past-due job)", runs.Load())
	}
	waitFor(t, 3*time.Second, func() bool { return runs.Load() == 2 })
}

func TestIntegrationFailureRetriesThenDead(t *testing.T) {
	var runs atomic.Int64
	jobs, raw, _ := setupJobs(t,
		WithPollInterval(30*time.Millisecond),
		WithRetryPolicy(200*time.Millisecond, 1*time.Second, 2*time.Second),
		WithJobHandler("flaky", func(ctx context.Context, job Job) error {
			runs.Add(1)
			return errors.New("boom")
		}),
	)
	id, err := jobs.Enqueue(context.Background(), "flaky", nil, WithMaxAttempts(3))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return inZset(raw, jobs.deadKey(), id) })
	if runs.Load() != 3 {
		t.Fatalf("runs = %d, want 3", runs.Load())
	}
	if inZset(raw, jobs.readyKey(), id) || inZset(raw, jobs.inflightKey(), id) {
		t.Fatalf("job should not be ready/inflight after dead-lettering")
	}
}

func TestIntegrationNoHandlerDeadLetters(t *testing.T) {
	jobs, raw, _ := setupJobs(t,
		WithPollInterval(30*time.Millisecond),
		WithJobHandler("known", func(ctx context.Context, job Job) error { return nil }),
	)
	id, err := jobs.Enqueue(context.Background(), "ghost", nil)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return inZset(raw, jobs.deadKey(), id) })
	if inZset(raw, jobs.readyKey(), id) || inZset(raw, jobs.inflightKey(), id) {
		t.Fatalf("ghost job should not be ready/inflight")
	}
}

func TestIntegrationVisibilityRecovery(t *testing.T) {
	var runs atomic.Int64
	jobs, raw, _ := setupJobs(t,
		WithPollInterval(30*time.Millisecond),
		WithJobHandler("slow", func(ctx context.Context, job Job) error {
			runs.Add(1)
			time.Sleep(300 * time.Millisecond)
			return nil
		}),
	)
	if _, err := jobs.Enqueue(context.Background(), "slow", nil, WithVisibility(120*time.Millisecond)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, 4*time.Second, func() bool { return runs.Load() >= 2 })
	waitFor(t, 4*time.Second, func() bool {
		return zsetEmpty(raw, jobs.readyKey()) && zsetEmpty(raw, jobs.inflightKey()) && zsetEmpty(raw, jobs.deadKey())
	})
}

func TestIntegrationConcurrencyBound(t *testing.T) {
	var mu sync.Mutex
	var active, maxActive int
	var completed atomic.Int64
	jobs, raw, _ := setupJobs(t,
		WithPollInterval(30*time.Millisecond),
		WithConcurrency(4),
		WithJobHandler("work", func(ctx context.Context, job Job) error {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			time.Sleep(30 * time.Millisecond)
			mu.Lock()
			active--
			mu.Unlock()
			completed.Add(1)
			return nil
		}),
	)
	for i := 0; i < 16; i++ {
		if _, err := jobs.Enqueue(context.Background(), "work", nil); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	waitFor(t, 5*time.Second, func() bool { return completed.Load() == 16 })
	if maxActive < 2 {
		t.Fatalf("expected overlapping runs, maxActive = %d", maxActive)
	}
	if maxActive > 4 {
		t.Fatalf("concurrency exceeded bound: maxActive = %d", maxActive)
	}
	if !zsetEmpty(raw, jobs.readyKey()) || !zsetEmpty(raw, jobs.inflightKey()) || !zsetEmpty(raw, jobs.deadKey()) {
		t.Fatalf("work zsets not drained")
	}
}

func TestIntegrationShutdownDrainRequeues(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	jobs, raw, cancel := setupJobs(t,
		WithPollInterval(30*time.Millisecond),
		WithShutdownDrainTimeout(300*time.Millisecond),
		WithJobHandler("blocker", func(ctx context.Context, job Job) error {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
			return errors.New("drained")
		}),
	)
	id, err := jobs.Enqueue(context.Background(), "blocker", nil)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	cancel()
	waitFor(t, 3*time.Second, func() bool { return inZset(raw, jobs.readyKey(), id) })
	if inZset(raw, jobs.inflightKey(), id) {
		t.Fatalf("job %s should not be inflight after drain", id)
	}
}

func TestIntegrationNamedValkeyPeer(t *testing.T) {
	var runs atomic.Int64
	jobs, _, _ := setupJobsNamed(t, "vj",
		WithValkeyName("vj"),
		WithPollInterval(30*time.Millisecond),
		WithJobHandler("n", func(ctx context.Context, job Job) error {
			runs.Add(1)
			return nil
		}),
	)
	if _, err := jobs.Enqueue(context.Background(), "n", []byte("x")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return runs.Load() == 1 })
}

func TestIntegrationEnqueueErrors(t *testing.T) {
	jobs, _, _ := setupJobs(t, WithPollInterval(30*time.Millisecond))
	if _, err := jobs.Enqueue(context.Background(), "", nil); err == nil {
		t.Fatal("empty type should error")
	}
}

func TestIntegrationDeadLetterListReplayPurge(t *testing.T) {
	var runs atomic.Int64
	jobs, raw, _ := setupJobs(t,
		WithPollInterval(30*time.Millisecond),
		WithRetryPolicy(50*time.Millisecond, 200*time.Millisecond, time.Second),
		WithJobHandler("once", func(ctx context.Context, job Job) error {
			runs.Add(1)
			return errors.New("boom")
		}),
	)
	id, err := jobs.Enqueue(context.Background(), "once", []byte("p"), WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return inZset(raw, jobs.deadKey(), id) })

	listed, err := jobs.ListDead(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("ListDead: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != id || string(listed[0].Payload) != "p" {
		t.Fatalf("ListDead = %+v, want one job %s", listed, id)
	}
	if listed[0].DeadAt.IsZero() {
		t.Fatal("ListDead should set DeadAt from the ZSET score")
	}
	if metricValue(jobs.Metrics(), "valkey_jobs_dead") < 1 {
		t.Fatalf("depth gauge valkey_jobs_dead missing: %+v", jobs.Metrics())
	}

	if err := jobs.Replay(context.Background(), id); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if inZset(raw, jobs.deadKey(), id) {
		t.Fatal("Replay should remove the job from dead")
	}
	waitFor(t, 3*time.Second, func() bool { return inZset(raw, jobs.deadKey(), id) })
	if runs.Load() != 2 {
		t.Fatalf("runs after replay = %d, want 2 (fresh attempt budget)", runs.Load())
	}

	if err := jobs.PurgeDead(context.Background(), id); err != nil {
		t.Fatalf("PurgeDead: %v", err)
	}
	if inZset(raw, jobs.deadKey(), id) {
		t.Fatal("PurgeDead should remove the member")
	}
	if err := jobs.PurgeDead(context.Background(), id); !errors.Is(err, ErrJobNotDead) {
		t.Fatalf("second PurgeDead = %v, want ErrJobNotDead", err)
	}

	id2, err := jobs.Enqueue(context.Background(), "once", nil, WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("Enqueue 2: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return inZset(raw, jobs.deadKey(), id2) })
	n, err := jobs.PurgeDeadAll(context.Background())
	if err != nil {
		t.Fatalf("PurgeDeadAll: %v", err)
	}
	if n < 1 {
		t.Fatalf("PurgeDeadAll count = %d, want >= 1", n)
	}
	if !zsetEmpty(raw, jobs.deadKey()) {
		t.Fatal("dead ZSET should be empty after PurgeDeadAll")
	}
}
