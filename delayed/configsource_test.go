package delayed

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
)

// TestWithConfigSourceInitializeDoesNotDeadlock is the mutex-reentry
// regression: Init holds c.mu then used to call applyConfig which locked
// again. A hang here is a failed CI, not a flake — bound the wait.
func TestWithConfigSourceInitializeDoesNotDeadlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.json")
	if err := os.WriteFile(path, []byte(`{"poll_interval_ms": 250}`), 0o644); err != nil {
		t.Fatal(err)
	}
	jobs := New(WithConfigSource("jobs", path))
	fw := cf.New(&cf.FrameworkOptions{
		Logs: &cf.LogsSettings{Format: "json", Level: "error"},
		Components: []cf.CaerusComponent{
			cf_valkey.New(cf_valkey.WithDegradedMode(true), cf_valkey.WithAddress("127.0.0.1:1")),
			jobs,
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := fw.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	jobs.mu.RLock()
	got := jobs.pollInterval
	jobs.mu.RUnlock()
	if got != 250*time.Millisecond {
		t.Fatalf("pollInterval = %v, want 250ms from file", got)
	}

	if err := os.WriteFile(path, []byte(`{"poll_interval_ms": 400}`), 0o644); err != nil {
		t.Fatal(err)
	}
	conf, ok := cf.Get[*cf_configuration.Configuration](fw)
	if !ok {
		t.Fatal("configuration component missing")
	}
	if err := conf.Reload("jobs"); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	jobs.mu.RLock()
	got = jobs.pollInterval
	jobs.mu.RUnlock()
	if got != 400*time.Millisecond {
		t.Fatalf("pollInterval after reload = %v, want 400ms", got)
	}
	if err := fw.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
