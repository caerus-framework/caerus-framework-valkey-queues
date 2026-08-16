package jobs

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
	"github.com/caerus-framework/caerus-framework-valkey-queues/internal/chassis"
	"github.com/valkey-io/valkey-go"
)

const (
	// ComponentName is the framework component name for the valkey-jobs
	// component. It is the identifier other components use in GetDependencies
	// to require it.
	ComponentName = "valkey-jobs"

	// ComponentStage is the stage data-layer components initialize in. It is
	// not a built-in bootstrap stage; AddComponent registers it automatically
	// the first time a component declares it.
	ComponentStage = cf.Stage("data")

	// Key namespace this component owns inside the valkey component's prefix
	// space. All keys are built through the peer's Key, so the valkey key
	// prefix (WithKeyPrefix) applies on top.
	nsJobs = "jobs"
)

// ZSET member names within the jobs namespace. The per-job payload lives in a
// hash at jobs:<id>; the ZSETs hold job ids:
//
//   - ready:    score = due epoch ms; claimable now when score <= now.
//   - inflight: score = visibility deadline epoch ms; reaped when passed.
//   - dead:     score = dead-lettered-at epoch ms; inspection only.
const (
	zReady    = "ready"
	zInflight = "inflight"
	zDead     = "dead"
)

// Defaults. Overridable via JobsConfig / With* options / per-enqueue options.
const (
	defaultPollInterval    = 500 * time.Millisecond
	defaultBatchSize       = 16
	defaultConcurrency     = 8
	defaultMaxAttempts     = 3
	defaultVisibility      = time.Minute
	defaultRetention       = 7 * 24 * time.Hour
	defaultRetryFixedDelay = 5 * time.Second
	defaultRetryFixedPhase = 30 * time.Second
	defaultRetryMaxDelay   = 5 * time.Minute
	defaultRetryJitter     = 0.5
	maxListDead            = 100
)

// JobsConfig is the file/env-drivable behavior configuration. Load it through
// the configuration component (caerus-framework-configuration) and pass it via
// WithConfigSource; both JSON and YAML tags are provided.
type JobsConfig struct {
	// WorkerEnabled turns the worker loop on or off. Nil (key omitted) keeps
	// the construct default (WithWorkerEnabled, else on when handlers exist).
	// Explicit false is how a producer-only replica stops claiming.
	WorkerEnabled *bool `json:"worker_enabled,omitempty" yaml:"worker_enabled,omitempty" env:"WORKER_ENABLED"`
	// PollIntervalMs is the worker poll cadence in ms (default 500).
	PollIntervalMs int64 `json:"poll_interval_ms,omitempty" yaml:"poll_interval_ms,omitempty" env:"POLL_INTERVAL_MS"`
	// BatchSize is the max jobs claimed per poll (default 16).
	BatchSize int64 `json:"batch_size,omitempty" yaml:"batch_size,omitempty" env:"BATCH_SIZE"`
	// Concurrency is the max number of handlers running at once (default 8).
	Concurrency int64 `json:"concurrency,omitempty" yaml:"concurrency,omitempty" env:"CONCURRENCY"`
	// RetryFixedDelayMs is the delay between attempts while in the fixed phase
	// (default 5000).
	RetryFixedDelayMs int64 `json:"retry_fixed_delay_ms,omitempty" yaml:"retry_fixed_delay_ms,omitempty" env:"RETRY_FIXED_DELAY_MS"`
	// RetryFixedPhaseMs is how long a failing job retries on the fixed delay
	// before switching to jittered exponential backoff (default 30000).
	RetryFixedPhaseMs int64 `json:"retry_fixed_phase_ms,omitempty" yaml:"retry_fixed_phase_ms,omitempty" env:"RETRY_FIXED_PHASE_MS"`
	// RetryMaxDelayMs caps the backoff (default 300000).
	RetryMaxDelayMs int64 `json:"retry_max_delay_ms,omitempty" yaml:"retry_max_delay_ms,omitempty" env:"RETRY_MAX_DELAY_MS"`
	// RetryJitter is the backoff jitter as a fraction around the base delay
	// (default 0.5 → delay in [0.5x, 1.5x]). Nil keeps the construct default.
	// Explicit zero disables jitter.
	RetryJitter *float64 `json:"retry_jitter,omitempty" yaml:"retry_jitter,omitempty" env:"RETRY_JITTER"`
}

// Sentinel errors for dead-letter operator calls (ListDead / Replay / PurgeDead).
var (
	// ErrJobNotDead means the id is not a member of the dead ZSET (already
	// replayed, purged, or never dead-lettered).
	ErrJobNotDead = errors.New("cf_valkey_jobs: job is not in the dead-letter set")
	// ErrJobMissing means the id was in dead but the payload hash is gone
	// (retention expired). Replay cannot restore it.
	ErrJobMissing = errors.New("cf_valkey_jobs: job payload is gone")
	// ErrAlreadyEnqueued means WithID named a job that still exists (ready,
	// inflight, or dead hash). After ack the id may be reused.
	ErrAlreadyEnqueued = errors.New("cf_valkey_jobs: job id already exists")
	// ErrInvalidJobID means WithID used a reserved name (ready, inflight,
	// dead, cron) or contained ":", which would collide with index keys.
	ErrInvalidJobID = errors.New("cf_valkey_jobs: invalid job id")
)

// Option configures the component at construction time.
type Option func(*options)

type options struct {
	loaded          *JobsConfig
	configSource    string
	configPath      string
	srcEnvPrefix    string
	srcFormat       cf_configuration.Format
	srcFormatSet    bool
	valkeyName      string
	logger          *slog.Logger
	loggerSet       bool
	name            string
	handlers        map[string]JobHandler
	workerEnabled   bool
	pollInterval    time.Duration
	batchSize       int64
	concurrency     int64
	retryFixedDelay time.Duration
	retryFixedPhase time.Duration
	retryMaxDelay   time.Duration
	retryJitter     float64
	shutdownDrain   time.Duration
	repeats         []repeatSpec
}

type repeatSpec struct {
	jobType string
	every   time.Duration
	payload []byte
}

// SourceOption configures the self-registered configuration source created by
// WithConfigSource.
type SourceOption func(*sourceOptions)

type sourceOptions struct {
	envPrefix string
	format    cf_configuration.Format
	formatSet bool
}

// WithSourceEnvPrefix sets the environment overlay prefix for the source
// (default: the uppercase source name with "-" replaced by "_", plus "_").
// An empty prefix disables env overlay.
func WithSourceEnvPrefix(prefix string) SourceOption {
	return func(o *sourceOptions) { o.envPrefix = prefix }
}

// WithSourceFormat forces the file format instead of inferring it from the
// path extension (".yaml"/".yml" → YAML; anything else JSON).
func WithSourceFormat(f cf_configuration.Format) SourceOption {
	return func(o *sourceOptions) { o.format = f; o.formatSet = true }
}

// defaultSourceEnvPrefix derives an environment prefix from a source name.
func defaultSourceEnvPrefix(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_"
}

// WithConfig sets a static configuration snapshot. Non-zero fields of cfg
// override the values set by the convenience options. Prefer WithConfigSource
// when using caerus-framework-configuration with hot-reload.
func WithConfig(cfg JobsConfig) Option {
	return func(o *options) { o.loaded = &cfg }
}

// WithConfigSource binds this component to a named configuration source and
// registers that source with the configuration component (via the framework's
// ConfigSourceRegistrar pass during argv absorption). The module owns the
// Source: the config type, the default EnvPrefix and its Owner (Name(), so
// named instances reload correctly). main only points the instance at where
// the config lives.
//
//	cf_valkey_jobs.New(cf_valkey_jobs.WithConfigSource("jobs", "config/jobs.json"))
//
// A path of "" registers an env-only (fileless) source when the EnvPrefix is
// non-empty. The path CLI override stays --<source-name> (ParseFlags).
// Declares a dependency on "configuration".
func WithConfigSource(name, path string, opts ...SourceOption) Option {
	return func(o *options) {
		so := sourceOptions{envPrefix: defaultSourceEnvPrefix(name)}
		for _, opt := range opts {
			opt(&so)
		}
		o.configSource = name
		o.configPath = path
		o.srcEnvPrefix = so.envPrefix
		o.srcFormat = so.format
		o.srcFormatSet = so.formatSet
	}
}

// WithJobHandler registers a handler for a job type. Jobs of that type claimed
// by the worker run fn; handlers must be idempotent (at-least-once delivery).
// Registering at least one handler enables the worker loop; enqueue works
// regardless.
func WithJobHandler(jobType string, fn JobHandler) Option {
	return func(o *options) {
		if o.handlers == nil {
			o.handlers = make(map[string]JobHandler)
		}
		o.handlers[jobType] = fn
	}
}

// WithRepeat enqueues jobType on a fixed interval while Run is alive. This is
// not a crontab (no calendar, no TZ, no catch-up of missed ticks). Several
// replicas share a valkey NX lock so only one fire happens per interval.
// every shorter than 1s is raised to 1s.
func WithRepeat(jobType string, every time.Duration, payload []byte) Option {
	return func(o *options) {
		if jobType == "" {
			return
		}
		if every < time.Second {
			every = time.Second
		}
		o.repeats = append(o.repeats, repeatSpec{jobType: jobType, every: every, payload: payload})
	}
}

// WithWorkerEnabled forces the worker loop on/off regardless of handlers
// (default: enabled when any handler is registered).
func WithWorkerEnabled(on bool) Option {
	return func(o *options) { o.workerEnabled = on }
}

// WithPollInterval sets the worker poll cadence (default 500ms).
func WithPollInterval(d time.Duration) Option {
	return func(o *options) { o.pollInterval = d }
}

// WithBatchSize sets the max jobs claimed per poll (default 16).
func WithBatchSize(n int64) Option {
	return func(o *options) { o.batchSize = n }
}

// WithConcurrency sets the max number of handlers running at once (default 8).
func WithConcurrency(n int64) Option {
	return func(o *options) { o.concurrency = n }
}

// WithRetryPolicy sets the mixed retry policy: fixedDelay applies while a job
// has been failing for less than fixedPhase; after that, jittered exponential
// backoff (counting attempts since the phase switch) grows toward maxDelay.
func WithRetryPolicy(fixedDelay, fixedPhase, maxDelay time.Duration) Option {
	return func(o *options) {
		o.retryFixedDelay = fixedDelay
		o.retryFixedPhase = fixedPhase
		o.retryMaxDelay = maxDelay
	}
}

// WithShutdownDrainTimeout bounds how long Run waits for in-flight handlers to
// finish after ctx cancellation before draining claimed jobs back to ready
// (default 10s).
func WithShutdownDrainTimeout(d time.Duration) Option {
	return func(o *options) { o.shutdownDrain = d }
}

// WithValkeyName binds the component to a valkey component with the given name
// (WithName on the valkey side). The default is the valkey ComponentName
// ("valkey").
func WithValkeyName(name string) Option {
	return func(o *options) { o.valkeyName = name }
}

// WithLogger overrides the logger used for component diagnostics. By default
// the component logs through the framework logs component (declared in
// GetDependencies); WithLogger is an explicit override for tests and embedded
// use and wins over the framework logger. slog.Default() remains the fallback
// only when neither is available.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger; o.loggerSet = true }
}

// WithName sets a custom component name, allowing multiple jobs instances in
// the same process. The default name is "valkey-jobs" (ComponentName).
// Retrieve named instances with GetByName[*CFValkeyJobs](fw, "jobs").
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// JobHandler processes one claimed job. It must honor ctx cancellation and be
// idempotent: delivery is at-least-once, and a hung handler is retried after
// its visibility timeout expires. A nil error acknowledges the job; any error
// requeues it per the retry policy (dead-lettered once attempts are
// exhausted).
type JobHandler func(ctx context.Context, job Job) error

// Job is a claimed job handed to a JobHandler, or a dead-letter row from ListDead.
type Job struct {
	ID          string
	Type        string
	Payload     []byte
	Attempts    int64 // 1-based attempt number of this run (stored count on ListDead)
	MaxAttempts int64
	CreatedAt   time.Time
	// DeadAt is set by ListDead (ZSET score). Zero on a claimed handler job.
	DeadAt time.Time
}

// EnqueueOption configures a single Enqueue call.
type EnqueueOption func(*enqueueOptions)

type enqueueOptions struct {
	runAt       time.Time
	id          string
	maxAttempts int64
	visibility  time.Duration
	retention   time.Duration
}

// WithRunAt schedules the job to become claimable at t. Zero or a past time
// makes it immediately claimable.
func WithRunAt(t time.Time) EnqueueOption {
	return func(o *enqueueOptions) { o.runAt = t }
}

// WithID sets a stable job id for this Enqueue. A second Enqueue with the
// same id while the hash still exists returns ErrAlreadyEnqueued (at-most-one
// copy in the queue). This is not exactly-once *execution*: a handler may
// still run more than once if visibility expires. Empty id is ignored.
// Ids must be a single path segment: no ":", and not ready / inflight /
// dead / cron (those are the index keys). Invalid ids return ErrInvalidJobID
// before any Valkey write.
func WithID(id string) EnqueueOption {
	return func(o *enqueueOptions) { o.id = id }
}

// WithDelay schedules the job d from now (overrides WithRunAt).
func WithDelay(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.runAt = time.Now().Add(d) }
}

// WithMaxAttempts sets the max number of runs before dead-lettering
// (default 3).
func WithMaxAttempts(n int64) EnqueueOption {
	return func(o *enqueueOptions) { o.maxAttempts = n }
}

// WithVisibility sets how long a claimed job stays owned by a worker before
// it is considered hung and retried (default 1m). Set it well above the
// handler's expected runtime.
func WithVisibility(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.visibility = d }
}

// WithRetention sets how long the job payload is kept after enqueue (and after
// dead-lettering) for inspection (default 7d).
func WithRetention(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.retention = d }
}

// CFValkeyJobs is the caerus-framework-valkey-jobs component: a lightweight
// delayed task queue over a cf_valkey.CFValkey peer. It is a stateless
// consumer of the peer (never a client snapshot) and builds every command
// through the peer's live Client() and prefix-aware Key(), so reconnects and
// key prefixes stay consistent.
//
// Delivery is at-least-once: a job runs, its handler either acknowledges it,
// requeues it with a retry delay, or (attempts exhausted) dead-letters it. A
// crashed or hung worker is recovered via the visibility timeout (the job is
// re-queued after its deadline). Handlers must be idempotent.
type CFValkeyJobs struct {
	mu            sync.RWMutex
	configSource  string
	configPath    string
	srcEnvPrefix  string
	srcFormat     cf_configuration.Format
	srcFormatSet  bool
	valkeyName    string
	loggerSet     bool
	workerOn      bool
	pollInterval  time.Duration
	batchSize     int64
	concurrency   int64
	retryFixedD   time.Duration
	retryFixedP   time.Duration
	retryMaxD     time.Duration
	retryJitter   float64
	shutdownDrain time.Duration
	handlers      map[string]JobHandler
	semCh         chan struct{}
	vk            *cf_valkey.CFValkey
	logger        *slog.Logger
	logsSub       *cf_logs.Subscription
	fw            *cf.CaerusFramework
	name          string

	inflightMu sync.Mutex
	inflight   map[string]struct{}
	handlerWG  sync.WaitGroup

	metersMu sync.Mutex
	meters   map[string]*typeMeter

	reloads atomic.Uint64
	repeats []repeatSpec
}

type typeMeter struct {
	enqueued atomic.Uint64
	run      atomic.Uint64
	failed   atomic.Uint64
	requeued atomic.Uint64
	dead     atomic.Uint64
	durSumNs atomic.Uint64
	durCount atomic.Uint64
}

// New creates a jobs component. The valkey peer is resolved at Init, not here.
func New(opts ...Option) *CFValkeyJobs {
	o := options{
		logger:          slog.Default(),
		workerEnabled:   true,
		pollInterval:    defaultPollInterval,
		batchSize:       defaultBatchSize,
		concurrency:     defaultConcurrency,
		retryFixedDelay: defaultRetryFixedDelay,
		retryFixedPhase: defaultRetryFixedPhase,
		retryMaxDelay:   defaultRetryMaxDelay,
		retryJitter:     defaultRetryJitter,
		shutdownDrain:   10 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}
	c := &CFValkeyJobs{
		configSource:  o.configSource,
		configPath:    o.configPath,
		srcEnvPrefix:  o.srcEnvPrefix,
		srcFormat:     o.srcFormat,
		srcFormatSet:  o.srcFormatSet,
		valkeyName:    o.valkeyName,
		logger:        o.logger,
		loggerSet:     o.loggerSet,
		workerOn:      o.workerEnabled,
		pollInterval:  o.pollInterval,
		batchSize:     o.batchSize,
		concurrency:   o.concurrency,
		retryFixedD:   o.retryFixedDelay,
		retryFixedP:   o.retryFixedPhase,
		retryMaxD:     o.retryMaxDelay,
		retryJitter:   o.retryJitter,
		shutdownDrain: o.shutdownDrain,
		handlers:      o.handlers,
		name:          o.name,
		semCh:         make(chan struct{}, o.concurrency),
		inflight:      make(map[string]struct{}),
		meters:        make(map[string]*typeMeter),
		repeats:       o.repeats,
	}
	if o.loaded != nil {
		c.applyConfig(*o.loaded)
	}
	return c
}

// applyConfig overlays present fields of cfg onto the component's settings.
// It runs last, so a loaded config wins over option-set defaults. Callers that
// already hold c.mu (Init, OnConfigReload) must use this; it does not lock.
func (c *CFValkeyJobs) applyConfig(cfg JobsConfig) {
	if cfg.WorkerEnabled != nil {
		c.workerOn = *cfg.WorkerEnabled
	}
	if cfg.PollIntervalMs > 0 {
		c.pollInterval = time.Duration(cfg.PollIntervalMs) * time.Millisecond
	}
	if cfg.BatchSize > 0 {
		c.batchSize = cfg.BatchSize
	}
	if cfg.Concurrency > 0 {
		c.concurrency = cfg.Concurrency
	}
	if c.semCh == nil || cap(c.semCh) != int(c.concurrency) {
		c.semCh = make(chan struct{}, c.concurrency)
	}
	if cfg.RetryFixedDelayMs > 0 {
		c.retryFixedD = time.Duration(cfg.RetryFixedDelayMs) * time.Millisecond
	}
	if cfg.RetryFixedPhaseMs > 0 {
		c.retryFixedP = time.Duration(cfg.RetryFixedPhaseMs) * time.Millisecond
	}
	if cfg.RetryMaxDelayMs > 0 {
		c.retryMaxD = time.Duration(cfg.RetryMaxDelayMs) * time.Millisecond
	}
	if cfg.RetryJitter != nil {
		c.retryJitter = *cfg.RetryJitter
	}
}

// Name implements cf.CaerusComponent. Returns the custom name set via WithName,
// or the default ComponentName ("valkey-jobs") if no custom name was set.
func (c *CFValkeyJobs) Name() string {
	if c.name != "" {
		return c.name
	}
	return ComponentName
}

// GetInitOrderStage implements cf.CaerusComponent.
func (c *CFValkeyJobs) GetInitOrderStage() cf.Stage { return ComponentStage }

// GetDependencies implements cf.Dependencies. The component depends on the
// valkey component it consumes (the actual peer name when WithValkeyName is
// set, the default ComponentName otherwise), logs through the framework logs
// component, and depends on configuration when WithConfigSource is set. Peer
// names are fixed at construction, so the graph is stable before Init.
func (c *CFValkeyJobs) GetDependencies() []string {
	deps := []string{chassis.PeerName(c.valkeyName), cf_logs.ComponentName}
	if c.configSource != "" {
		deps = append(deps, cf_configuration.ComponentName)
	}
	return deps
}

// Init implements cf.CaerusComponent. It resolves the valkey peer component
// (by name or the default "valkey"), failing fast when it is missing or not
// yet initialized. No connection is opened here; the peer owns its client.
func (c *CFValkeyJobs) Init(ctx context.Context, fw *cf.CaerusFramework) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.vk != nil {
		return nil // already initialized
	}
	c.fw = fw
	if !c.loggerSet {
		if logs, ok := cf.Get[*cf_logs.Logs](fw); ok {
			c.logsSub = logs.OnReconfigureFor(c.Name(), func(l *slog.Logger) { c.logger = l })
		}
	}
	if c.configSource != "" {
		if err := c.applyConfigFromSource(); err != nil {
			return err
		}
	}
	vk, err := chassis.ResolveValkey(fw, c.valkeyName)
	if err != nil {
		return fmt.Errorf("cf_valkey_jobs: %w", err)
	}
	c.vk = vk
	if vk.Client() == nil {
		c.logger.Warn("cf_valkey_jobs: valkey Client() is nil (degraded / not connected); Init succeeded, Health stays not-ready until ping works",
			"valkey", chassis.PeerName(c.valkeyName))
	}
	return nil
}

// peerName returns the configured valkey peer name, or the valkey
// ComponentName when unset. Callers must not hold the mutex.
func (c *CFValkeyJobs) peerName() string {
	if c.valkeyName != "" {
		return c.valkeyName
	}
	return cf_valkey.ComponentName
}

// applyConfigFromSource reloads the bound configuration source and overlays it
// onto the component's settings. It must be called with the mutex held.
func (c *CFValkeyJobs) applyConfigFromSource() error {
	conf, ok := cf.Get[*cf_configuration.Configuration](c.fw)
	if !ok {
		return errors.New("cf_valkey_jobs: configuration component not registered")
	}
	loaded, ok := cf_configuration.Get[JobsConfig](conf, c.configSource)
	if !ok {
		return fmt.Errorf("cf_valkey_jobs: configuration source %q not found", c.configSource)
	}
	c.applyConfig(*loaded)
	return nil
}

// jobsKey builds a namespace key through the valkey peer's prefix-aware Key.
func (c *CFValkeyJobs) jobsKey(parts ...string) string {
	if vk := c.peer(); vk != nil {
		return vk.Key(append([]string{nsJobs}, parts...)...)
	}
	return strings.Join(append([]string{nsJobs}, parts...), ":")
}

// readyKey / inflightKey / deadKey / jobKey resolve the fixed ZSETs and a job's
// payload hash.
func (c *CFValkeyJobs) readyKey() string    { return c.jobsKey(zReady) }
func (c *CFValkeyJobs) inflightKey() string { return c.jobsKey(zInflight) }
func (c *CFValkeyJobs) deadKey() string     { return c.jobsKey(zDead) }
func (c *CFValkeyJobs) jobKey(id string) string {
	return c.jobsKey(id)
}

// peer returns the resolved valkey peer component (nil before Init or after
// Shutdown).
func (c *CFValkeyJobs) peer() *cf_valkey.CFValkey {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.vk
}

// client returns the peer's live valkey client. It follows the peer-pointer
// convention: the peer is re-read per use, so a client swap on config reload is
// picked up immediately.
func (c *CFValkeyJobs) client() (valkey.Client, error) {
	vk := c.peer()
	if vk == nil {
		return nil, errors.New("cf_valkey_jobs: component is not initialized")
	}
	cl := vk.Client()
	if cl == nil {
		return nil, errors.New("cf_valkey_jobs: valkey client is not initialized")
	}
	return cl, nil
}

// Enqueue stores a job with a JSON-ish opaque payload. The job type names the
// handler that must be registered on a worker. Returns the job id.
func (c *CFValkeyJobs) Enqueue(ctx context.Context, jobType string, payload []byte, opts ...EnqueueOption) (string, error) {
	if jobType == "" {
		return "", errors.New("cf_valkey_jobs: Enqueue: empty job type")
	}
	eo := enqueueOptions{
		maxAttempts: defaultMaxAttempts,
		visibility:  defaultVisibility,
		retention:   defaultRetention,
	}
	for _, opt := range opts {
		opt(&eo)
	}
	if eo.maxAttempts < 1 {
		eo.maxAttempts = 1
	}
	if eo.visibility <= 0 {
		eo.visibility = defaultVisibility
	}
	if eo.retention <= 0 {
		eo.retention = defaultRetention
	}
	now := time.Now()
	dueMs := now.UnixMilli()
	if !eo.runAt.IsZero() {
		if d := eo.runAt.UnixMilli(); d > dueMs {
			dueMs = d
		}
	}
	id := eo.id
	if id == "" {
		var err error
		id, err = newID()
		if err != nil {
			return "", err
		}
	}
	if err := validateJobID(id); err != nil {
		return "", err
	}
	client, err := c.client()
	if err != nil {
		return "", err
	}
	resp := client.Do(ctx, client.B().Eval().Script(luaEnqueue).
		Numkeys(2).
		Key(c.jobKey(id), c.readyKey()).
		Arg(jobType).
		Arg(string(payload)).
		Arg(strconv.FormatInt(eo.maxAttempts, 10)).
		Arg(strconv.FormatInt(now.UnixMilli(), 10)).
		Arg(strconv.FormatInt(eo.retention.Milliseconds(), 10)).
		Arg(strconv.FormatInt(dueMs, 10)).
		Arg(id).
		Arg(strconv.FormatInt(eo.visibility.Milliseconds(), 10)).
		Build())
	if err := resp.Error(); err != nil {
		return "", err
	}
	n, err := resp.AsInt64()
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", ErrAlreadyEnqueued
	}
	c.meter(jobType).enqueued.Add(1)
	return id, nil
}

const (
	listDeadCap = maxListDead
)

// ListDead returns a page of dead-lettered jobs, oldest dead-letter first
// (ZSET score = dead-at). offset is 0-based. limit is capped at 100; a
// non-positive limit means that cap.
func (c *CFValkeyJobs) ListDead(ctx context.Context, offset, limit int64) ([]Job, error) {
	client, err := c.client()
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 || limit > listDeadCap {
		limit = listDeadCap
	}
	resp := client.Do(ctx, client.B().Zrangebyscore().Key(c.deadKey()).
		Min("-inf").Max("+inf").
		Withscores().
		Limit(offset, limit).
		Build())
	if resp.Error() != nil {
		return nil, resp.Error()
	}
	zs, err := resp.AsZScores()
	if err != nil {
		return nil, err
	}
	out := make([]Job, 0, len(zs))
	for _, z := range zs {
		job, err := c.loadJob(ctx, client, z.Member)
		if err != nil {
			job = Job{ID: z.Member}
		}
		job.DeadAt = time.UnixMilli(int64(z.Score))
		out = append(out, job)
	}
	return out, nil
}

// Replay moves a dead-lettered job back to ready with the same id, attempts
// and retry bookkeeping cleared, due now. The next claim is attempt 1 of
// max_attempts again. Returns ErrJobNotDead or ErrJobMissing when the row
// cannot be restored.
func (c *CFValkeyJobs) Replay(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("cf_valkey_jobs: Replay: empty job id")
	}
	client, err := c.client()
	if err != nil {
		return err
	}
	nowMs := time.Now().UnixMilli()
	resp := client.Do(ctx, client.B().Eval().Script(luaReplay).
		Numkeys(3).
		Key(c.deadKey(), c.readyKey(), c.jobKey(id)).
		Arg(id).
		Arg(strconv.FormatInt(nowMs, 10)).
		Build())
	if resp.Error() != nil {
		return resp.Error()
	}
	n, err := resp.AsInt64()
	if err != nil {
		return err
	}
	switch n {
	case 1:
		return nil
	case -1:
		return ErrJobNotDead
	case -2:
		return ErrJobMissing
	default:
		return fmt.Errorf("cf_valkey_jobs: Replay: unexpected status %d", n)
	}
}

// PurgeDead deletes one dead-lettered job (ZSET member + payload hash).
// Returns ErrJobNotDead if the id is not in the dead set (does not delete a
// ready or inflight hash).
func (c *CFValkeyJobs) PurgeDead(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("cf_valkey_jobs: PurgeDead: empty job id")
	}
	client, err := c.client()
	if err != nil {
		return err
	}
	resp := client.Do(ctx, client.B().Eval().Script(luaPurgeDead).
		Numkeys(2).
		Key(c.deadKey(), c.jobKey(id)).
		Arg(id).
		Build())
	if resp.Error() != nil {
		return resp.Error()
	}
	n, err := resp.AsInt64()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrJobNotDead
	}
	return nil
}

// PurgeDeadAll deletes every dead-lettered job. The method name is the
// confirmation; an empty DLQ succeeds with a zero count.
func (c *CFValkeyJobs) PurgeDeadAll(ctx context.Context) (int64, error) {
	client, err := c.client()
	if err != nil {
		return 0, err
	}
	resp := client.Do(ctx, client.B().Zrangebyscore().Key(c.deadKey()).
		Min("-inf").Max("+inf").Build())
	if resp.Error() != nil {
		return 0, resp.Error()
	}
	ids, err := resp.AsStrSlice()
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	cmds := make([]valkey.Completed, 0, len(ids)+1)
	for _, id := range ids {
		cmds = append(cmds, client.B().Del().Key(c.jobKey(id)).Build())
	}
	cmds = append(cmds, client.B().Del().Key(c.deadKey()).Build())
	for _, r := range client.DoMulti(ctx, cmds...) {
		if r.Error() != nil {
			return 0, r.Error()
		}
	}
	return int64(len(ids)), nil
}

func (c *CFValkeyJobs) loadJob(ctx context.Context, client valkey.Client, id string) (Job, error) {
	resp := client.Do(ctx, client.B().Hgetall().Key(c.jobKey(id)).Build())
	if resp.Error() != nil {
		return Job{}, resp.Error()
	}
	m, err := resp.AsStrMap()
	if err != nil {
		return Job{}, err
	}
	if len(m) == 0 {
		return Job{}, ErrJobMissing
	}
	job := Job{ID: id, Type: m["type"], Payload: []byte(m["payload"])}
	job.Attempts, _ = strconv.ParseInt(m["attempts"], 10, 64)
	job.MaxAttempts, _ = strconv.ParseInt(m["max_attempts"], 10, 64)
	if ms, err := strconv.ParseInt(m["created_ms"], 10, 64); err == nil {
		job.CreatedAt = time.UnixMilli(ms)
	}
	return job, nil
}

// Run implements cf.Runnable. It polls until ctx is canceled, then drains
// claimed jobs back to ready. Each poll checks worker_enabled so a reload can
// stop or resume claiming without a process restart. No handlers still means
// the loop does not claim.
func (c *CFValkeyJobs) Run(ctx context.Context) error {
	c.loop(ctx)
	c.drain(context.WithoutCancel(ctx))
	return nil
}

func (c *CFValkeyJobs) workerEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.workerOn && len(c.handlers) > 0
}

// loop polls on the configured cadence until ctx is done, then waits for
// in-flight handlers (bounded) before returning.
func (c *CFValkeyJobs) loop(ctx context.Context) {
	for {
		interval := c.currentPollInterval()
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			c.waitHandlers()
			return
		case <-timer.C:
			c.tickRepeats(ctx)
			if c.workerEnabled() {
				c.tick(ctx)
			}
		}
	}
}

func (c *CFValkeyJobs) currentPollInterval() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pollInterval
}

func (c *CFValkeyJobs) currentBatchSize() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.batchSize
}

func (c *CFValkeyJobs) sem() chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.semCh
}

func (c *CFValkeyJobs) tickRepeats(ctx context.Context) {
	if len(c.repeats) == 0 {
		return
	}
	client, err := c.client()
	if err != nil {
		return
	}
	for _, r := range c.repeats {
		px := r.every.Milliseconds()
		if px < 1000 {
			px = 1000
		}
		lock := c.jobsKey("cron", r.jobType)
		resp := client.Do(ctx, client.B().Set().Key(lock).Value("1").Nx().PxMilliseconds(px).Build())
		ok, err := nxAcquired(resp)
		if err != nil {
			c.logger.Warn("cf_valkey_jobs: repeat lock failed", "type", r.jobType, "err", err)
			continue
		}
		if !ok {
			continue
		}
		if _, err := c.Enqueue(ctx, r.jobType, r.payload); err != nil {
			c.logger.Warn("cf_valkey_jobs: repeat enqueue failed", "type", r.jobType, "err", err)
		}
	}
}

// tick runs one reap + claim pass. Both transitions are atomic per id via Lua;
// a job can move at most one way per pass.
func (c *CFValkeyJobs) tick(ctx context.Context) {
	client, err := c.client()
	if err != nil {
		return
	}
	now := time.Now()
	nowMs := now.UnixMilli()

	// Reap: recover jobs whose visibility deadline passed (hung/crashed worker).
	for _, id := range c.dueIDs(ctx, client, c.inflightKey(), nowMs) {
		c.reapOne(ctx, id, nowMs)
	}

	// Claim: run due jobs.
	for _, id := range c.dueIDs(ctx, client, c.readyKey(), nowMs) {
		c.processOne(ctx, client, id)
	}
}

// dueIDs lists up to batch ids from zset whose score <= nowMs.
func (c *CFValkeyJobs) dueIDs(ctx context.Context, client valkey.Client, key string, nowMs int64) []string {
	resp := client.Do(ctx, client.B().Zrangebyscore().Key(key).
		Min("-inf").Max(strconv.FormatInt(nowMs, 10)).
		Limit(0, c.currentBatchSize()).Build())
	if resp.Error() != nil {
		c.logger.Warn("cf_valkey_jobs: poll failed", "err", resp.Error())
		return nil
	}
	ids, err := resp.AsStrSlice()
	if err != nil {
		c.logger.Warn("cf_valkey_jobs: poll decode failed", "err", err)
		return nil
	}
	return ids
}

// reapOne moves an expired-inflight job back to ready (or dead once attempts
// are exhausted). bump increments attempts: the hung run counts as one.
func (c *CFValkeyJobs) reapOne(ctx context.Context, id string, nowMs int64) {
	status, jtype, err := c.release(ctx, id, nowMs, nowMs, 0, 0, true)
	if err != nil {
		c.logger.Warn("cf_valkey_jobs: reap failed", "job", id, "err", err)
		return
	}
	switch status {
	case releaseRequeued:
		c.meter(jtype).requeued.Add(1)
		c.logger.Info("cf_valkey_jobs: recovered hung job", "job", id, "type", jtype)
	case releaseDead:
		c.meter(jtype).dead.Add(1)
		c.logger.Error("cf_valkey_jobs: job dead-lettered after reap", "job", id, "type", jtype)
	}
}

// processOne claims a due job (atomic per id) and dispatches its handler. The
// concurrency token is held until the handler completes so at most
// `concurrency` jobs run at once.
func (c *CFValkeyJobs) processOne(ctx context.Context, client valkey.Client, id string) {
	sem := c.sem()
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return
	}

	nowMs := time.Now().UnixMilli()
	resp := client.Do(ctx, client.B().Eval().Script(luaClaim).
		Numkeys(3).
		Key(c.jobKey(id), c.readyKey(), c.inflightKey()).
		Arg(id).
		Arg(strconv.FormatInt(nowMs, 10)).
		Build())
	if resp.Error() != nil {
		<-sem
		if !errors.Is(resp.Error(), valkey.Nil) {
			c.logger.Warn("cf_valkey_jobs: claim failed", "job", id, "err", resp.Error())
		}
		return
	}
	fields, err := resp.AsStrSlice()
	if err != nil || len(fields) < 6 {
		<-sem
		c.logger.Error("cf_valkey_jobs: claim returned malformed result", "job", id, "err", err)
		return
	}
	job, retryStart, jitterBase, err := parseClaim(fields)
	if err != nil {
		<-sem
		c.logger.Error("cf_valkey_jobs: claim decode failed", "job", id, "err", err)
		return
	}
	if job.ID != id {
		<-sem
		return
	}
	c.trackInflight(id)
	c.dispatch(ctx, job, retryStart, jitterBase, sem)
}

func (c *CFValkeyJobs) dispatch(ctx context.Context, job Job, retryStart, jitterBase int64, sem chan struct{}) {
	c.mu.RLock()
	handler, ok := c.handlers[job.Type]
	c.mu.RUnlock()
	if !ok {
		// No handler for this type: dead-letter immediately rather than hot-loop.
		status, err := c.dead(ctx, job.ID)
		if err != nil {
			c.logger.Warn("cf_valkey_jobs: dead-letter failed", "job", job.ID, "type", job.Type, "err", err)
		} else if status == releaseDead {
			c.logger.Error("cf_valkey_jobs: no handler for job type; dead-lettering", "job", job.ID, "type", job.Type)
		}
		c.meter(job.Type).dead.Add(1)
		c.untrackInflight(job.ID)
		<-sem
		return
	}

	c.handlerWG.Add(1)
	go func() {
		defer c.handlerWG.Done()
		defer func() { <-sem }()
		start := time.Now()
		err := handler(ctx, job)
		durNs := time.Since(start).Nanoseconds()
		m := c.meter(job.Type)
		m.run.Add(1)
		m.durSumNs.Add(uint64(durNs))
		m.durCount.Add(1)
		if err == nil {
			if ackErr := c.ack(ctx, job.ID); ackErr != nil {
				c.logger.Warn("cf_valkey_jobs: ack failed", "job", job.ID, "type", job.Type, "err", ackErr)
			}
			c.untrackInflight(job.ID)
			return
		}
		m.failed.Add(1)
		now := time.Now()
		delay, newStart, newBase := c.retryDelay(job.Attempts, retryStart, jitterBase, now)
		due := now.Add(delay).UnixMilli()
		status, _, relErr := c.release(ctx, job.ID, due, now.UnixMilli(), newStart, newBase, false)
		if relErr != nil {
			c.logger.Warn("cf_valkey_jobs: requeue failed", "job", job.ID, "type", job.Type, "err", relErr)
			return
		}
		switch status {
		case releaseRequeued:
			m.requeued.Add(1)
			c.logger.Info("cf_valkey_jobs: job requeued", "job", job.ID, "type", job.Type, "attempt", job.Attempts, "delay", delay.String())
		case releaseDead:
			m.dead.Add(1)
			c.logger.Error("cf_valkey_jobs: job dead-lettered", "job", job.ID, "type", job.Type, "attempt", job.Attempts)
		default:
			c.logger.Debug("cf_valkey_jobs: release skipped (not inflight)", "job", job.ID, "type", job.Type)
		}
		c.untrackInflight(job.ID)
	}()
}

// retryDelay computes the mixed-policy delay after a failed attempt: a fixed
// delay while the job has been retrying for less than the fixed phase, then
// jittered exponential backoff counting attempts since the phase switch,
// capped at the max delay. newStart returns the retry epoch to persist (the
// first failure time), newBase the attempts counter at the phase switch.
func (c *CFValkeyJobs) retryDelay(attempts, retryStart, jitterBase int64, now time.Time) (delay time.Duration, newStart, newBase int64) {
	c.mu.RLock()
	fixedD, fixedP, maxD, jitter := c.retryFixedD, c.retryFixedP, c.retryMaxD, c.retryJitter
	c.mu.RUnlock()

	newStart = retryStart
	if newStart == 0 {
		newStart = now.UnixMilli()
	}
	newBase = jitterBase
	delay = fixedD
	if now.UnixMilli()-newStart <= fixedP.Milliseconds() {
		return delay, newStart, newBase
	}
	if newBase == 0 {
		newBase = attempts
	}
	n := attempts - newBase + 1
	if n < 1 {
		n = 1
	}
	if n > 16 {
		n = 16
	}
	delay = fixedD * time.Duration(1<<(n-1))
	if delay > maxD {
		delay = maxD
	}
	if jitter > 0 {
		factor := (1 - jitter) + 2*jitter*rand.Float64()
		delay = time.Duration(float64(delay) * factor)
		if delay > maxD {
			delay = maxD
		}
	}
	return delay, newStart, newBase
}

// ack removes a finished job (inflight entry + payload). Uses the peer's live
// Client() so a reload mid-handler does not ack on a closed snapshot.
func (c *CFValkeyJobs) ack(ctx context.Context, id string) error {
	client, err := c.client()
	if err != nil {
		return err
	}
	return client.Do(ctx, client.B().Eval().Script(luaAck).
		Numkeys(3).
		Key(c.inflightKey(), c.deadKey(), c.jobKey(id)).
		Arg(id).
		Build()).Error()
}

// dead dead-letters a job immediately without retrying (e.g. no handler
// registered for its type). Returns -1 if the job was not inflight.
func (c *CFValkeyJobs) dead(ctx context.Context, id string) (int, error) {
	client, err := c.client()
	if err != nil {
		return 0, err
	}
	now := time.Now().UnixMilli()
	resp := client.Do(ctx, client.B().Eval().Script(luaDead).
		Numkeys(3).
		Key(c.inflightKey(), c.deadKey(), c.jobKey(id)).
		Arg(id).
		Arg(strconv.FormatInt(now, 10)).
		Arg(strconv.FormatInt(defaultRetention.Milliseconds(), 10)).
		Build())
	if resp.Error() != nil {
		return 0, resp.Error()
	}
	n, err := resp.AsInt64()
	return int(n), err
}

// release moves a job from inflight to ready (retry), dead, or neither (already
// handled). bump increments attempts (true for reap recovery, false for a
// handler failure where the claim already counted). Returns the outcome and
// the job type. Uses the peer's live Client().
func (c *CFValkeyJobs) release(ctx context.Context, id string, dueMs, deadAtMs, retryStartMs, jitterBase int64, bump bool) (int, string, error) {
	client, err := c.client()
	if err != nil {
		return 0, "", err
	}
	bumpS := "0"
	if bump {
		bumpS = "1"
	}
	resp := client.Do(ctx, client.B().Eval().Script(luaRelease).
		Numkeys(4).
		Key(c.inflightKey(), c.jobKey(id), c.readyKey(), c.deadKey()).
		Arg(id).
		Arg(strconv.FormatInt(dueMs, 10)).
		Arg(strconv.FormatInt(defaultRetention.Milliseconds(), 10)).
		Arg(strconv.FormatInt(deadAtMs, 10)).
		Arg(strconv.FormatInt(defaultRetention.Milliseconds(), 10)).
		Arg(bumpS).
		Arg(strconv.FormatInt(retryStartMs, 10)).
		Arg(strconv.FormatInt(jitterBase, 10)).
		Build())
	if resp.Error() != nil {
		return 0, "", resp.Error()
	}
	fields, err := resp.AsStrSlice()
	if err != nil || len(fields) < 2 {
		return 0, "", errors.New("cf_valkey_jobs: release returned malformed result")
	}
	status, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, "", err
	}
	return status, fields[1], nil
}

const (
	releaseRequeued = 1
	releaseDead     = 0
	releaseMissed   = -1
)

// drain requeues every still-claimed job on graceful shutdown so a deploy does
// not lose work.
func (c *CFValkeyJobs) drain(ctx context.Context) {
	ids := c.snapshotInflight()
	if len(ids) == 0 {
		return
	}
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	nowMs := time.Now().UnixMilli()
	for _, id := range ids {
		status, jtype, err := c.release(dctx, id, nowMs, nowMs, 0, 0, false)
		if err != nil {
			c.logger.Warn("cf_valkey_jobs: drain requeue failed", "job", id, "err", err)
			continue
		}
		switch status {
		case releaseRequeued:
			c.meter(jtype).requeued.Add(1)
		case releaseDead:
			c.meter(jtype).dead.Add(1)
		}
	}
	c.inflightMu.Lock()
	c.inflight = make(map[string]struct{})
	c.inflightMu.Unlock()
}

// waitHandlers blocks until running handlers finish, bounded by the shutdown
// drain timeout so a non-cooperating handler cannot hang shutdown.
func (c *CFValkeyJobs) waitHandlers() {
	c.mu.RLock()
	timeout := c.shutdownDrain
	c.mu.RUnlock()
	done := make(chan struct{})
	go func() {
		c.handlerWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		c.logger.Warn("cf_valkey_jobs: timed out waiting for handlers; draining claimed jobs")
	}
}

func (c *CFValkeyJobs) trackInflight(id string) {
	c.inflightMu.Lock()
	c.inflight[id] = struct{}{}
	c.inflightMu.Unlock()
}

func (c *CFValkeyJobs) untrackInflight(id string) {
	c.inflightMu.Lock()
	delete(c.inflight, id)
	c.inflightMu.Unlock()
}

func (c *CFValkeyJobs) snapshotInflight() []string {
	c.inflightMu.Lock()
	defer c.inflightMu.Unlock()
	ids := make([]string, 0, len(c.inflight))
	for id := range c.inflight {
		ids = append(ids, id)
	}
	return ids
}

// OnConfigReload implements cf.ConfigReloader. It re-reads the worker tunables
// from the bound configuration source; the poll loop picks them up on the next
// tick, and a concurrency change swaps the semaphore.
func (c *CFValkeyJobs) OnConfigReload(source string, cfg any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if source != c.configSource || c.vk == nil || c.fw == nil {
		return
	}
	if _, ok := cfg.(*JobsConfig); !ok {
		c.logger.Error("cf_valkey_jobs: config reload rejected", "source", source, "type", fmt.Sprintf("%T", cfg))
		return
	}
	if err := c.applyConfigFromSource(); err != nil {
		c.logger.Error("cf_valkey_jobs: config reload rejected; keeping previous", "err", err)
		return
	}
	c.reloads.Add(1)
	c.logger.Info("cf_valkey_jobs: worker tunables reloaded",
		"poll_interval", c.pollInterval.String(),
		"batch_size", c.batchSize,
		"concurrency", c.concurrency,
	)
}

// RegisterConfigSources implements cf.ConfigSourceRegistrar. The framework
// calls it during argv absorption; it registers this component's configuration
// source (name, path, env prefix, format, Owner) with the configuration
// component. No-op when no source is bound.
func (c *CFValkeyJobs) RegisterConfigSources(conf any) error {
	cfg, ok := conf.(*cf_configuration.Configuration)
	if !ok {
		return fmt.Errorf("cf_valkey_jobs: RegisterConfigSources: expected configuration component, got %T", conf)
	}
	if c.configSource == "" {
		return nil
	}
	format := c.srcFormat
	if !c.srcFormatSet {
		if p := strings.ToLower(c.configPath); strings.HasSuffix(p, ".yaml") || strings.HasSuffix(p, ".yml") {
			format = cf_configuration.FormatYAML
		} else {
			format = cf_configuration.FormatJSON
		}
	}
	return cf_configuration.AddSource(cfg, cf_configuration.Source[JobsConfig]{
		Name:      c.configSource,
		Path:      c.configPath,
		Format:    format,
		Owner:     c.Name(),
		EnvPrefix: c.srcEnvPrefix,
	})
}

// Shutdown implements cf.CaerusComponent. It unsubscribes the logs
// subscription and drops the valkey peer. Further use returns an error.
func (c *CFValkeyJobs) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logsSub != nil {
		c.logsSub.Unsubscribe()
		c.logsSub = nil
	}
	c.vk = nil
	return nil
}

// Client returns the peer's live valkey client (nil before Init or after
// Shutdown). Useful for direct commands against the same key space.
func (c *CFValkeyJobs) Client() valkey.Client {
	vk := c.peer()
	if vk == nil {
		return nil
	}
	return vk.Client()
}

// Health implements cf.HealthProvider. It reports healthy while the peer's
// valkey client is initialized; real connectivity is owned by the valkey
// component's own Health (aggregated by observability's /readyz).
func (c *CFValkeyJobs) Health(ctx context.Context) error {
	vk := c.peer()
	if vk == nil || vk.Client() == nil {
		return errors.New("cf_valkey_jobs: valkey client is not initialized")
	}
	return nil
}

// meter returns (creating on first use) the per-type counter group.
func (c *CFValkeyJobs) meter(jobType string) *typeMeter {
	c.metersMu.Lock()
	defer c.metersMu.Unlock()
	m, ok := c.meters[jobType]
	if !ok {
		m = &typeMeter{}
		c.meters[jobType] = m
	}
	return m
}

// Metrics implements cf_observability.MetricsProvider. It reports operation
// counters (per job type) while the peer's client is initialized; before Init
// or after Shutdown it returns nil, so the observability component skips it
// (lazy pickup). Counters are cumulative for the process lifetime.
func (c *CFValkeyJobs) Metrics() []cf_observability.Metric {
	if c.Client() == nil {
		return nil
	}
	labels := map[string]string{"component": c.Name()}
	ms := []cf_observability.Metric{
		{
			Name:   "valkey_jobs_info",
			Help:   "Valkey jobs component descriptor; 1 while initialized.",
			Value:  1,
			Labels: chassis.CopyLabels(labels),
		},
		{
			Name:   "valkey_jobs_config_reloads_total",
			Help:   "Total number of successful worker-tunable reloads.",
			Value:  float64(c.reloads.Load()),
			Labels: chassis.CopyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_jobs_ready",
			Help:   "Jobs waiting in the ready ZSET.",
			Value:  c.zcard(c.readyKey()),
			Labels: chassis.CopyLabels(labels),
			Type:   cf_observability.MetricTypeGauge,
		},
		{
			Name:   "valkey_jobs_inflight",
			Help:   "Jobs in the inflight ZSET.",
			Value:  c.zcard(c.inflightKey()),
			Labels: chassis.CopyLabels(labels),
			Type:   cf_observability.MetricTypeGauge,
		},
		{
			Name:   "valkey_jobs_dead",
			Help:   "Jobs in the dead-letter ZSET.",
			Value:  c.zcard(c.deadKey()),
			Labels: chassis.CopyLabels(labels),
			Type:   cf_observability.MetricTypeGauge,
		},
	}

	c.metersMu.Lock()
	types := make([]string, 0, len(c.meters))
	for t := range c.meters {
		types = append(types, t)
	}
	sort.Strings(types)
	ms = append(ms, c.metricRows(types)...)
	c.metersMu.Unlock()
	return ms
}

func (c *CFValkeyJobs) metricRows(types []string) []cf_observability.Metric {
	var ms []cf_observability.Metric
	for _, t := range types {
		m := c.meters[t]
		lbl := map[string]string{"component": c.Name(), "type": t}
		ms = append(ms,
			cf_observability.Metric{Name: "valkey_jobs_enqueued_total", Help: "Total number of jobs enqueued.", Value: float64(m.enqueued.Load()), Labels: chassis.CopyLabels(lbl), Type: cf_observability.MetricTypeCounter},
			cf_observability.Metric{Name: "valkey_jobs_run_total", Help: "Total number of handler runs.", Value: float64(m.run.Load()), Labels: chassis.CopyLabels(lbl), Type: cf_observability.MetricTypeCounter},
			cf_observability.Metric{Name: "valkey_jobs_failed_total", Help: "Total number of handler failures.", Value: float64(m.failed.Load()), Labels: chassis.CopyLabels(lbl), Type: cf_observability.MetricTypeCounter},
			cf_observability.Metric{Name: "valkey_jobs_requeued_total", Help: "Total number of requeues (retries and recovery).", Value: float64(m.requeued.Load()), Labels: chassis.CopyLabels(lbl), Type: cf_observability.MetricTypeCounter},
			cf_observability.Metric{Name: "valkey_jobs_dead_total", Help: "Total number of dead-lettered jobs.", Value: float64(m.dead.Load()), Labels: chassis.CopyLabels(lbl), Type: cf_observability.MetricTypeCounter},
			cf_observability.Metric{Name: "valkey_jobs_duration_seconds_sum", Help: "Total handler duration.", Value: float64(m.durSumNs.Load()) / 1e9, Labels: chassis.CopyLabels(lbl), Type: cf_observability.MetricTypeCounter},
			cf_observability.Metric{Name: "valkey_jobs_duration_seconds_count", Help: "Number of handler duration samples.", Value: float64(m.durCount.Load()), Labels: chassis.CopyLabels(lbl), Type: cf_observability.MetricTypeCounter},
		)
	}
	return ms
}

func (c *CFValkeyJobs) zcard(key string) float64 {
	client, err := c.client()
	if err != nil {
		return 0
	}
	resp := client.Do(context.Background(), client.B().Zcard().Key(key).Build())
	n, err := resp.AsInt64()
	if err != nil {
		return 0
	}
	return float64(n)
}

// parseClaim decodes a claim result: {id, attempts, type, payload,
// max_attempts, created_ms, retry_start_ms, jitter_base}.
func parseClaim(fields []string) (Job, int64, int64, error) {
	if len(fields) < 8 {
		return Job{}, 0, 0, errors.New("cf_valkey_jobs: claim result too short")
	}
	attempts, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return Job{}, 0, 0, err
	}
	maxA, err := strconv.ParseInt(fields[4], 10, 64)
	if err != nil {
		return Job{}, 0, 0, err
	}
	createdMs, err := strconv.ParseInt(fields[5], 10, 64)
	if err != nil {
		return Job{}, 0, 0, err
	}
	retryStart, _ := strconv.ParseInt(fields[6], 10, 64)
	jitterBase, _ := strconv.ParseInt(fields[7], 10, 64)
	return Job{
		ID:          fields[0],
		Type:        fields[2],
		Payload:     []byte(fields[3]),
		Attempts:    attempts,
		MaxAttempts: maxA,
		CreatedAt:   time.UnixMilli(createdMs),
	}, retryStart, jitterBase, nil
}

// validateJobID rejects ids that share a Redis key with the ready / inflight /
// dead ZSETs or the WithRepeat lock prefix (jobs:cron:…). Colon is refused so
// a caller cannot walk into those names as extra Key() segments.
func validateJobID(id string) error {
	if id == "" || strings.Contains(id, ":") {
		return ErrInvalidJobID
	}
	switch id {
	case zReady, zInflight, zDead, "cron":
		return ErrInvalidJobID
	}
	return nil
}

// nxAcquired is true only when SET NX created the key. A lost NX is a nil
// bulk (valkey.Nil), not a transport error — enqueue must not run then.
func nxAcquired(resp valkey.ValkeyResult) (bool, error) {
	if err := resp.Error(); err != nil {
		if errors.Is(err, valkey.Nil) {
			return false, nil
		}
		return false, err
	}
	s, err := resp.ToString()
	if err != nil {
		if errors.Is(err, valkey.Nil) {
			return false, nil
		}
		return false, err
	}
	return s == "OK", nil
}

// newID returns a 128-bit random hex id.
func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := cryptorand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// Lua scripts. The ready ZSET is scored by due-ms; the inflight ZSET by
// visibility-deadline-ms; the dead ZSET by dead-lettered-at-ms. A job's
// payload hash is jobs:<id>.

// luaEnqueue atomically stores the payload and schedules it.
// KEYS: [1]=job hash, [2]=ready. ARGV: type, payload, max_attempts,
// created_ms, retention_ms, due_ms, id, visibility_ms.
const luaEnqueue = `if redis.call("EXISTS", KEYS[1]) == 1 then
  return 0
end
redis.call("HSET", KEYS[1], "type", ARGV[1], "payload", ARGV[2],
  "max_attempts", ARGV[3], "created_ms", ARGV[4],
  "attempts", "0", "retry_start_ms", "0", "jitter_base", "0",
  "visibility_ms", ARGV[8])
redis.call("PEXPIRE", KEYS[1], ARGV[5])
redis.call("ZADD", KEYS[2], ARGV[6], ARGV[7])
return 1`

// luaClaim atomically moves a due job into inflight, bumping its attempt count,
// and sets its visibility deadline from the job's own visibility_ms. Returns
// nil when another worker already claimed it.
// KEYS: [1]=job hash, [2]=ready, [3]=inflight. ARGV: id, now_ms.
const luaClaim = `if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return nil
end
local v = tonumber(redis.call("HGET", KEYS[1], "visibility_ms"))
if v == nil or v <= 0 then
  v = 60000
end
redis.call("ZADD", KEYS[3], tonumber(ARGV[2]) + v, ARGV[1])
redis.call("HINCRBY", KEYS[1], "attempts", 1)
return {ARGV[1],
  redis.call("HGET", KEYS[1], "attempts"),
  redis.call("HGET", KEYS[1], "type"),
  redis.call("HGET", KEYS[1], "payload"),
  redis.call("HGET", KEYS[1], "max_attempts"),
  redis.call("HGET", KEYS[1], "created_ms"),
  redis.call("HGET", KEYS[1], "retry_start_ms"),
  redis.call("HGET", KEYS[1], "jitter_base")}`

// luaAck removes a finished job.
// KEYS: [1]=inflight, [2]=dead, [3]=job hash. ARGV: id.
const luaAck = `redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("DEL", KEYS[3])
return 1`

// luaDead dead-letters a job without retrying (no handler, fatal outcome).
// KEYS: [1]=inflight, [2]=dead, [3]=job hash. ARGV: id, dead_at_ms,
// retention_ms.
const luaDead = `if redis.call("ZREM", KEYS[1], ARGV[1]) == 0 then
  return -1
end
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
redis.call("PEXPIRE", KEYS[3], ARGV[3])
return 0`

// luaRelease moves a claimed job to ready (retry) or dead, returning
// {status, type}: 1 = requeued, 0 = dead-lettered, -1 = not inflight.
// bump (ARGV[6]) counts the aborting run as an attempt (true for reap).
// ARGV[7]/[8] persist the retry epoch and jitter base (0 = unchanged).
// KEYS: [1]=inflight, [2]=job hash, [3]=ready, [4]=dead.
// ARGV: id, due_ms, retention_ms, dead_at_ms, dead_retention_ms, bump,
// retry_start_ms, jitter_base.
const luaRelease = `if redis.call("ZREM", KEYS[1], ARGV[1]) == 0 then
  return {tostring(-1), ""}
end
if ARGV[6] == "1" then
  redis.call("HINCRBY", KEYS[2], "attempts", 1)
end
if ARGV[7] ~= "0" then
  redis.call("HSET", KEYS[2], "retry_start_ms", ARGV[7])
end
if ARGV[8] ~= "0" then
  redis.call("HSET", KEYS[2], "jitter_base", ARGV[8])
end
local t = redis.call("HGET", KEYS[2], "type")
local a = tonumber(redis.call("HGET", KEYS[2], "attempts"))
local m = tonumber(redis.call("HGET", KEYS[2], "max_attempts"))
if a < m then
  redis.call("ZADD", KEYS[3], ARGV[2], ARGV[1])
  redis.call("PEXPIRE", KEYS[2], ARGV[3])
  return {tostring(1), t}
end
redis.call("ZADD", KEYS[4], ARGV[4], ARGV[1])
redis.call("PEXPIRE", KEYS[2], ARGV[5])
return {tostring(0), t}`

// luaReplay moves a dead job back to ready with the same id. Attempts and
// retry bookkeeping are cleared so the next claim is a fresh life.
// KEYS: [1]=dead, [2]=ready, [3]=job hash. ARGV: id, due_ms.
// Returns 1 ok, -1 not in dead, -2 hash missing.
const luaReplay = `if redis.call("ZREM", KEYS[1], ARGV[1]) == 0 then
  return -1
end
if redis.call("EXISTS", KEYS[3]) == 0 then
  return -2
end
redis.call("HSET", KEYS[3], "attempts", "0", "retry_start_ms", "0", "jitter_base", "0")
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
return 1`

// luaPurgeDead removes one dead member and its hash. Does not touch ready or
// inflight. KEYS: [1]=dead, [2]=job hash. ARGV: id. Returns 1 or -1.
const luaPurgeDead = `if redis.call("ZREM", KEYS[1], ARGV[1]) == 0 then
  return -1
end
redis.call("DEL", KEYS[2])
return 1`

// Compile-time interface assertions.
var (
	_ cf.CaerusComponent               = (*CFValkeyJobs)(nil)
	_ cf.Dependencies                  = (*CFValkeyJobs)(nil)
	_ cf.HealthProvider                = (*CFValkeyJobs)(nil)
	_ cf_observability.MetricsProvider = (*CFValkeyJobs)(nil)
	_ cf.ConfigReloader                = (*CFValkeyJobs)(nil)
	_ cf.Runnable                      = (*CFValkeyJobs)(nil)
	_ cf.ConfigSourceRegistrar         = (*CFValkeyJobs)(nil)
)
