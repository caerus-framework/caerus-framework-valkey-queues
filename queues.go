// Package queues is an optional parent for Valkey work machines in this
// module (VPQ and jobs). It does not merge them into one Queue type: each
// child keeps its Lua, Name(), and Run loop. The parent only groups
// machines you pass in so one AddComponent / Subcomponents() expands them.
//
// Omitting a machine means it is not registered. There is no “start every
// queue” default.
package queues

import (
	"context"

	cf "github.com/caerus-framework/caerus-framework"
	"github.com/caerus-framework/caerus-framework-valkey-queues/jobs"
	"github.com/caerus-framework/caerus-framework-valkey-queues/vpq"
)

const (
	// ComponentName is the parent’s registry name. Children keep their own
	// names ("vpq", "valkey-jobs", or WithName on the child).
	ComponentName = "valkey-queues"

	// ComponentStage is the app plane: children stay in the data stage and
	// initialize with valkey. The parent is a bag; Init is a no-op.
	ComponentStage = cf.Stage("app")
)

// CFValkeyQueues groups constructed VPQ and/or jobs instances. Construct
// each machine with its own options, then pass it here. The framework
// expands Subcomponents(); this type must not Init/Run/Shutdown children.
type CFValkeyQueues struct {
	name     string
	children []cf.CaerusComponent
}

// Option configures the parent at construction.
type Option func(*CFValkeyQueues)

// WithName sets the parent’s component name (default ComponentName).
func WithName(name string) Option {
	return func(c *CFValkeyQueues) { c.name = name }
}

// WithVPQ adds a priority-queue machine. Nil is ignored. More than one is
// allowed when each child has a distinct Name() (WithName on vpq).
func WithVPQ(q *vpq.PriorityQueue) Option {
	return func(c *CFValkeyQueues) {
		if q != nil {
			c.children = append(c.children, q)
		}
	}
}

// WithJobs adds a delayed-jobs machine. Nil is ignored. Distinct Name()
// per instance, same as WithVPQ.
func WithJobs(j *jobs.CFValkeyJobs) Option {
	return func(c *CFValkeyQueues) {
		if j != nil {
			c.children = append(c.children, j)
		}
	}
}

// New builds a parent. Pass every machine this process should run; do not
// pass a machine you do not want registered.
func New(opts ...Option) *CFValkeyQueues {
	c := &CFValkeyQueues{}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Name implements cf.CaerusComponent.
func (c *CFValkeyQueues) Name() string {
	if c.name != "" {
		return c.name
	}
	return ComponentName
}

// GetInitOrderStage implements cf.CaerusComponent.
func (c *CFValkeyQueues) GetInitOrderStage() cf.Stage { return ComponentStage }

// Init implements cf.CaerusComponent. Children initialize on their own.
func (c *CFValkeyQueues) Init(context.Context, *cf.CaerusFramework) error { return nil }

// Shutdown implements cf.CaerusComponent. Children shut down on their own.
func (c *CFValkeyQueues) Shutdown(context.Context) error { return nil }

// Subcomponents implements cf.Subcomponents. Order is the WithVPQ / WithJobs
// call order.
func (c *CFValkeyQueues) Subcomponents() []cf.CaerusComponent {
	return c.children
}

// VPQ returns the first *vpq.PriorityQueue child, or nil.
func (c *CFValkeyQueues) VPQ() *vpq.PriorityQueue {
	for _, ch := range c.children {
		if q, ok := ch.(*vpq.PriorityQueue); ok {
			return q
		}
	}
	return nil
}

// Jobs returns the first *jobs.CFValkeyJobs child, or nil.
func (c *CFValkeyQueues) Jobs() *jobs.CFValkeyJobs {
	for _, ch := range c.children {
		if j, ok := ch.(*jobs.CFValkeyJobs); ok {
			return j
		}
	}
	return nil
}

var (
	_ cf.CaerusComponent = (*CFValkeyQueues)(nil)
	_ cf.Subcomponents   = (*CFValkeyQueues)(nil)
)
