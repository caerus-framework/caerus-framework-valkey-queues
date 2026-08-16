package queues

import (
	"testing"

	cf "github.com/caerus-framework/caerus-framework"
	"github.com/caerus-framework/caerus-framework-valkey-queues/jobs"
	"github.com/caerus-framework/caerus-framework-valkey-queues/vpq"
)

func TestNewDefaults(t *testing.T) {
	q := New()
	if q.Name() != ComponentName {
		t.Fatalf("Name() = %q, want %q", q.Name(), ComponentName)
	}
	if q.GetInitOrderStage() != ComponentStage {
		t.Fatalf("GetInitOrderStage() = %q, want %q", q.GetInitOrderStage(), ComponentStage)
	}
	if len(q.Subcomponents()) != 0 {
		t.Fatalf("empty New should have no children, got %d", len(q.Subcomponents()))
	}
	if q.VPQ() != nil || q.Jobs() != nil {
		t.Fatal("empty parent should not expose machines")
	}
}

func TestWithName(t *testing.T) {
	q := New(WithName("work"))
	if q.Name() != "work" {
		t.Fatalf("Name() = %q, want work", q.Name())
	}
}

func TestWithNilMachinesIgnored(t *testing.T) {
	q := New(WithVPQ(nil), WithJobs(nil))
	if len(q.Subcomponents()) != 0 {
		t.Fatal("nil machines must not become children")
	}
}

func TestSubcomponentsOrderAndAccessors(t *testing.T) {
	pq := vpq.New(vpq.WithQueueName("interest"))
	jb := jobs.New()
	q := New(WithJobs(jb), WithVPQ(pq))
	kids := q.Subcomponents()
	if len(kids) != 2 || kids[0] != jb || kids[1] != pq {
		t.Fatalf("Subcomponents order = %v, want jobs then vpq", kids)
	}
	if q.VPQ() != pq {
		t.Fatal("VPQ() should return the priority-queue child")
	}
	if q.Jobs() != jb {
		t.Fatal("Jobs() should return the jobs child")
	}
}

func TestAddComponentExpandsChildren(t *testing.T) {
	fw := cf.New()
	pq := vpq.New(vpq.WithQueueName("interest"), vpq.WithName("interest"))
	jb := jobs.New()
	parent := New(WithVPQ(pq), WithJobs(jb))
	if err := fw.AddComponent(parent); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
	if _, ok := cf.GetByName[*CFValkeyQueues](fw, ComponentName); !ok {
		t.Fatal("parent not registered")
	}
	if _, ok := cf.GetByName[*vpq.PriorityQueue](fw, "interest"); !ok {
		t.Fatal("vpq child not registered")
	}
	if _, ok := cf.GetByName[*jobs.CFValkeyJobs](fw, jobs.ComponentName); !ok {
		t.Fatal("jobs child not registered")
	}
}

func TestAddComponentOmitsUnpassedMachine(t *testing.T) {
	fw := cf.New()
	pq := vpq.New(vpq.WithQueueName("interest"))
	if err := fw.AddComponent(New(WithVPQ(pq))); err != nil {
		t.Fatalf("AddComponent: %v", err)
	}
	if _, ok := cf.Get[*jobs.CFValkeyJobs](fw); ok {
		t.Fatal("jobs must not be registered when WithJobs was omitted")
	}
	if _, ok := cf.Get[*vpq.PriorityQueue](fw); !ok {
		t.Fatal("vpq child missing")
	}
}
