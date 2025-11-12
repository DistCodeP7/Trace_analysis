package main

import (
	"reflect"
	"testing"
)

func TestBuildCausalGraph_TransitiveReduction(t *testing.T) {
	// Processes A and B
	procs := []string{"A", "B"}

	// Construct vector clocks for a chain e0 -> e1 -> e2
	// e0: A increments to 1
	vc0 := NewVectorClock(procs)
	vc0["A"] = 1
	vc0["B"] = 0

	// e1: receives message from e0, B increments to 1 and merges A:1
	vc1 := NewVectorClock(procs)
	vc1["A"] = 1
	vc1["B"] = 1

	// e2: local event on A after merging e1, A increments to 2, B stays 1
	vc2 := NewVectorClock(procs)
	vc2["A"] = 2
	vc2["B"] = 1

	trace := Trace{
		&Event{ID: 0, Type: EventSend, Process: "A", MessageID: 0, VClock: vc0},
		&Event{ID: 1, Type: EventReceive, Process: "B", MessageID: 0, VClock: vc1},
		&Event{ID: 2, Type: EventSend, Process: "A", MessageID: 1, VClock: vc2},
	}

	g := BuildCausalGraph(trace)
	g.ReduceTransitive()

	// After transitive reduction we expect edges: 0->1 and 1->2 (0->2 removed)
	expected := map[int][]int{
		0: {1},
		1: {2},
		2: {},
	}

	if len(g.Edges) != len(expected) {
		t.Fatalf("unexpected number of nodes in edges map: got %d want %d", len(g.Edges), len(expected))
	}

	for k, want := range expected {
		got, ok := g.Edges[k]
		if !ok {
			t.Fatalf("missing edges entry for node %d", k)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("edges for node %d: got %v want %v", k, got, want)
		}
	}
}

func TestBuildCausalGraph_ConcurrentEventsNoEdges(t *testing.T) {
	// Two concurrent sends on different processes: no happens-before relation
	procs := []string{"A", "B"}

	vc0 := NewVectorClock(procs)
	vc0["A"] = 1
	vc0["B"] = 0

	vc1 := NewVectorClock(procs)
	vc1["A"] = 0
	vc1["B"] = 1

	trace := Trace{
		&Event{ID: 0, Type: EventSend, Process: "A", MessageID: 0, VClock: vc0},
		&Event{ID: 1, Type: EventSend, Process: "B", MessageID: 1, VClock: vc1},
	}

	g := BuildCausalGraph(trace)

	// Expect no edges between events; each entry should exist with empty slice
	expected := map[int][]int{
		0: {},
		1: {},
	}

	if len(g.Edges) != len(expected) {
		t.Fatalf("unexpected number of nodes in edges map: got %d want %d", len(g.Edges), len(expected))
	}

	for k, want := range expected {
		got, ok := g.Edges[k]
		if !ok {
			t.Fatalf("missing edges entry for node %d", k)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("edges for node %d: got %v want %v", k, got, want)
		}
	}
}

func TestCausalGraph_TransitiveReductionAutomatic(t *testing.T) {
	// Hardcoded trace with a natural transitive edge: 0->2 via 1
	trace := Trace{
		&Event{ID: 0, Type: EventSend, Process: "A", MessageID: 0, VClock: VectorClock{"A": 1, "B": 0}},
		&Event{ID: 1, Type: EventReceive, Process: "B", MessageID: 0, VClock: VectorClock{"A": 1, "B": 1}},
		&Event{ID: 2, Type: EventSend, Process: "B", MessageID: 1, VClock: VectorClock{"A": 1, "B": 2}},
	}

	g := BuildCausalGraph(trace)

	// Count edges before reduction
	totalBefore := 0
	for _, es := range g.Edges {
		totalBefore += len(es)
	}

	g.ReduceTransitive()

	// Count edges after reduction
	totalAfter := 0
	for _, es := range g.Edges {
		totalAfter += len(es)
	}

	if totalAfter >= totalBefore {
		t.Fatalf("expected some edges to be removed after transitive reduction, got before=%d after=%d", totalBefore, totalAfter)
	}

	// Minimal DAG: only direct edges remain
	expected := map[int][]int{
		0: {1}, // 0->2 removed
		1: {2},
		2: {},
	}

	for id, want := range expected {
		got := g.Edges[id]
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("node %d: got %v want %v", id, got, want)
		}
	}
}

func TestCausalGraph_TransitiveReductionLarger(t *testing.T) {

	trace := Trace{
		&Event{ID: 0, Type: EventSend, Process: "A", MessageID: 0, VClock: VectorClock{"A": 1, "B": 0, "C": 0}},
		&Event{ID: 1, Type: EventReceive, Process: "B", MessageID: 0, VClock: VectorClock{"A": 1, "B": 1, "C": 0}},
		&Event{ID: 2, Type: EventSend, Process: "B", MessageID: 1, VClock: VectorClock{"A": 1, "B": 2, "C": 0}},
		&Event{ID: 3, Type: EventSend, Process: "A", MessageID: 2, VClock: VectorClock{"A": 2, "B": 0, "C": 0}},
		&Event{ID: 4, Type: EventReceive, Process: "C", MessageID: 2, VClock: VectorClock{"A": 2, "B": 0, "C": 1}},
		&Event{ID: 5, Type: EventSend, Process: "C", MessageID: 3, VClock: VectorClock{"A": 2, "B": 0, "C": 2}},
		&Event{ID: 6, Type: EventReceive, Process: "C", MessageID: 1, VClock: VectorClock{"A": 2, "B": 2, "C": 3}},
	}

	g := BuildCausalGraph(trace)

	// Count edges before reduction
	totalBefore := 0
	for _, es := range g.Edges {
		totalBefore += len(es)
	}

	g.ReduceTransitive()
	g.WriteDOT("dot")
	// Count edges after reduction
	totalAfter := 0
	for _, es := range g.Edges {
		totalAfter += len(es)
	}

	if totalAfter >= totalBefore {
		t.Fatalf("expected some edges to be removed after transitive reduction, got before=%d after=%d", totalBefore, totalAfter)
	}

	// Minimal DAG: only direct edges remain
	expected := map[int][]int{
		0: {1, 3}, // 0->1 and 0->3 are direct
		1: {2},    // 1->2 direct
		2: {6},    // B local last event
		3: {4},    // 3->4 direct
		4: {5},    // 4->5 direct
		5: {6},    // C local last event
		6: {},
	}

	for id, want := range expected {
		got := g.Edges[id]
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("node %d: got %v want %v", id, got, want)
		}
	}
}
