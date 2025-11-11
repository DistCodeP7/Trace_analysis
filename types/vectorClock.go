package types

import (
	"fmt"
	"maps"
	"sort"
	"strings"
)

type VectorClock map[string]int

// NewVectorClock initializes a VectorClock with 0 for each process
func NewVectorClock(processes []string) VectorClock {
	vc := make(VectorClock)
	for _, key := range processes {
		vc[key] = 0
	}
	return vc
}

// Creates a deep copy of the given VectorClock
func DeepCopy(vc VectorClock) VectorClock {
	newVC := make(VectorClock, len(vc))
	maps.Copy(newVC, vc)
	return newVC
}

// String returns a string representation of the VectorClock
func (vc VectorClock) String() string {
	keys := make([]string, 0, len(vc))
	for k := range vc {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, vc[k]))
	}
	return "<" + strings.Join(parts, ", ") + ">"
}

// Returns true if vc happens-before other.
// 1. Checks that vc <= other and that
// 2. At least one entry is strictly less.
func (vc VectorClock) HappensBefore(other VectorClock) bool {
	lessOrEqual := true
	strictlyLess := false

	for p := range vc {
		if vc[p] > other[p] {
			lessOrEqual = false
			break
		}
		if vc[p] < other[p] {
			strictlyLess = true
		}
	}

	return lessOrEqual && strictlyLess
}
