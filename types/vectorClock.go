package types

import (
	"fmt"
	"maps"
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
	var parts []string
	for p, v := range vc {
		parts = append(parts, fmt.Sprintf("%s:%d", p, v))
	}
	return "<" + strings.Join(parts, ", ") + ">"
}
