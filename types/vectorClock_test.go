package types

import "testing"

func TestVectorClock_HappensBefore(t *testing.T) {
	tests := []struct {
		name  string
		vc    VectorClock
		other VectorClock
		want  bool
	}{
		{
			name:  "equal clocks -> not happens before",
			vc:    VectorClock{"A": 1, "B": 2},
			other: VectorClock{"A": 1, "B": 2},
			want:  false,
		},
		{
			name:  "strictly less on one entry -> happens before",
			vc:    VectorClock{"A": 1, "B": 1},
			other: VectorClock{"A": 2, "B": 1},
			want:  true,
		},
		{
			name:  "greater on one entry -> not happens before",
			vc:    VectorClock{"A": 2, "B": 1},
			other: VectorClock{"A": 1, "B": 1},
			want:  false,
		},
		{
			name:  "concurrent clocks -> not happens before",
			vc:    VectorClock{"A": 2, "B": 1},
			other: VectorClock{"A": 1, "B": 2},
			want:  false,
		},
		{
			name:  "other has extra keys but no strictly less -> not happens before",
			vc:    VectorClock{"A": 1},
			other: VectorClock{"A": 1, "B": 100},
			want:  false,
		},
		{
			name:  "other has extra keys and vc strictly less on shared key -> happens before",
			vc:    VectorClock{"A": 1},
			other: VectorClock{"A": 2, "B": 0},
			want:  true,
		},
		{
			name:  "vc has key not present in other (treated as zero) and greater -> not happens before",
			vc:    VectorClock{"A": 1, "C": 1},
			other: VectorClock{"A": 1}, // other["C"] == 0
			want:  false,
		},
		{
			name:  "vc has key not present in other but less on another -> not happens before because C>0",
			vc:    VectorClock{"A": 0, "C": 1},
			other: VectorClock{"A": 1}, // other["C"] == 0 so vc["C"]>other["C"] -> not lessOrEqual
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.vc.HappensBefore(tt.other)
			if got != tt.want {
				t.Fatalf("HappensBefore(%v, %v) = %v; want %v", tt.vc, tt.other, got, tt.want)
			}
		})
	}
}
