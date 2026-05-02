package obs

import (
	"math/rand/v2"
	"time"
)

const jitterFloor = 1 * time.Millisecond

// JitterAround returns d perturbed by ±fraction × d × rand-bias.
//
// Formula: out = d + d * (rand()*2 - 1) * fraction
// Range:   [d*(1-fraction), d*(1+fraction)]
// Floor:   1ms (returned even when arithmetic produces ≤ 0)
//
// Panics if d <= 0 or fraction < 0 or fraction > 1.
// randSrc=nil → uses math/rand/v2.Float64.
func JitterAround(d time.Duration, fraction float64, randSrc func() float64) time.Duration {
	if d <= 0 {
		panic("obs.JitterAround: d must be > 0")
	}
	if fraction < 0 || fraction > 1 {
		panic("obs.JitterAround: fraction must be in [0, 1]")
	}
	if randSrc == nil {
		randSrc = rand.Float64
	}
	r := randSrc()
	if r < 0 {
		r = 0
	} else if r > 1 {
		r = 1
	}
	bias := (r*2 - 1) * fraction
	out := d + time.Duration(float64(d)*bias)
	if out < jitterFloor {
		return jitterFloor
	}
	return out
}
