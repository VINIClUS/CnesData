package obs

import (
	"testing"
	"time"
)

func fixedRand(v float64) func() float64 {
	return func() float64 { return v }
}

func TestJitterAround_BoundsAtRandZero(t *testing.T) {
	got := JitterAround(30*time.Second, 0.20, fixedRand(0.0))
	want := 24 * time.Second
	if got != want {
		t.Errorf("rand=0.0 got %v want %v", got, want)
	}
}

func TestJitterAround_BoundsAtRandOne(t *testing.T) {
	got := JitterAround(30*time.Second, 0.20, fixedRand(1.0))
	want := 36 * time.Second
	if got != want {
		t.Errorf("rand=1.0 got %v want %v", got, want)
	}
}

func TestJitterAround_MidpointIdentity(t *testing.T) {
	got := JitterAround(30*time.Second, 0.20, fixedRand(0.5))
	want := 30 * time.Second
	if got != want {
		t.Errorf("rand=0.5 got %v want %v", got, want)
	}
}

func TestJitterAround_ZeroFractionIdentity(t *testing.T) {
	for _, r := range []float64{0.0, 0.25, 0.5, 0.75, 1.0} {
		got := JitterAround(30*time.Second, 0.0, fixedRand(r))
		if got != 30*time.Second {
			t.Errorf("rand=%v fraction=0 got %v want 30s", r, got)
		}
	}
}

func TestJitterAround_FloorAt1ms(t *testing.T) {
	got := JitterAround(1*time.Millisecond, 1.0, fixedRand(0.0))
	if got < 1*time.Millisecond {
		t.Errorf("got %v want >= 1ms", got)
	}
}

func TestJitterAround_PanicsOnNegativeD(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for d=-1s")
		}
	}()
	JitterAround(-1*time.Second, 0.2, fixedRand(0.5))
}

func TestJitterAround_PanicsOnInvalidFraction(t *testing.T) {
	for _, f := range []float64{-0.1, 1.1} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic for fraction=%v", f)
				}
			}()
			JitterAround(30*time.Second, f, fixedRand(0.5))
		}()
	}
}

func TestJitterAround_NilRandUsesDefault(t *testing.T) {
	got := JitterAround(30*time.Second, 0.20, nil)
	min, max := 24*time.Second, 36*time.Second
	if got < min || got > max {
		t.Errorf("nil rand got %v want in [%v, %v]", got, min, max)
	}
}

func TestJitterAround_RandBelowZeroClampedToZero(t *testing.T) {
	got := JitterAround(30*time.Second, 0.20, fixedRand(-0.1))
	want := 24 * time.Second
	if got != want {
		t.Errorf("rand=-0.1 (clamped to 0) got %v want %v", got, want)
	}
}

func TestJitterAround_RandAboveOneClampedToOne(t *testing.T) {
	got := JitterAround(30*time.Second, 0.20, fixedRand(1.1))
	want := 36 * time.Second
	if got != want {
		t.Errorf("rand=1.1 (clamped to 1) got %v want %v", got, want)
	}
}
