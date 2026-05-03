package delta

import (
	"encoding/hex"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHash_Deterministic(t *testing.T) {
	r := Row{"A": "1", "B": 2, "C": "x"}
	fields := []string{"A", "B", "C"}
	h1 := Hash(r, fields)
	h2 := Hash(r, fields)
	require.Equal(t, h1, h2)
}

func TestHash_FieldOrderIndependent(t *testing.T) {
	r := Row{"A": "1", "B": 2, "C": "x"}
	expected := Hash(r, []string{"A", "B", "C"})
	for i := 0; i < 100; i++ {
		fields := []string{"A", "B", "C"}
		rng := rand.New(rand.NewPCG(uint64(i), 0))
		rng.Shuffle(len(fields), func(a, b int) {
			fields[a], fields[b] = fields[b], fields[a]
		})
		got := Hash(r, fields)
		require.Equal(t, expected, got,
			"shuffle iter %d: order %v produced different hash", i, fields)
	}
}

func TestHash_DifferentValuesDifferent(t *testing.T) {
	r1 := Row{"A": "1"}
	r2 := Row{"A": "2"}
	require.NotEqual(t, Hash(r1, []string{"A"}), Hash(r2, []string{"A"}))
}

func TestHash_NilHandled(t *testing.T) {
	r1 := Row{"A": nil}
	r2 := Row{"A": ""}
	require.NotEqual(t, Hash(r1, []string{"A"}), Hash(r2, []string{"A"}),
		"nil must canonicalize differently from empty string")
}

func TestHash_TimeNormalizedUTC(t *testing.T) {
	utc := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	loc := utc.In(time.FixedZone("offset", 3600))
	r1 := Row{"T": utc}
	r2 := Row{"T": loc}
	require.Equal(t, Hash(r1, []string{"T"}), Hash(r2, []string{"T"}),
		"same instant in different zones must hash equal")
}

func TestHash_BytesEncoded(t *testing.T) {
	r := Row{"B": []byte{0x01, 0x02}}
	got := Hash(r, []string{"B"})
	require.NotEmpty(t, got)
}

func TestHashHex_Length64(t *testing.T) {
	r := Row{"A": "1"}
	h := HashHex(r, []string{"A"})
	require.Len(t, h, 64)
	_, err := hex.DecodeString(h)
	require.NoError(t, err)
}
