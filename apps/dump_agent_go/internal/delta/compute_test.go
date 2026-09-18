package delta

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompute_ColdStartAllInsert(t *testing.T) {
	prof := ProfileFor("cnes", "estabelecimentos")
	committed := map[string][32]byte{}
	current := []Row{
		{"CNES": "1", "NOME_FANTA": "A"},
		{"CNES": "2", "NOME_FANTA": "B"},
	}
	hashes := map[string][32]byte{
		"1": Hash(current[0], prof.FingerprintColumns),
		"2": Hash(current[1], prof.FingerprintColumns),
	}
	ds := Compute(committed, hashes, current, prof)
	require.Len(t, ds.Inserts, 2)
	require.Empty(t, ds.Updates)
	require.Empty(t, ds.Deletes)
}

func TestCompute_NoChanges(t *testing.T) {
	prof := ProfileFor("cnes", "estabelecimentos")
	current := []Row{{"CNES": "1", "NOME_FANTA": "A"}}
	h := Hash(current[0], prof.FingerprintColumns)
	committed := map[string][32]byte{"1": h}
	hashes := map[string][32]byte{"1": h}
	ds := Compute(committed, hashes, current, prof)
	require.Empty(t, ds.Inserts)
	require.Empty(t, ds.Updates)
	require.Empty(t, ds.Deletes)
}

func TestCompute_OneUpdate(t *testing.T) {
	prof := ProfileFor("cnes", "estabelecimentos")
	prior := Row{"CNES": "1", "NOME_FANTA": "OLD"}
	committed := map[string][32]byte{"1": Hash(prior, prof.FingerprintColumns)}
	current := []Row{{"CNES": "1", "NOME_FANTA": "NEW"}}
	hashes := map[string][32]byte{"1": Hash(current[0], prof.FingerprintColumns)}
	ds := Compute(committed, hashes, current, prof)
	require.Empty(t, ds.Inserts)
	require.Len(t, ds.Updates, 1)
	require.Equal(t, "NEW", ds.Updates[0]["NOME_FANTA"])
	require.Empty(t, ds.Deletes)
}

func TestCompute_OneDelete(t *testing.T) {
	prof := ProfileFor("cnes", "estabelecimentos")
	prior := Row{"CNES": "1", "NOME_FANTA": "A"}
	committed := map[string][32]byte{
		"1": Hash(prior, prof.FingerprintColumns),
		"2": [32]byte{},
	}
	current := []Row{{"CNES": "1", "NOME_FANTA": "A"}}
	hashes := map[string][32]byte{"1": Hash(current[0], prof.FingerprintColumns)}
	ds := Compute(committed, hashes, current, prof)
	require.Empty(t, ds.Inserts)
	require.Empty(t, ds.Updates)
	require.Len(t, ds.Deletes, 1)
	require.Equal(t, "2", asString(ds.Deletes[0]["CNES"]))
}

func TestCompute_AllDeletedWhenCurrentEmpty(t *testing.T) {
	prof := ProfileFor("cnes", "estabelecimentos")
	committed := map[string][32]byte{
		"1": [32]byte{}, "2": [32]byte{}, "3": [32]byte{},
	}
	ds := Compute(committed, map[string][32]byte{}, nil, prof)
	require.Empty(t, ds.Inserts)
	require.Empty(t, ds.Updates)
	require.Len(t, ds.Deletes, 3)
}

func TestCompute_MixedIUD(t *testing.T) {
	prof := ProfileFor("cnes", "estabelecimentos")
	committed := map[string][32]byte{
		"keep_same":   Hash(Row{"CNES": "keep_same", "NOME_FANTA": "S"}, prof.FingerprintColumns),
		"will_update": Hash(Row{"CNES": "will_update", "NOME_FANTA": "OLD"}, prof.FingerprintColumns),
		"will_delete": [32]byte{0xff},
	}
	current := []Row{
		{"CNES": "keep_same", "NOME_FANTA": "S"},
		{"CNES": "will_update", "NOME_FANTA": "NEW"},
		{"CNES": "new_one", "NOME_FANTA": "X"},
	}
	hashes := map[string][32]byte{}
	for _, r := range current {
		pk := prof.PKExtractor(r)
		hashes[pk] = Hash(r, prof.FingerprintColumns)
	}
	ds := Compute(committed, hashes, current, prof)
	require.Len(t, ds.Inserts, 1)
	require.Equal(t, "new_one", asString(ds.Inserts[0]["CNES"]))
	require.Len(t, ds.Updates, 1)
	require.Equal(t, "will_update", asString(ds.Updates[0]["CNES"]))
	require.Len(t, ds.Deletes, 1)
	require.Equal(t, "will_delete", asString(ds.Deletes[0]["CNES"]))
}
