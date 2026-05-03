package delta

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOp_String(t *testing.T) {
	require.Equal(t, "I", OpInsert.String())
	require.Equal(t, "U", OpUpdate.String())
	require.Equal(t, "D", OpDelete.String())
	require.Equal(t, "?", OpUnknown.String())
}

func TestSourceKey_BucketPath(t *testing.T) {
	k := SourceKey{Source: "cnes", Intent: "profissionais", Competencia: "202605"}
	require.Equal(t, "cnes/profissionais/202605", k.BucketPath())
}

func TestSourceKey_ZeroValueEmpty(t *testing.T) {
	var k SourceKey
	require.Equal(t, "", k.Source)
	require.Equal(t, "", k.Intent)
	require.Equal(t, "", k.Competencia)
}

func TestDeltaSet_TotalCount(t *testing.T) {
	ds := DeltaSet{
		Inserts: []Row{{}, {}},
		Updates: []Row{{}},
		Deletes: []Row{{}, {}, {}},
	}
	require.Equal(t, 6, ds.TotalCount())
}
