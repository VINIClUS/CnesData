package manifest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubtiposRawPorFonteSeguemOrdemCongelada(t *testing.T) {
	require.Equal(t, []string{"CNES_VINCULO"}, RawSubtypes(SourceTypeCNESLocal))
	require.Equal(t, []string{"SIHD_INTERNACAO", "SIHD_PROC_AIH"}, RawSubtypes(SourceTypeSIHD))
	require.Equal(t, []string{"BPA_C", "BPA_I"}, RawSubtypes(SourceTypeBPAMag))
	require.Equal(t, []string{"SIA_APA", "SIA_BPI", "SIA_BPIHST", "DIM_SIGTAP", "DIM_MUNICIPIO"},
		RawSubtypes(SourceTypeSIALocal))
	require.Empty(t, RawSubtypes(SourceTypeCNESNacional))
	require.Empty(t, RawSubtypes("OUTRA"))
}

func TestSubtiposRawRetornamCopiaIndependente(t *testing.T) {
	subtypes := RawSubtypes(SourceTypeSIHD)
	subtypes[0] = "ALTERADO"
	require.Equal(t, "SIHD_INTERNACAO", RawSubtypes(SourceTypeSIHD)[0])
}

func TestParRawValidoAceitaSoAMatrizDoEdge(t *testing.T) {
	for source, subtype := range map[SourceType]string{
		SourceTypeCNESLocal: "CNES_VINCULO", SourceTypeSIHD: "SIHD_PROC_AIH",
		SourceTypeBPAMag: "BPA_I", SourceTypeSIALocal: "DIM_MUNICIPIO",
	} {
		require.True(t, ValidRawPair(source, subtype), "%s/%s", source, subtype)
	}
	require.False(t, ValidRawPair(SourceTypeSIHD, "BPA_C"))
	require.False(t, ValidRawPair(SourceTypeCNESNacional, "CNES_VINCULO"))
	require.False(t, ValidRawPair(SourceTypeSIALocal, ""))
}
