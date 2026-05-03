package delta

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSplitPK_SinglePart(t *testing.T) {
	parts := splitPK("CNES1")
	require.Equal(t, []string{"CNES1"}, parts)
}

func TestSplitPK_ThreeParts(t *testing.T) {
	parts := splitPK("CPF1|CNES1|CBO1")
	require.Equal(t, []string{"CPF1", "CNES1", "CBO1"}, parts)
}

func TestSplitPK_Empty(t *testing.T) {
	parts := splitPK("")
	require.Equal(t, []string{""}, parts)
}

func TestPKColumnsForProfile_AllSources(t *testing.T) {
	require.Equal(t, []string{"CNES"}, pkColumnsForProfile("cnes", "estabelecimentos"))
	require.Equal(t, []string{"CPF_PROF", "CNES", "COD_CBO"},
		pkColumnsForProfile("cnes", "profissionais"))
	require.Equal(t, []string{"SEQ_EQUIPE"}, pkColumnsForProfile("cnes", "equipes"))
	require.Equal(t, []string{"NUM_AIH"}, pkColumnsForProfile("sihd", "aih"))
	require.Equal(t, []string{"CPF", "COMPETEN", "COD_PROC"},
		pkColumnsForProfile("bpa", "linhas"))
	require.Nil(t, pkColumnsForProfile("xyz", "abc"))
}

func TestDeletedRow_AllSources(t *testing.T) {
	for _, tc := range []struct {
		source, intent, pk string
		expectedKey        string
		expectedVal        string
	}{
		{"cnes", "estabelecimentos", "CNES1", "CNES", "CNES1"},
		{"cnes", "equipes", "EQ01", "SEQ_EQUIPE", "EQ01"},
		{"sihd", "aih", "AIH9", "NUM_AIH", "AIH9"},
	} {
		prof := ProfileFor(tc.source, tc.intent)
		row := deletedRow(tc.pk, prof)
		require.Equal(t, tc.expectedVal, row[tc.expectedKey],
			"%s/%s", tc.source, tc.intent)
	}
}

func TestDeletedRow_CompositePKBPA(t *testing.T) {
	prof := ProfileFor("bpa", "linhas")
	row := deletedRow("111|202605|0301010029", prof)
	require.Equal(t, "111", row["CPF"])
	require.Equal(t, "202605", row["COMPETEN"])
	require.Equal(t, "0301010029", row["COD_PROC"])
}

func TestDeletedRow_UnknownProfileReturnsEmpty(t *testing.T) {
	prof := Profile{Source: "xyz", Intent: "abc"}
	row := deletedRow("anything", prof)
	require.Empty(t, row)
}

func TestCnesEquipesProfile_PKExtractor_Short(t *testing.T) {
	p := ProfileFor("cnes", "equipes")
	require.Equal(t, "AB", p.PKExtractor(Row{"SEQ_EQUIPE": "AB"}))
}

func TestCnesEquipesProfile_PKExtractor_Long(t *testing.T) {
	p := ProfileFor("cnes", "equipes")
	require.Equal(t, "ABCD", p.PKExtractor(Row{"SEQ_EQUIPE": "ABCDEFGH"}))
}

func TestSihdAIHProfile_PKExtractor(t *testing.T) {
	p := ProfileFor("sihd", "aih")
	require.Equal(t, "999", p.PKExtractor(Row{"NUM_AIH": "999"}))
}

func TestAsString_NumericTypes(t *testing.T) {
	require.Equal(t, "42", asString(42))
	require.Equal(t, "42", asString(int64(42)))
	require.Equal(t, "3.14", asString(3.14))
	require.Equal(t, "true", asString(true))
}

func TestCanonicalString_AllTypes(t *testing.T) {
	for _, tc := range []struct {
		in       any
		expected string
	}{
		{int(42), "42"},
		{int32(42), "42"},
		{int64(42), "42"},
		{float32(3.5), "3.5"},
		{float64(3.5), "3.5"},
		{true, "true"},
		{false, "false"},
		{[]byte{0xAB, 0xCD}, "abcd"},
		{"plain", "plain"},
	} {
		require.Equal(t, tc.expected, canonicalString(tc.in),
			"input %T = %v", tc.in, tc.in)
	}
}

func TestCanonicalString_TimeUTC(t *testing.T) {
	tm := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	got := canonicalString(tm)
	require.Equal(t, "2026-05-03T12:00:00Z", got)
}

func TestCanonicalString_Struct(t *testing.T) {
	type x struct {
		A int    `json:"a"`
		B string `json:"b"`
	}
	got := canonicalString(x{A: 1, B: "y"})
	require.Equal(t, `{"a":1,"b":"y"}`, got)
}

func TestCanonicalString_NilReturnsToken(t *testing.T) {
	require.Equal(t, nullToken, canonicalString(nil))
}
