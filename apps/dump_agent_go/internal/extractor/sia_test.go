package extractor

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var siaSubtypes = []string{"SIA_APA", "SIA_BPI", "SIA_BPIHST", "DIM_SIGTAP", "DIM_MUNICIPIO"}

func siaFixtures() string {
	return filepath.Join("..", "..", "test", "integration", "fixtures", "sia_synthetic")
}

func extractFixtures(t *testing.T, subtypes ...string) *SIAResult {
	t.Helper()
	result, err := ExtractSIA(siaFixtures(), "202601", subtypes)
	require.NoError(t, err)
	return result
}

// writeTestDBF grava um DBF mínimo com campos C(10) para cenários de contrato.
func writeTestDBF(t *testing.T, path string, fields []string, rows [][]string) {
	t.Helper()
	var buf bytes.Buffer
	headerLen := uint16(32 + 32*len(fields) + 1)
	recordLen := uint16(1 + 10*len(fields))
	buf.Write([]byte{0x03, 126, 1, 1})
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, uint32(len(rows))))
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, headerLen))
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, recordLen))
	buf.Write(make([]byte, 20))
	for _, name := range fields {
		desc := make([]byte, 32)
		copy(desc, name)
		desc[11], desc[16] = 'C', 10
		buf.Write(desc)
	}
	buf.WriteByte(0x0D)
	for _, row := range rows {
		buf.WriteByte(' ')
		for _, v := range row {
			buf.WriteString((v + strings.Repeat(" ", 10))[:10])
		}
	}
	buf.WriteByte(0x1A)
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o644))
}

func TestSIA_ExtraiTodosOsSubtiposDoLayoutReal(t *testing.T) {
	result := extractFixtures(t, siaSubtypes...)

	require.Len(t, result.APA, 4)
	require.Len(t, result.BPI, 8)
	require.Len(t, result.BPIHST, 12)
	require.Len(t, result.SIGTAP, 4)
	require.Len(t, result.CADMUN, 2)
}

func TestSIA_APAJuntaSPRDComSAPAPorNumeroECompetencia(t *testing.T) {
	apa := extractFixtures(t, "SIA_APA").APA

	byNum := map[string]SIAAPARow{}
	for _, row := range apa {
		byNum[row.Apanum] = row
	}
	require.NotContains(t, byNum, "", "linhas S_PRD sem PRD_APANUM não são APAC")
	first := byNum["3526100000001"]
	require.Equal(t, "2269481", first.Uid)
	require.Equal(t, "0304100021", first.Procedimento)
	require.Equal(t, "C509", first.CidPrincipal)
	require.True(t, strings.HasPrefix(first.DtInicio, "202601"), "cabeçalho da competência do PRD")
	require.Len(t, first.CnsExecutante, 15)
	require.Equal(t, int64(2), *first.QtProduzida)
	require.Equal(t, int64(15025), *first.ValorProduzCents)
	require.Equal(t, int64(15025), *first.ValorAprovCents)
}

func TestSIA_APASemCabecalhoMantemLinhaComCamposVazios(t *testing.T) {
	apa := extractFixtures(t, "SIA_APA").APA

	for _, row := range apa {
		if row.Apanum == "3526100000099" {
			require.Empty(t, row.DtInicio)
			require.Empty(t, row.CnsExecutante)
			return
		}
	}
	t.Fatal("APAC órfã ausente")
}

func TestSIA_NumericoEmBrancoViraNulo(t *testing.T) {
	apa := extractFixtures(t, "SIA_APA").APA

	for _, row := range apa {
		if row.Apanum == "3526100000003" {
			require.Nil(t, row.QtAprovada)
			require.Nil(t, row.ValorAprovCents)
			require.NotNil(t, row.QtProduzida)
			return
		}
	}
	t.Fatal("APAC sem aprovação ausente")
}

func TestSIA_BPIUsaCamposReais(t *testing.T) {
	result := extractFixtures(t, "SIA_BPI", "SIA_BPIHST")

	for _, row := range append(result.BPI, result.BPIHST...) {
		require.Equal(t, "2269481", row.Uid)
		require.Len(t, row.Procedimento, 10)
		require.Equal(t, "001", row.Folha)
		require.Len(t, row.Seq, 2)
		require.Len(t, row.DtAtendimento, 8)
		require.Len(t, row.CnsProfissional, 15)
		require.NotNil(t, row.QtProduzida)
	}
}

func TestSIA_SIGTAPVemDeSPAFiltradoPelaCompetencia(t *testing.T) {
	sigtap := extractFixtures(t, "DIM_SIGTAP").SIGTAP

	for _, row := range sigtap {
		require.Len(t, row.CoProcedimento, 10)
		require.Equal(t, "202601", row.DtCompetencia)
		require.NotEmpty(t, row.NoProcedimento)
		require.Contains(t, []string{"0", "1", "2", "3"}, row.TpComplexidade)
		require.Len(t, row.CoFinanciamento, 2)
	}
}

func TestSIA_AceitaCompetenciaComHifen(t *testing.T) {
	result, err := ExtractSIA(siaFixtures(), "2026-01", []string{"DIM_SIGTAP"})
	require.NoError(t, err)
	require.Len(t, result.SIGTAP, 4)
}

func TestSIA_SIGTAPSemLinhasNaCompetenciaFalha(t *testing.T) {
	_, err := ExtractSIA(siaFixtures(), "209912", []string{"DIM_SIGTAP"})
	require.ErrorContains(t, err, "sia_sigtap_empty competencia=209912")
}

func TestSIA_CADMUNTemCodmunicDeQuatroDigitos(t *testing.T) {
	cadmun := extractFixtures(t, "DIM_MUNICIPIO").CADMUN

	require.Equal(t, "35", cadmun[0].CodUF)
	require.Equal(t, "4130", cadmun[0].CodMun)
	require.InDelta(t, 1234567.89, *cadmun[0].TetoPab, 0.001)
	require.Equal(t, "SÃO PAULO", cadmun[1].Nome)
	require.Nil(t, cadmun[1].TetoPab)
}

func TestSIA_LeSoOsArquivosDosSubtiposPedidos(t *testing.T) {
	tmp := t.TempDir()
	data, err := os.ReadFile(filepath.Join(siaFixtures(), "CADMUN.DBF"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "CADMUN.DBF"), data, 0o644))

	result, err := ExtractSIA(tmp, "202601", []string{"DIM_MUNICIPIO"})
	require.NoError(t, err)
	require.Len(t, result.CADMUN, 2)

	_, err = ExtractSIA(tmp, "202601", []string{"SIA_BPI"})
	require.ErrorContains(t, err, "sia_file_missing file=S_BPI.DBF")
}

func TestSIA_RejeitaLayoutSinteticoAntigo(t *testing.T) {
	tmp := t.TempDir()
	writeTestDBF(t, filepath.Join(tmp, "S_BPI.DBF"),
		[]string{"BPI_CMP", "BPI_CNES", "BPI_PROC", "BPI_QT", "BPI_FOLHA"},
		[][]string{{"202601", "2269481", "0301010056", "1", "1"}})

	_, err := ExtractSIA(tmp, "202601", []string{"SIA_BPI"})
	require.ErrorContains(t, err, "sia_field_missing file=S_BPI.DBF field=BPI_UID")
}

func TestSIA_NumericoInvalidoFalha(t *testing.T) {
	tmp := t.TempDir()
	writeTestDBF(t, filepath.Join(tmp, "CADMUN.DBF"), fieldsCADMUN,
		[][]string{{"35", "4130", "X", "PB", "abc", ""}})

	_, err := ExtractSIA(tmp, "202601", []string{"DIM_MUNICIPIO"})
	require.ErrorContains(t, err, "sia_numeric_invalid file=CADMUN.DBF field=TETOPAB row=0")
}

func TestSIA_SubtipoDesconhecidoFalha(t *testing.T) {
	_, err := ExtractSIA(siaFixtures(), "202601", []string{"BOGUS"})
	require.ErrorContains(t, err, "unknown_sia_subtype=BOGUS")
}

func TestSIA_DiretorioAusenteFalha(t *testing.T) {
	_, err := ExtractSIA(filepath.Join("nonexistent", "path"), "202601", siaSubtypes)
	require.ErrorContains(t, err, "sia_dir_missing")
}

func TestSIA_DBFCorrompidoFalha(t *testing.T) {
	tmp := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "CADMUN.DBF"), []byte("not a dbf"), 0o644))

	_, err := ExtractSIA(tmp, "202601", []string{"DIM_MUNICIPIO"})
	require.ErrorContains(t, err, "dbf_open file=CADMUN.DBF")
}

func TestSIA_Cp1252Sanitize(t *testing.T) {
	got, dirty := SanitizeString("SAO JO\xc3O")
	require.NotEmpty(t, got)
	require.Positive(t, dirty)
}

func TestSIA_BPIHSTAusenteViraSlotZeroRowSemAfrouxarOsDemais(t *testing.T) {
	tmp := t.TempDir()

	result, err := ExtractSIA(tmp, "202601", []string{"SIA_BPIHST"})
	require.NoError(t, err)
	require.NotNil(t, result.BPIHST)
	require.Empty(t, result.BPIHST)

	for subtype, file := range map[string]string{
		"SIA_APA": "S_APA.DBF", "SIA_BPI": "S_BPI.DBF",
		"DIM_SIGTAP": "S_PA.DBF", "DIM_MUNICIPIO": "CADMUN.DBF",
	} {
		_, err = ExtractSIA(tmp, "202601", []string{subtype})
		require.ErrorContains(t, err, "sia_file_missing file="+file)
	}
}

func TestSIA_BPIHSTPresenteComLayoutErradoContinuaFalhando(t *testing.T) {
	tmp := t.TempDir()
	writeTestDBF(t, filepath.Join(tmp, "S_BPIHST.DBF"), []string{"BPI_CMP"}, nil)

	_, err := ExtractSIA(tmp, "202601", []string{"SIA_BPIHST"})
	require.ErrorContains(t, err, "sia_field_missing file=S_BPIHST.DBF")
}
