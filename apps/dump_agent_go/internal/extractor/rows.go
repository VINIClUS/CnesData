package extractor

import "github.com/cnesdata/dumpagent/internal/delta"

// ExtractionIntent valores aceitos por ExtractionParams.Intent.
const (
	IntentCnesProfissionais    = "profissionais"
	IntentCnesEstabelecimentos = "estabelecimentos"
	IntentCnesEquipes          = "equipes"
	IntentSihdProducao         = "sihd_producao"
)

// CnesProfissionalRow raw row para intent `profissionais`.
// Tags parquet: nomes snake_case alinhados com consumer Python (data_processor).
type CnesProfissionalRow struct {
	CPFProf           string `parquet:"cpf_prof"`
	CodCNS            string `parquet:"cod_cns"`
	NomeProf          string `parquet:"nome_prof"`
	NoSocial          string `parquet:"no_social"`
	Sexo              string `parquet:"sexo"`
	DataNasc          string `parquet:"data_nasc"`
	CodCBO            string `parquet:"cod_cbo"`
	IndVinc           string `parquet:"ind_vinc"`
	TPSUSNaoSUS       string `parquet:"tp_sus_nao_sus"`
	CargaHorariaTotal int64  `parquet:"carga_horaria_total"`
	CGHoraAmb         int64  `parquet:"cg_horaamb"`
	CGHoraOutr        int64  `parquet:"cghoraoutr"`
	CGHoraHosp        int64  `parquet:"cghorahosp"`
	CNES              string `parquet:"cnes"`
	NomeFanta         string `parquet:"nome_fanta"`
	TPUnidID          string `parquet:"tp_unid_id"`
	CodMunGest        string `parquet:"codmungest"`
}

// CnesEstabelecimentoRow raw row para intent `estabelecimentos`.
type CnesEstabelecimentoRow struct {
	CNES       string `parquet:"cnes"`
	NomeFanta  string `parquet:"nome_fanta"`
	TPUnidID   string `parquet:"tp_unid_id"`
	CodMunGest string `parquet:"codmungest"`
	CNPJMant   string `parquet:"cnpj_mant"`
}

// CnesEquipeRow raw row para intent `equipes`.
type CnesEquipeRow struct {
	SeqEquipe string `parquet:"seq_equipe"`
	INE       string `parquet:"ine"`
	DSArea    string `parquet:"ds_area"`
	TPEquipe  string `parquet:"tp_equipe"`
	CodMun    string `parquet:"cod_mun"`
}

// SihdProducaoRow raw row para intent `sihd_producao`.
// Campos derivam do data-dictionary SIHD — ajustar conforme Spec 1 emitir.
type SihdProducaoRow struct {
	AIHNumero    string `parquet:"aih_numero"`
	Procedimento string `parquet:"procedimento"`
	Competencia  string `parquet:"competencia"`
	CNES         string `parquet:"cnes"`
	Valor        int64  `parquet:"valor"`
}

// ToRow returns canonical column-keyed map for delta fingerprinting.
// Keys mirror delta.ProfileFor("cnes","profissionais").FingerprintColumns
// + Pk fields (CPF_PROF, CNES, COD_CBO).
func (r CnesProfissionalRow) ToRow() delta.Row {
	return delta.Row{
		"CPF_PROF":            r.CPFProf,
		"COD_CNS":             r.CodCNS,
		"NOME_PROF":           r.NomeProf,
		"NO_SOCIAL":           r.NoSocial,
		"SEXO":                r.Sexo,
		"DATA_NASC":           r.DataNasc,
		"COD_CBO":             r.CodCBO,
		"IND_VINC":            r.IndVinc,
		"TP_SUS_NAO_SUS":      r.TPSUSNaoSUS,
		"CARGA_HORARIA_TOTAL": r.CargaHorariaTotal,
		"CG_HORAAMB":          r.CGHoraAmb,
		"CGHORAOUTR":          r.CGHoraOutr,
		"CGHORAHOSP":          r.CGHoraHosp,
		"CNES":                r.CNES,
		"NOME_FANTA":          r.NomeFanta,
		"TP_UNID_ID":          r.TPUnidID,
		"CODMUNGEST":          r.CodMunGest,
	}
}

// ToRow returns canonical column-keyed map for delta fingerprinting.
// Keys mirror delta.ProfileFor("cnes","estabelecimentos") expectations.
// Columns absent from the extractor (BAIRRO, CEP, etc.) are omitted; the
// fingerprint hash treats missing keys as null.
func (r CnesEstabelecimentoRow) ToRow() delta.Row {
	return delta.Row{
		"CNES":       r.CNES,
		"NOME_FANTA": r.NomeFanta,
		"TP_UNID_ID": r.TPUnidID,
		"CODMUNGEST": r.CodMunGest,
		"CNPJ_MANT":  r.CNPJMant,
	}
}

// ToRow returns canonical column-keyed map for delta fingerprinting.
// Keys mirror delta.ProfileFor("cnes","equipes").FingerprintColumns +
// Pk SEQ_EQUIPE. AREA / MICROAREA / NOME_REF / COD_CBO not extracted today.
func (r CnesEquipeRow) ToRow() delta.Row {
	return delta.Row{
		"SEQ_EQUIPE": r.SeqEquipe,
		"INE":        r.INE,
		"DS_AREA":    r.DSArea,
		"TP_EQUIPE":  r.TPEquipe,
		"COD_MUN":    r.CodMun,
	}
}

// ToRow returns canonical column-keyed map for delta fingerprinting.
// Keys mirror delta.ProfileFor("sihd","aih") expectations: NUM_AIH PK,
// COD_PROC for procedimento, VALOR_TOTAL for value, COMPETEN, CNES.
func (r SihdProducaoRow) ToRow() delta.Row {
	return delta.Row{
		"NUM_AIH":     r.AIHNumero,
		"COD_PROC":    r.Procedimento,
		"COMPETEN":    r.Competencia,
		"CNES":        r.CNES,
		"VALOR_TOTAL": r.Valor,
	}
}
