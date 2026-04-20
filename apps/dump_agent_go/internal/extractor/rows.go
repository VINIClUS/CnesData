package extractor

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
