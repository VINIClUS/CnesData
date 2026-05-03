package delta

import "fmt"

// Profile holds per-(source, intent) data driving fingerprinting.
// PKExtractor returns the stable string key from a Row.
// FingerprintColumns is the sorted list of columns SHA-256 covers.
type Profile struct {
	Source             string
	Intent             string
	PKExtractor        func(Row) string
	FingerprintColumns []string
}

// ProfileFor returns the profile for the given (source, intent) pair.
// Unknown combinations return zero Profile (Source == "").
func ProfileFor(source, intent string) Profile {
	switch source + "/" + intent {
	case "cnes/estabelecimentos":
		return cnesEstabelecimentosProfile()
	case "cnes/profissionais":
		return cnesProfissionaisProfile()
	case "cnes/equipes":
		return cnesEquipesProfile()
	case "sihd/aih":
		return sihdAIHProfile()
	case "bpa/linhas":
		return bpaLinhasProfile()
	default:
		return Profile{}
	}
}

func cnesEstabelecimentosProfile() Profile {
	return Profile{
		Source: "cnes", Intent: "estabelecimentos",
		PKExtractor: func(r Row) string { return asString(r["CNES"]) },
		FingerprintColumns: []string{
			"ATIVO", "BAIRRO", "CEP", "CNES", "CODMUNGEST", "COD_CEP",
			"NOME_FANTA", "NO_LOGRADOURO", "NU_ENDERECO", "NU_TELEFONE",
			"TP_UNID_ID",
		},
	}
}

func cnesProfissionaisProfile() Profile {
	return Profile{
		Source: "cnes", Intent: "profissionais",
		PKExtractor: func(r Row) string {
			return fmt.Sprintf("%s|%s|%s",
				asString(r["CPF_PROF"]), asString(r["CNES"]),
				asString(r["COD_CBO"]))
		},
		FingerprintColumns: []string{
			"CARGA_HORARIA_TOTAL", "CGHORAHOSP", "CGHORAOUTR", "CG_HORAAMB",
			"CNES", "COD_CBO", "COD_CNS", "CPF_PROF", "DATA_NASC",
			"IND_VINC", "NOME_PROF", "NO_SOCIAL", "SEXO", "TP_SUS_NAO_SUS",
		},
	}
}

func cnesEquipesProfile() Profile {
	return Profile{
		Source: "cnes", Intent: "equipes",
		PKExtractor: func(r Row) string {
			s := asString(r["SEQ_EQUIPE"])
			if len(s) >= 4 {
				return s[:4]
			}
			return s
		},
		FingerprintColumns: []string{
			"AREA", "CNES", "COD_CBO", "MICROAREA", "NOME_REF",
			"SEQ_EQUIPE", "TP_EQUIPE",
		},
	}
}

func sihdAIHProfile() Profile {
	return Profile{
		Source: "sihd", Intent: "aih",
		PKExtractor: func(r Row) string { return asString(r["NUM_AIH"]) },
		FingerprintColumns: []string{
			"CGC_HOSP", "CNES", "COD_PROC", "COMPETEN", "CPF_PAC",
			"DATA_INTERNA", "DATA_SAIDA", "ESPECIALIDADE", "MOTIVO_ALTA",
			"NOME_PAC", "NUM_AIH", "TIPO_AIH", "VALOR_TOTAL",
		},
	}
}

func bpaLinhasProfile() Profile {
	return Profile{
		Source: "bpa", Intent: "linhas",
		PKExtractor: func(r Row) string {
			return fmt.Sprintf("%s|%s|%s",
				asString(r["CPF"]), asString(r["COMPETEN"]),
				asString(r["COD_PROC"]))
		},
		FingerprintColumns: []string{
			"CBO", "CNES", "COD_PROC", "COMPETEN", "CPF",
			"MOTIVO_AUTORIZ", "NOME", "QTD", "VALOR_UNIT",
		},
	}
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}
