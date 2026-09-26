package manifest

import "slices"

var rawSubtypes = map[SourceType][]string{
	SourceTypeCNESLocal: {"CNES_VINCULO"},
	SourceTypeSIHD:      {"SIHD_INTERNACAO", "SIHD_PROC_AIH"},
	SourceTypeBPAMag:    {"BPA_C", "BPA_I"},
	SourceTypeSIALocal:  {"SIA_APA", "SIA_BPI", "SIA_BPIHST", "DIM_SIGTAP", "DIM_MUNICIPIO"},
}

// RawSubtypes lista, em ordem fixa, os file_subtypes que o Edge emite por fonte.
func RawSubtypes(sourceType SourceType) []string {
	return slices.Clone(rawSubtypes[sourceType])
}

// ValidRawPair indica se o par (source_type, file_subtype) pertence à matriz do Edge.
func ValidRawPair(sourceType SourceType, fileSubtype string) bool {
	return slices.Contains(rawSubtypes[sourceType], fileSubtype)
}
