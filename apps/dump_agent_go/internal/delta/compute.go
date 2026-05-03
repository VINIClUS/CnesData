package delta

// Compute diffs current vs committed snapshots and produces a DeltaSet.
// committed: map[pk]→last-cycle-hash
// currentHashes: map[pk]→this-cycle-hash
// currentRows: in same order as extracted (used for I/U payloads)
// For Deletes, only the PK column(s) are populated in the emitted Row.
func Compute(
	committed, currentHashes map[string][32]byte,
	currentRows []Row, prof Profile,
) DeltaSet {
	ds := DeltaSet{}
	for _, r := range currentRows {
		pk := prof.PKExtractor(r)
		curH, ok := currentHashes[pk]
		if !ok {
			continue
		}
		prior, exists := committed[pk]
		if !exists {
			ds.Inserts = append(ds.Inserts, r)
			continue
		}
		if prior != curH {
			ds.Updates = append(ds.Updates, r)
		}
	}
	for pk := range committed {
		if _, stillThere := currentHashes[pk]; !stillThere {
			ds.Deletes = append(ds.Deletes, deletedRow(pk, prof))
		}
	}
	return ds
}

// deletedRow synthesizes a minimal Row for an Op=Delete entry.
// Splits composite PK on "|" and assigns to PK columns by lookup table.
func deletedRow(pk string, prof Profile) Row {
	r := Row{}
	cols := pkColumnsForProfile(prof.Source, prof.Intent)
	if len(cols) == 1 {
		r[cols[0]] = pk
		return r
	}
	parts := splitPK(pk)
	for i, col := range cols {
		if i >= len(parts) {
			break
		}
		r[col] = parts[i]
	}
	return r
}

func pkColumnsForProfile(source, intent string) []string {
	switch source + "/" + intent {
	case "cnes/estabelecimentos":
		return []string{"CNES"}
	case "cnes/profissionais":
		return []string{"CPF_PROF", "CNES", "COD_CBO"}
	case "cnes/equipes":
		return []string{"SEQ_EQUIPE"}
	case "sihd/aih":
		return []string{"NUM_AIH"}
	case "bpa/linhas":
		return []string{"CPF", "COMPETEN", "COD_PROC"}
	default:
		return nil
	}
}

func splitPK(pk string) []string {
	parts := []string{}
	cur := ""
	for i := 0; i < len(pk); i++ {
		if pk[i] == '|' {
			parts = append(parts, cur)
			cur = ""
			continue
		}
		cur += string(pk[i])
	}
	parts = append(parts, cur)
	return parts
}
