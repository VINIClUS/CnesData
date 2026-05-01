package extractor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSIA_ReadAllDBFs(t *testing.T) {
	dir := filepath.Join("..", "..", "test", "integration", "fixtures",
		"sia_synthetic")
	result, err := ExtractSIA(dir)
	if err != nil {
		t.Fatalf("extract err=%v", err)
	}
	if len(result.APA) == 0 {
		t.Error("APA empty")
	}
	if len(result.BPI) == 0 {
		t.Error("BPI empty")
	}
	if len(result.BPIHST) == 0 {
		t.Error("BPIHST empty")
	}
	if len(result.CDN) == 0 {
		t.Error("CDN empty")
	}
	if len(result.CADMUN) == 0 {
		t.Error("CADMUN empty")
	}
}

func TestSIA_MissingDir(t *testing.T) {
	_, err := ExtractSIA(filepath.Join("nonexistent", "path"))
	if err == nil {
		t.Fatal("expected error for missing dir")
	}
}

func TestSIA_EmptyDir(t *testing.T) {
	tmp := t.TempDir()
	_, err := ExtractSIA(tmp)
	if err != nil {
		t.Fatalf("empty dir should not error, got=%v", err)
	}
}

func TestSIA_Cp1252Sanitize(t *testing.T) {
	raw := "SAO JO\xc3O"
	got, dirty := SanitizeString(raw)
	if got == "" {
		t.Error("expected non-empty sanitized output")
	}
	if dirty == 0 {
		t.Error("expected dirty counter > 0 for invalid UTF-8 byte")
	}
}

func TestSIA_APAFields(t *testing.T) {
	dir := filepath.Join("..", "..", "test", "integration", "fixtures",
		"sia_synthetic")
	result, err := ExtractSIA(dir)
	if err != nil {
		t.Fatalf("extract err=%v", err)
	}
	for i, r := range result.APA {
		if r.Competencia != "202601" {
			t.Errorf("APA[%d].Competencia=%q want=202601", i, r.Competencia)
		}
		if r.Cnes != "2269481" {
			t.Errorf("APA[%d].Cnes=%q want=2269481", i, r.Cnes)
		}
		if r.Quantidade < 1 || r.Quantidade > 10 {
			t.Errorf("APA[%d].Quantidade=%d out of range", i, r.Quantidade)
		}
		if r.DtInicio.IsZero() {
			t.Errorf("APA[%d].DtInicio zero", i)
		}
	}
}

func TestSIA_BPIFields(t *testing.T) {
	dir := filepath.Join("..", "..", "test", "integration", "fixtures",
		"sia_synthetic")
	result, err := ExtractSIA(dir)
	if err != nil {
		t.Fatalf("extract err=%v", err)
	}
	if len(result.BPI) == 0 {
		t.Fatal("BPI empty")
	}
	for i, r := range result.BPI {
		if r.Competencia != "202601" {
			t.Errorf("BPI[%d].Competencia=%q want=202601", i, r.Competencia)
		}
		if r.Cnes != "2269481" {
			t.Errorf("BPI[%d].Cnes=%q want=2269481", i, r.Cnes)
		}
		if r.Quantidade < 0 {
			t.Errorf("BPI[%d].Quantidade=%d negative", i, r.Quantidade)
		}
		if r.DtAtendimento.IsZero() {
			t.Errorf("BPI[%d].DtAtendimento zero", i)
		}
	}
}

func TestSIA_CDNFields(t *testing.T) {
	dir := filepath.Join("..", "..", "test", "integration", "fixtures",
		"sia_synthetic")
	result, err := ExtractSIA(dir)
	if err != nil {
		t.Fatalf("extract err=%v", err)
	}
	if len(result.CDN) == 0 {
		t.Fatal("CDN empty")
	}
	for i, r := range result.CDN {
		if r.Tabela == "" {
			t.Errorf("CDN[%d].Tabela empty", i)
		}
		if r.Item == "" {
			t.Errorf("CDN[%d].Item empty", i)
		}
	}
}

func TestSIA_CADMUNFields(t *testing.T) {
	dir := filepath.Join("..", "..", "test", "integration", "fixtures",
		"sia_synthetic")
	result, err := ExtractSIA(dir)
	if err != nil {
		t.Fatalf("extract err=%v", err)
	}
	if len(result.CADMUN) == 0 {
		t.Fatal("CADMUN empty")
	}
	for i, r := range result.CADMUN {
		if r.CodMun == "" {
			t.Errorf("CADMUN[%d].CodMun empty", i)
		}
		if len(r.CodMun) != 6 && len(r.CodMun) != 7 {
			t.Errorf("CADMUN[%d].CodMun=%q unexpected length", i, r.CodMun)
		}
	}
}

func TestSIA_CorruptDBFErrors(t *testing.T) {
	tmp := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmp, "S_APA.DBF"),
		[]byte("this is not a dbf"), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := ExtractSIA(tmp)
	if err == nil {
		t.Fatal("expected error for corrupt DBF, got nil")
	}
	if result == nil {
		t.Error("result nil; expected partial result even on error")
	}
}
