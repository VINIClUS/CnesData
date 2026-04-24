package extractor

import (
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
