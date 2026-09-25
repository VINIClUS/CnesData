package extractor

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/LindsayBradford/go-dbf/godbf"
)

// siaTable envolve um DBF SIA já validado contra os campos exigidos.
type siaTable struct {
	*godbf.DbfTable
	file string
}

func openSIADBF(dir, file string, required []string) (*siaTable, error) {
	path := filepath.Join(dir, file)
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("sia_file_missing file=%s: %w", file, err)
		}
		return nil, fmt.Errorf("sia_file_stat file=%s: %w", file, err)
	}
	t, err := godbf.NewFromFile(path, "windows-1252")
	if err != nil {
		return nil, fmt.Errorf("dbf_open file=%s: %w", file, err)
	}
	present := make(map[string]bool, len(t.FieldNames()))
	for _, name := range t.FieldNames() {
		present[strings.ToUpper(name)] = true
	}
	for _, name := range required {
		if !present[name] {
			return nil, fmt.Errorf("sia_field_missing file=%s field=%s", file, name)
		}
	}
	return &siaTable{DbfTable: t, file: file}, nil
}

// Blank text stays "" (not null); data_processor treats blank as null, as in BPA.
func (t *siaTable) text(row int, field string) string {
	v, _ := t.FieldValueByName(row, field)
	clean, _ := SanitizeString(strings.TrimSpace(v))
	return clean
}

func (t *siaTable) decimal(row int, field string) (*float64, error) {
	v := t.text(row, field)
	if v == "" {
		return nil, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return nil, fmt.Errorf("sia_numeric_invalid file=%s field=%s row=%d", t.file, field, row)
	}
	return &f, nil
}

func (t *siaTable) integer(row int, field string) (*int64, error) {
	f, err := t.decimal(row, field)
	if f == nil || err != nil {
		return nil, err
	}
	n := int64(math.Round(*f))
	return &n, nil
}

func (t *siaTable) cents(row int, field string) (*int64, error) {
	f, err := t.decimal(row, field)
	if f == nil || err != nil {
		return nil, err
	}
	n := int64(math.Round(*f * 100))
	return &n, nil
}
