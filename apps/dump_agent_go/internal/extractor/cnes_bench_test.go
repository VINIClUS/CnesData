package extractor_test

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/cnesdata/dumpagent/internal/extractor"
)

func BenchmarkExtractCnesProfissionais(b *testing.B) {
	db, mock, err := sqlmock.New()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	cols := []string{
		"cpf_prof", "cod_cns", "nome_prof", "no_social", "sexo", "data_nasc",
		"cod_cbo", "ind_vinc", "tp_sus_nao_sus", "carga_horaria_total",
		"cg_horaamb", "cghoraoutr", "cghorahosp",
		"cnes", "nome_fanta", "tp_unid_id", "codmungest",
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows := sqlmock.NewRows(cols)
		for j := 0; j < 1000; j++ {
			rows.AddRow(
				"11111111111", "123456789012345", "Nome "+string(rune(j%26+'A')),
				"", "M", "1990-01-01",
				"225125", "1", "S", 40,
				20, 10, 10,
				"2000001", "UBS Test", "05", "354130",
			)
		}
		mock.ExpectQuery("SELECT prof.CPF_PROF").
			WithArgs("354130").
			WillReturnRows(rows)

		ch := make(chan extractor.CnesProfissionalRow, 1024)
		done := make(chan struct{})
		go func() {
			for range ch {
			}
			close(done)
		}()

		err := extractor.ExtractCnesProfissionais(context.Background(), conn,
			extractor.ExtractionParams{CodMunGest: "354130"}, ch)
		close(ch)
		<-done
		if err != nil {
			b.Fatal(err)
		}
	}
}
