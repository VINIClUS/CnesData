// Spike: validate nakagami/firebirdsql driver against FB 1.5 GDB.
// Exit 0 = compatible, 1 = rejected protocol (abort spec).
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/nakagami/firebirdsql"
)

func main() {
	host := flag.String("host", "localhost", "FB host (requires embedded server or running instance)")
	port := flag.Int("port", 3050, "FB port")
	path := flag.String("path", "", "GDB absolute path")
	user := flag.String("user", "SYSDBA", "user")
	pass := flag.String("pass", "masterkey", "password")
	flag.Parse()

	if *path == "" {
		slog.Error("spike_missing_path")
		os.Exit(2)
	}

	dsn := fmt.Sprintf("%s:%s@%s:%d/%s?charset=WIN1252", *user, *pass, *host, *port, *path)
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		slog.Error("spike_open_fail", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		slog.Error("spike_ping_fail", "err", err.Error())
		os.Exit(1)
	}

	rows, err := db.Query(`SELECT NU_COMPETENCIA, CO_CNES FROM BPA_CAB`)
	if err != nil {
		slog.Error("spike_query_fail", "err", err.Error())
		os.Exit(1)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		var comp, cnes string
		if err := rows.Scan(&comp, &cnes); err != nil {
			slog.Error("spike_scan_fail", "err", err.Error())
			os.Exit(1)
		}
		slog.Info("spike_row", "comp", comp, "cnes", cnes)
		n++
	}

	slog.Info("spike_ok", "rows", n)
}
