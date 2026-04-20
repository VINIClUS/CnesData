package fbdriver_test

import (
	"testing"

	"github.com/cnesdata/dumpagent/internal/fbdriver"
	"github.com/stretchr/testify/require"
)

func TestBuildDSN_AllFields(t *testing.T) {
	dsn := fbdriver.BuildDSN(fbdriver.ConnConfig{
		Host:     "localhost",
		Port:     3050,
		Path:     "C:/CNES.GDB",
		User:     "SYSDBA",
		Password: "masterkey",
		Charset:  "WIN1252",
	})
	require.Equal(t, "SYSDBA:masterkey@localhost:3050/C:/CNES.GDB?charset=WIN1252", dsn)
}

func TestBuildDSN_DefaultCharsetWIN1252(t *testing.T) {
	dsn := fbdriver.BuildDSN(fbdriver.ConnConfig{
		Host: "h", Port: 3050, Path: "/db", User: "u", Password: "p",
	})
	require.Contains(t, dsn, "charset=WIN1252")
}
