package main

import (
	"testing"

	"github.com/cnesdata/dumpagent/internal/discover"
	"github.com/stretchr/testify/require"
)

func TestResolveString_CLIWinsAll(t *testing.T) {
	got, layer := resolveString("CLI_VAL", envFnConst("ENV_VAL"), "YAML_VAL", "DEFAULT")
	require.Equal(t, "CLI_VAL", got)
	require.Equal(t, layerCLI, layer)
}

func TestResolveString_EnvBeatsYAML(t *testing.T) {
	got, layer := resolveString("", envFnConst("ENV_VAL"), "YAML_VAL", "DEFAULT")
	require.Equal(t, "ENV_VAL", got)
	require.Equal(t, layerEnv, layer)
}

func TestResolveString_YAMLBeatsDefault(t *testing.T) {
	got, layer := resolveString("", envFnConst(""), "YAML_VAL", "DEFAULT")
	require.Equal(t, "YAML_VAL", got)
	require.Equal(t, layerYAML, layer)
}

func TestResolveString_DefaultLast(t *testing.T) {
	got, layer := resolveString("", envFnConst(""), "", "DEFAULT")
	require.Equal(t, "DEFAULT", got)
	require.Equal(t, layerDefault, layer)
}

func TestResolveInt_FallsThroughToDefault(t *testing.T) {
	got, layer := resolveInt(0, envFnConst(""), 0, 3050)
	require.Equal(t, 3050, got)
	require.Equal(t, layerDefault, layer)
}

func TestResolvePathConfig_FBSourceFromYAML(t *testing.T) {
	cfg := discover.Config{
		CNES: discover.FBDSN{
			Host: "yaml-host", Port: 3060,
			DatabasePath: `C:\YAML\CNES.GDB`,
			User:         "YAMLUSER",
			Charset:      "UTF8",
		},
	}
	envs := envFnNone()
	res := ResolvePathConfig(cfg, envs, RunCLIFlags{})
	require.Equal(t, "yaml-host", res.CNES.Host)
	require.Equal(t, 3060, res.CNES.Port)
	require.Equal(t, `C:\YAML\CNES.GDB`, res.CNES.DatabasePath)
	require.Equal(t, "YAMLUSER", res.CNES.User)
	require.Equal(t, "UTF8", res.CNES.Charset)
}

func TestResolvePathConfig_EnvOverridesYAML(t *testing.T) {
	cfg := discover.Config{
		CNES: discover.FBDSN{Host: "yaml-host", DatabasePath: `C:\YAML\X.GDB`},
	}
	envs := envFnMap(map[string]string{
		"CNES_DB_HOST": "env-host",
		"CNES_DB_PATH": `C:\ENV\X.GDB`,
	})
	res := ResolvePathConfig(cfg, envs, RunCLIFlags{})
	require.Equal(t, "env-host", res.CNES.Host)
	require.Equal(t, `C:\ENV\X.GDB`, res.CNES.DatabasePath)
	require.Len(t, res.Overrides, 2)
}

func TestResolvePathConfig_DefaultsForMissingFields(t *testing.T) {
	res := ResolvePathConfig(discover.Config{}, envFnNone(), RunCLIFlags{})
	require.Equal(t, "localhost", res.CNES.Host)
	require.Equal(t, 3050, res.CNES.Port)
	require.Equal(t, "SYSDBA", res.CNES.User)
	require.Equal(t, "WIN1252", res.CNES.Charset)
	require.Equal(t, "", res.CNES.DatabasePath)
}

func envFnConst(v string) func(string) string {
	return func(_ string) string { return v }
}

func envFnNone() func(string) string {
	return func(_ string) string { return "" }
}

func envFnMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}
