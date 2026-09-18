package main

import (
	"strconv"

	"github.com/cnesdata/dumpagent/internal/discover"
)

type ConfigLayer int

const (
	layerNone ConfigLayer = iota
	layerCLI
	layerEnv
	layerYAML
	layerDefault
)

func (l ConfigLayer) String() string {
	switch l {
	case layerCLI:
		return "cli"
	case layerEnv:
		return "env"
	case layerYAML:
		return "yaml"
	case layerDefault:
		return "default"
	default:
		return "none"
	}
}

type FBDSNFlags struct {
	Host         string
	Port         int
	DatabasePath string
	User         string
	Charset      string
}

type RunCLIFlags struct {
	CNES FBDSNFlags
	SIHD FBDSNFlags
	BPA  FBDSNFlags
	SIA  string
}

type OverrideRecord struct {
	Layer  ConfigLayer
	Source string
	Field  string
}

type PathConfig struct {
	CNES      discover.FBDSN
	SIHD      discover.FBDSN
	BPA       discover.FBDSN
	SIADir    string
	Overrides []OverrideRecord
}

type fbResolveCtx struct {
	Source string
	Flags  FBDSNFlags
	EnvFn  func(string) string
	YAML   discover.FBDSN
}

func ResolvePathConfig(
	cfg discover.Config, envFn func(string) string, flags RunCLIFlags,
) PathConfig {
	out := PathConfig{}
	out.CNES, out.Overrides = resolveFB(fbResolveCtx{
		Source: "cnes", Flags: flags.CNES, EnvFn: envFn, YAML: cfg.CNES,
	}, out.Overrides)
	out.SIHD, out.Overrides = resolveFB(fbResolveCtx{
		Source: "sihd", Flags: flags.SIHD, EnvFn: envFn, YAML: cfg.SIHD,
	}, out.Overrides)
	out.BPA, out.Overrides = resolveFB(fbResolveCtx{
		Source: "bpa", Flags: flags.BPA, EnvFn: envFn, YAML: cfg.BPA,
	}, out.Overrides)
	out.SIADir, out.Overrides = resolveSIA(flags.SIA, envFn,
		cfg.SIA.DBFDir, out.Overrides)
	return out
}

func resolveFB(
	c fbResolveCtx, overrides []OverrideRecord,
) (discover.FBDSN, []OverrideRecord) {
	prefix := upperASCII(c.Source)
	host, hl := resolveString(c.Flags.Host,
		envFnK(c.EnvFn, prefix+"_DB_HOST"), c.YAML.Host, "localhost")
	overrides = appendOverride(overrides, hl, c.Source, "host")
	port, pl := resolveInt(c.Flags.Port,
		envFnK(c.EnvFn, prefix+"_DB_PORT"), c.YAML.Port, 3050)
	overrides = appendOverride(overrides, pl, c.Source, "port")
	path, pthl := resolveString(c.Flags.DatabasePath,
		envFnK(c.EnvFn, prefix+"_DB_PATH"), c.YAML.DatabasePath, "")
	overrides = appendOverride(overrides, pthl, c.Source, "database_path")
	user, ul := resolveString(c.Flags.User,
		envFnK(c.EnvFn, prefix+"_DB_USER"), c.YAML.User, "SYSDBA")
	overrides = appendOverride(overrides, ul, c.Source, "user")
	charset, cl := resolveString(c.Flags.Charset,
		envFnK(c.EnvFn, prefix+"_DB_CHARSET"), c.YAML.Charset, "WIN1252")
	overrides = appendOverride(overrides, cl, c.Source, "charset")
	return discover.FBDSN{
		Host: host, Port: port, DatabasePath: path,
		User: user, Charset: charset,
	}, overrides
}

func resolveSIA(
	cliDir string, envFn func(string) string, yamlDir string,
	overrides []OverrideRecord,
) (string, []OverrideRecord) {
	dir, layer := resolveString(cliDir, envFnK(envFn, "SIA_DIR"), yamlDir, "")
	overrides = appendOverride(overrides, layer, "sia", "dbf_dir")
	return dir, overrides
}

func appendOverride(
	overrides []OverrideRecord, layer ConfigLayer, source, field string,
) []OverrideRecord {
	if layer == layerCLI || layer == layerEnv {
		overrides = append(overrides,
			OverrideRecord{Layer: layer, Source: source, Field: field})
	}
	return overrides
}

func resolveString(cli string, envFn func(string) string, yaml, def string) (string, ConfigLayer) {
	if cli != "" {
		return cli, layerCLI
	}
	if v := envFn(""); v != "" {
		return v, layerEnv
	}
	if yaml != "" {
		return yaml, layerYAML
	}
	return def, layerDefault
}

func resolveInt(cli int, envFn func(string) string, yaml, def int) (int, ConfigLayer) {
	if cli != 0 {
		return cli, layerCLI
	}
	if v := envFn(""); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n != 0 {
			return n, layerEnv
		}
	}
	if yaml != 0 {
		return yaml, layerYAML
	}
	return def, layerDefault
}

func envFnK(envFn func(string) string, key string) func(string) string {
	return func(string) string { return envFn(key) }
}

func upperASCII(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		out[i] = c
	}
	return string(out)
}
