package main

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const agentRawModeVar = "AGENT_RAW_MODE"

type rawModeEnvCase struct {
	name  string
	value string
	unset bool
	raw   bool
}

var rawModeEnvCases = []rawModeEnvCase{
	{name: "ausente", unset: true, raw: true},
	{name: "vazio", value: "", raw: true},
	{name: "true", value: "true", raw: true},
	{name: "TRUE", value: "TRUE", raw: true},
	{name: "false", value: "false", raw: false},
	{name: "FALSE", value: "FALSE", raw: false},
	{name: "False", value: "False", raw: false},
	{name: "zero", value: "0", raw: true},
	{name: "no", value: "no", raw: true},
	{name: "valor_desconhecido", value: "banana", raw: true},
}

func setRawModeEnv(t *testing.T, value string, unset bool) {
	t.Helper()
	t.Setenv(agentRawModeVar, value)
	if unset {
		require.NoError(t, os.Unsetenv(agentRawModeVar))
	}
}

func TestDefaultRunFlagsUsaRawSalvoOptOutExplicito(t *testing.T) {
	for _, tc := range rawModeEnvCases {
		t.Run(tc.name, func(t *testing.T) {
			setRawModeEnv(t, tc.value, tc.unset)

			require.Equal(t, tc.raw, defaultRunFlags().Raw)
		})
	}
}

func TestParseRunFlagsUsaRawSalvoOptOutExplicitoNoAmbiente(t *testing.T) {
	for _, tc := range rawModeEnvCases {
		t.Run(tc.name, func(t *testing.T) {
			setRawModeEnv(t, tc.value, tc.unset)

			require.Equal(t, tc.raw, parseRunFlags(nil).Raw)
		})
	}
}

func TestParseRunFlagsFlagExplicitaPrevaleceSobreAmbiente(t *testing.T) {
	cases := []struct {
		name  string
		value string
		unset bool
		args  []string
		raw   bool
	}{
		{name: "raw sem ambiente", unset: true, args: []string{"--raw"}, raw: true},
		{name: "raw=false sem ambiente", unset: true, args: []string{"--raw=false"}},
		{name: "raw=false com ambiente true", value: "true", args: []string{"--raw=false"}},
		{name: "raw com ambiente false", value: "false", args: []string{"--raw"}, raw: true},
		{name: "raw=true com ambiente false", value: "false", args: []string{"--raw=true"}, raw: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRawModeEnv(t, tc.value, tc.unset)

			require.Equal(t, tc.raw, parseRunFlags(tc.args).Raw)
		})
	}
}

func TestLogRunModeRegistraProtocoloEValorDoAmbiente(t *testing.T) {
	cases := []struct {
		name  string
		value string
		flags RunFlags
		want  []string
	}{
		{name: "raw por padrao", flags: RunFlags{Raw: true},
			want: []string{"protocol=raw", `agent_raw_mode=""`}},
		{name: "legado por opt-out", value: "false", flags: RunFlags{},
			want: []string{"protocol=legacy", "agent_raw_mode=false"}},
		{name: "valor inesperado segue raw", value: "0", flags: RunFlags{Raw: true},
			want: []string{"protocol=raw", "agent_raw_mode=0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRawModeEnv(t, tc.value, false)
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))

			logRunMode(logger, tc.flags)

			out := buf.String()
			require.Equal(t, 1, strings.Count(out, "msg=run_mode"))
			for _, fragment := range tc.want {
				require.Contains(t, out, fragment)
			}
		})
	}
}
