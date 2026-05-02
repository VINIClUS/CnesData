package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/cnesdata/dumpagent/internal/diagnose"
)

func TestCmdDiagnose_NoFlags_ExitZeroWhenNoFail(t *testing.T) {
	prevRun := diagnoseRunFn
	diagnoseRunFn = func(_ ctxLike, _ diagnose.Config) []diagnose.Check {
		return []diagnose.Check{
			{Name: "cert", Severity: diagnose.SeverityPass},
		}
	}
	defer func() { diagnoseRunFn = prevRun }()
	var out bytes.Buffer
	code := runDiagnose([]string{}, &out)
	if code != 0 {
		t.Errorf("got exit %d want 0\noutput:\n%s", code, out.String())
	}
	if !strings.Contains(out.String(), "[PASS] cert") {
		t.Errorf("missing PASS line in output:\n%s", out.String())
	}
}

func TestCmdDiagnose_AnyFailExitOne(t *testing.T) {
	prevRun := diagnoseRunFn
	diagnoseRunFn = func(_ ctxLike, _ diagnose.Config) []diagnose.Check {
		return []diagnose.Check{
			{Name: "minio", Severity: diagnose.SeverityFail},
		}
	}
	defer func() { diagnoseRunFn = prevRun }()
	var out bytes.Buffer
	code := runDiagnose([]string{}, &out)
	if code != 1 {
		t.Errorf("got exit %d want 1", code)
	}
}

func TestCmdDiagnose_JSONFlagOutputsJSON(t *testing.T) {
	prevRun := diagnoseRunFn
	diagnoseRunFn = func(_ ctxLike, _ diagnose.Config) []diagnose.Check {
		return []diagnose.Check{
			{Name: "cert", Severity: diagnose.SeverityPass},
		}
	}
	defer func() { diagnoseRunFn = prevRun }()
	var out bytes.Buffer
	_ = runDiagnose([]string{"--json"}, &out)
	body := bytes.TrimSpace(out.Bytes())
	if len(body) == 0 || body[0] != '[' {
		t.Errorf("expected JSON array, got: %s", string(body))
	}
	var arr []diagnose.Check
	if err := json.Unmarshal(body, &arr); err != nil {
		t.Errorf("invalid JSON: %v\noutput: %s", err, string(body))
	}
}

func TestCmdDiagnose_ProbeFlagPropagates(t *testing.T) {
	captured := diagnose.Config{}
	prevRun := diagnoseRunFn
	diagnoseRunFn = func(_ ctxLike, cfg diagnose.Config) []diagnose.Check {
		captured = cfg
		return nil
	}
	defer func() { diagnoseRunFn = prevRun }()
	_ = runDiagnose([]string{"--probe"}, new(bytes.Buffer))
	if !captured.Probe {
		t.Errorf("Probe not propagated to Config")
	}
}
