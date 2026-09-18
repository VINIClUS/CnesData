package audit

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

// VerifyError marks one corrupt or HMAC-mismatched line.
type VerifyError struct {
	LineNum int
	Reason  string
}

// VerifyFile reads each JSONL line, recomputes HMAC over CanonicalJSON
// (sorted keys, hmac field excluded), and returns the parsed events
// alongside per-line errors.
func VerifyFile(path string, key []byte) ([]map[string]any, []VerifyError, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("verify_open: %w", err)
	}
	defer f.Close()
	var (
		valid   []map[string]any
		errs    []VerifyError
		lineNum int
	)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		ev, verr := verifyLine(line, key)
		if verr != "" {
			errs = append(errs, VerifyError{LineNum: lineNum, Reason: verr})
			continue
		}
		valid = append(valid, ev)
	}
	return valid, errs, nil
}

func verifyLine(line, key []byte) (map[string]any, string) {
	var m map[string]any
	if err := json.Unmarshal(line, &m); err != nil {
		return nil, "json_invalid: " + err.Error()
	}
	hmacField, ok := m["hmac"].(string)
	if !ok {
		return nil, "hmac_field_missing"
	}
	delete(m, "hmac")
	canonical, err := json.Marshal(m)
	if err != nil {
		return nil, "canonical_marshal: " + err.Error()
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(canonical)
	want := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(hmacField)) {
		return nil, "hmac_mismatch"
	}
	return m, ""
}
