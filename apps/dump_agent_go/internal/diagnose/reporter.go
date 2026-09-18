package diagnose

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// Text renders checks in human-readable format with a summary line.
func Text(w io.Writer, checks []Check) error {
	if _, err := fmt.Fprintln(w, "dumpagent diagnose"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "=================="); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	var nPass, nWarn, nFail int
	for _, c := range checks {
		switch c.Severity {
		case SeverityPass:
			nPass++
		case SeverityWarn:
			nWarn++
		case SeverityFail:
			nFail++
		}
		if _, err := fmt.Fprintf(w, "[%s] %-12s %s\n",
			c.Severity, c.Name, formatFields(c.Fields)); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "Summary: %d PASS, %d WARN, %d FAIL\n",
		nPass, nWarn, nFail)
	return err
}

// JSON encodes checks as a JSON array.
func JSON(w io.Writer, checks []Check) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(checks)
}

// AnyFail reports whether any check has SeverityFail.
func AnyFail(checks []Check) bool {
	for _, c := range checks {
		if c.Severity == SeverityFail {
			return true
		}
	}
	return false
}

// formatFields renders fields as key=val space-separated, deterministic order.
func formatFields(fields map[string]any) string {
	if len(fields) == 0 {
		return ""
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, fields[k]))
	}
	return strings.Join(parts, " ")
}
