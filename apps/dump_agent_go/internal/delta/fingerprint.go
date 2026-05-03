package delta

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"time"
)

const (
	unitSep   = byte(0x1F)
	recordSep = byte(0x1E)
	nullToken = "\x00NULL"
)

// Hash returns SHA-256 over canonical JSON of the named fields.
// Field order is normalized via lexicographic sort to make output
// deterministic across caller-supplied slice orderings.
func Hash(row Row, fields []string) [32]byte {
	sorted := append([]string(nil), fields...)
	sort.Strings(sorted)
	var buf bytes.Buffer
	for _, f := range sorted {
		buf.WriteString(f)
		buf.WriteByte(unitSep)
		buf.WriteString(canonicalString(row[f]))
		buf.WriteByte(recordSep)
	}
	return sha256.Sum256(buf.Bytes())
}

// HashHex returns Hash() as a 64-char lowercase hex string.
func HashHex(row Row, fields []string) string {
	h := Hash(row, fields)
	return hex.EncodeToString(h[:])
}

func canonicalString(v any) string {
	switch x := v.(type) {
	case nil:
		return nullToken
	case string:
		return x
	case []byte:
		return hex.EncodeToString(x)
	case bool:
		if x {
			return "true"
		}
		return "false"
	case int:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return ""
		}
		return string(b)
	}
}
