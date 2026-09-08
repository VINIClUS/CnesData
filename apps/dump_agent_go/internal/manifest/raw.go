package manifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type SourceType string

const (
	SourceTypeCNESLocal    SourceType = "CNES_LOCAL"
	SourceTypeCNESNacional SourceType = "CNES_NACIONAL"
	SourceTypeSIHD         SourceType = "SIHD"
	SourceTypeBPAMag       SourceType = "BPA_MAG"
	SourceTypeSIALocal     SourceType = "SIA_LOCAL"
)

type SnapshotMode string

const (
	SnapshotModeFull  SnapshotMode = "FULL"
	SnapshotModeDelta SnapshotMode = "DELTA"
)

type Raw struct {
	ManifestVersion        int          `json:"manifest_version"`
	ManifestID             string       `json:"manifest_id"`
	TenantID               string       `json:"tenant_id"`
	SourceType             SourceType   `json:"source_type"`
	FileSubtype            string       `json:"file_subtype"`
	Competencia            string       `json:"competencia"`
	AgentID                string       `json:"agent_id"`
	AgentVersion           string       `json:"agent_version"`
	SchemaVersion          string       `json:"schema_version"`
	SnapshotMode           SnapshotMode `json:"snapshot_mode"`
	SnapshotID             string       `json:"snapshot_id"`
	BaseSnapshotID         *string      `json:"base_snapshot_id"`
	Sequence               uint32       `json:"sequence"`
	PreviousManifestSHA256 *string      `json:"previous_manifest_sha256"`
	ObjectSHA256           string       `json:"object_sha256"`
	RowCount               int64        `json:"row_count"`
	SizeBytes              int64        `json:"size_bytes"`
	ObjectKey              string       `json:"object_key"`
	CreatedAt              time.Time    `json:"created_at"`
}

type PreviousHead struct {
	SnapshotID     string
	Sequence       uint32
	ManifestSHA256 string
}

type BuildRequest struct {
	JobID         string
	TenantID      string
	SourceType    SourceType
	FileSubtype   string
	Competencia   string
	AgentID       string
	AgentVersion  string
	SchemaVersion string
	SnapshotMode  SnapshotMode
	ObjectSHA256  string
	RowCount      int64
	SizeBytes     int64
	CreatedAt     time.Time
	Previous      *PreviousHead
}

var (
	competenciaPattern = regexp.MustCompile(`^[0-9]{4}-(0[1-9]|1[0-2])$`)
	hashPattern        = regexp.MustCompile(`^[0-9a-f]{64}$`)
	safeSegmentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
)

func Build(request BuildRequest) (Raw, error) {
	baseSnapshotID, sequence, previousHash, err := buildChain(request)
	if err != nil {
		return Raw{}, err
	}
	raw := Raw{
		ManifestVersion: 1, ManifestID: request.JobID, TenantID: request.TenantID,
		SourceType: request.SourceType, FileSubtype: request.FileSubtype,
		Competencia: request.Competencia, AgentID: request.AgentID,
		AgentVersion: request.AgentVersion, SchemaVersion: request.SchemaVersion,
		SnapshotMode: request.SnapshotMode, SnapshotID: request.JobID,
		BaseSnapshotID: baseSnapshotID, Sequence: sequence,
		PreviousManifestSHA256: previousHash, ObjectSHA256: request.ObjectSHA256,
		RowCount: request.RowCount, SizeBytes: request.SizeBytes,
		CreatedAt: request.CreatedAt,
	}
	raw.ObjectKey = rawObjectKey(raw)
	if err := validateRaw(raw); err != nil {
		return Raw{}, err
	}
	return raw, nil
}

func CanonicalJSON(raw Raw) ([]byte, error) {
	if err := validateRaw(raw); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	type fields Raw
	value := struct {
		*fields
		CreatedAt string `json:"created_at"`
	}{(*fields)(&raw), pythonTimestamp(raw.CreatedAt)}
	if err := encoder.Encode(value); err != nil {
		return nil, fmt.Errorf("json=%w", err)
	}
	return pythonSeparators(bytes.TrimSuffix(output.Bytes(), []byte{'\n'})), nil
}

func pythonTimestamp(value time.Time) string {
	if value.Nanosecond() == 0 {
		return value.Format(time.RFC3339)
	}
	return value.Format("2006-01-02T15:04:05.000000Z")
}

func pythonSeparators(input []byte) []byte {
	var output bytes.Buffer
	for i := 0; i < len(input); i++ {
		if input[i] != '\\' || i+1 >= len(input) {
			output.WriteByte(input[i])
			continue
		}
		if i+6 <= len(input) && string(input[i:i+5]) == `\u202` &&
			(input[i+5] == '8' || input[i+5] == '9') {
			output.WriteRune('\u2028' + rune(input[i+5]-'8'))
			i += 5
			continue
		}
		output.Write(input[i : i+2])
		i++
	}
	return output.Bytes()
}

func SHA256(raw Raw) (string, error) {
	payload, err := CanonicalJSON(raw)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func buildChain(request BuildRequest) (*string, uint32, *string, error) {
	switch request.SnapshotMode {
	case SnapshotModeFull:
		return nil, 1, nil, nil
	case SnapshotModeDelta:
		if err := validatePrevious(request.Previous); err != nil {
			return nil, 0, nil, err
		}
		base := request.Previous.SnapshotID
		previousHash := request.Previous.ManifestSHA256
		return &base, request.Previous.Sequence + 1, &previousHash, nil
	default:
		return nil, 0, nil, invalid("snapshot_mode")
	}
}

func validatePrevious(previous *PreviousHead) error {
	if previous == nil {
		return invalid("previous")
	}
	if !safeSegmentPattern.MatchString(previous.SnapshotID) {
		return invalid("previous_snapshot_id")
	}
	if previous.Sequence == 0 || previous.Sequence == ^uint32(0) {
		return invalid("previous_sequence")
	}
	if !hashPattern.MatchString(previous.ManifestSHA256) {
		return invalid("previous_manifest_sha256")
	}
	return nil
}

func validateRaw(raw Raw) error {
	if raw.ManifestVersion != 1 {
		return invalid("manifest_version")
	}
	if err := validateRequired(raw); err != nil {
		return err
	}
	if !validSourceType(raw.SourceType) {
		return invalid("source_type")
	}
	if !competenciaPattern.MatchString(raw.Competencia) {
		return invalid("competencia")
	}
	if !hashPattern.MatchString(raw.ObjectSHA256) {
		return invalid("object_sha256")
	}
	if raw.RowCount < 0 || raw.SizeBytes <= 0 {
		return invalid("cardinality")
	}
	if err := validateTimestamp(raw.CreatedAt); err != nil {
		return err
	}
	if err := validateChain(raw); err != nil {
		return err
	}
	return validateObjectKey(raw)
}

func validateTimestamp(value time.Time) error {
	if _, offset := value.Zone(); offset != 0 {
		return invalid("created_at")
	}
	if value.Nanosecond()%1000 != 0 {
		return invalid("created_at_precision")
	}
	return nil
}

func validateRequired(raw Raw) error {
	values := []struct {
		name  string
		value string
	}{
		{"manifest_id", raw.ManifestID}, {"tenant_id", raw.TenantID},
		{"file_subtype", raw.FileSubtype}, {"agent_id", raw.AgentID},
		{"agent_version", raw.AgentVersion}, {"schema_version", raw.SchemaVersion},
		{"snapshot_id", raw.SnapshotID}, {"object_key", raw.ObjectKey},
	}
	for _, field := range values {
		if field.value == "" {
			return invalid(field.name)
		}
	}
	return nil
}

func validateChain(raw Raw) error {
	switch raw.SnapshotMode {
	case SnapshotModeFull:
		if raw.Sequence != 1 || raw.BaseSnapshotID != nil || raw.PreviousManifestSHA256 != nil {
			return invalid("full_chain")
		}
	case SnapshotModeDelta:
		return validateDeltaChain(raw)
	default:
		return invalid("snapshot_mode")
	}
	return nil
}

func validateDeltaChain(raw Raw) error {
	if raw.Sequence < 2 || raw.BaseSnapshotID == nil || *raw.BaseSnapshotID == "" {
		return invalid("delta_chain")
	}
	if raw.PreviousManifestSHA256 == nil ||
		!hashPattern.MatchString(*raw.PreviousManifestSHA256) {
		return invalid("previous_manifest_sha256")
	}
	return nil
}

func validateObjectKey(raw Raw) error {
	parts := strings.Split(raw.ObjectKey, "/")
	if len(parts) != 6 {
		return invalid("object_key_layout")
	}
	for _, part := range parts {
		if !safeSegmentPattern.MatchString(part) {
			return invalid("object_key_segment")
		}
	}
	wantPrefix := []string{
		"raw", raw.TenantID, string(raw.SourceType), raw.Competencia, raw.SnapshotID,
	}
	if strings.Join(parts[:5], "/") != strings.Join(wantPrefix, "/") {
		return invalid("object_key_identity")
	}
	if parts[5] != "data.parquet" {
		return invalid("object_key_filename")
	}
	return nil
}

func validSourceType(sourceType SourceType) bool {
	switch sourceType {
	case SourceTypeCNESLocal, SourceTypeCNESNacional, SourceTypeSIHD,
		SourceTypeBPAMag, SourceTypeSIALocal:
		return true
	default:
		return false
	}
}

func rawObjectKey(raw Raw) string {
	return fmt.Sprintf(
		"raw/%s/%s/%s/%s/data.parquet",
		raw.TenantID, raw.SourceType, raw.Competencia, raw.SnapshotID,
	)
}

func invalid(field string) error {
	return fmt.Errorf("field=%s", field)
}
