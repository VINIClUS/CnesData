// Package discover auto-detects legacy data source paths on Windows hosts.
package discover

// SourceID identifies a legacy data source family discovered on the host.
type SourceID int

const (
	SourceUnknown SourceID = iota
	SourceCNES
	SourceSIHD
	SourceBPA
	SourceSIA
)

func (s SourceID) String() string {
	switch s {
	case SourceCNES:
		return "cnes"
	case SourceSIHD:
		return "sihd"
	case SourceBPA:
		return "bpa"
	case SourceSIA:
		return "sia"
	default:
		return "unknown"
	}
}

// Strategy names the discovery technique that produced a Candidate.
type Strategy int

const (
	StrategyUnknown Strategy = iota
	StrategyRegistry
	StrategyFSTemplate
	StrategyFSWalk
)

func (s Strategy) String() string {
	switch s {
	case StrategyRegistry:
		return "registry"
	case StrategyFSTemplate:
		return "fs_template"
	case StrategyFSWalk:
		return "fs_walk"
	default:
		return "unknown"
	}
}

// Candidate is a single discovery hit with its scoring metadata.
type Candidate struct {
	Path     string
	Score    int
	Strategy Strategy
}

// ByScoreThenPath sorts candidates by descending score, then ascending path.
type ByScoreThenPath []Candidate

func (b ByScoreThenPath) Len() int      { return len(b) }
func (b ByScoreThenPath) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b ByScoreThenPath) Less(i, j int) bool {
	if b[i].Score != b[j].Score {
		return b[i].Score > b[j].Score
	}
	return b[i].Path < b[j].Path
}

// FBDSN holds Firebird connection parameters serialised in discover YAML.
type FBDSN struct {
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	DatabasePath string `yaml:"database_path"`
	User         string `yaml:"user"`
	Charset      string `yaml:"charset"`
}

// DBFLayout points at a directory of DBF files for the SIA source.
type DBFLayout struct {
	DBFDir string `yaml:"dbf_dir"`
}

// SourceResult bundles the top candidate and alternates for one source.
type SourceResult struct {
	Source     SourceID
	Top        Candidate
	Alternates []Candidate
}

// Config mirrors the on-disk discover YAML produced by the discovery scan.
type Config struct {
	CNES FBDSN     `yaml:"cnes"`
	SIHD FBDSN     `yaml:"sihd"`
	BPA  FBDSN     `yaml:"bpa"`
	SIA  DBFLayout `yaml:"sia"`
}
