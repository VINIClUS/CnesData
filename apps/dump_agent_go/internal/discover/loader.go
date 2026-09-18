package discover

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ErrNoYAML signals the config file does not exist (caller falls back).
var ErrNoYAML = errors.New("yaml_absent")

// LoadYAML reads + strict-parses the discover YAML.
// Unknown top-level keys → error. Missing file → ErrNoYAML.
func LoadYAML(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Config{}, ErrNoYAML
		}
		return Config{}, fmt.Errorf("yaml_read: %w", err)
	}
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("yaml_invalid: %w", err)
	}
	return cfg, nil
}
