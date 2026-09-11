// Package config laddar och validerar de konfigurationsstyrda modellreglerna
// från en katalog med YAML-filer.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"gopkg.in/yaml.v3"
)

// LoadModelsDir läser alla .yaml/.yml-filer i dir i lexikografisk
// filnamnsordning och slår ihop deras regler till en ordnad lista. Ordningen
// definierar precedens inom samma prioritetsnivå.
func LoadModelsDir(dir string) ([]transform.Rule, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("could not read models directory %s: %w", dir, err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		switch strings.ToLower(filepath.Ext(e.Name())) {
		case ".yaml", ".yml":
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	var rules []transform.Rule
	for _, name := range names {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("could not read model file %s: %w", name, err)
		}

		r, err := ParseModel(b)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}

		rules = append(rules, r...)
	}

	return rules, nil
}

// ParseModel avkodar en YAML-fil strikt (okända fält ger fel) och returnerar
// dess regler.
func ParseModel(b []byte) ([]transform.Rule, error) {
	var m transform.Model

	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)

	if err := dec.Decode(&m); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, fmt.Errorf("could not parse model: %w", err)
	}

	return m.Rules, nil
}
