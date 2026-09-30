package rules

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
)

// SeedDir läser alla .yaml/.yml-filer i dir i lexikografisk ordning (samma
// ordning som LoadModelsDir) och seedar dem via store.SeedRules med stabil
// nyckel <fil>#<index>. Anropas vid start; idempotent.
func SeedDir(ctx context.Context, s *PgStore, dir string) (SeedResult, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return SeedResult{}, fmt.Errorf("could not read models directory %s: %w", dir, err)
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
	var seed []SeedEntry
	for _, name := range names {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return SeedResult{}, fmt.Errorf("could not read model file %s: %w", name, err)
		}
		rules, err := config.ParseModel(b)
		if err != nil {
			return SeedResult{}, fmt.Errorf("%s: %w", name, err)
		}
		for i, r := range rules {
			seed = append(seed, SeedEntry{
				SeedKey: fmt.Sprintf("%s#%d", name, i),
				Kind:    string(r.Match.Kind),
				Rule:    r,
			})
		}
	}
	if len(seed) == 0 {
		return SeedResult{}, fmt.Errorf("no model rules found in %s", dir)
	}
	return s.SeedRules(ctx, seed)
}
