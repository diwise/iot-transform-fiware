package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/matryer/is"
)

// Okända fält ska ge fel (strikt avkodning).
func TestParseModelRejectsUnknownField(t *testing.T) {
	is := is.New(t)

	_, err := ParseModel([]byte("rules:\n  - match: { kind: measurement }\n    entities: []\n    oops: 1\n"))
	is.True(err != nil)
}

// Filer läses i lexikografisk filnamnsordning och reglerna slås ihop i den
// ordningen.
func TestLoadModelsDirUsesLexicographicOrder(t *testing.T) {
	is := is.New(t)

	dir := t.TempDir()
	model := "rules:\n  - match: { kind: thing, type: %s }\n    entities: []\n"
	is.NoErr(os.WriteFile(filepath.Join(dir, "a-first.yaml"), []byte(fmt.Sprintf(model, "First")), 0o644))
	is.NoErr(os.WriteFile(filepath.Join(dir, "b-second.yaml"), []byte(fmt.Sprintf(model, "Second")), 0o644))
	is.NoErr(os.WriteFile(filepath.Join(dir, "ignored.txt"), []byte("not yaml"), 0o644))

	rules, err := LoadModelsDir(dir)
	is.NoErr(err)
	is.Equal(len(rules), 2)
	is.Equal(rules[0].Match.Type, "First")
	is.Equal(rules[1].Match.Type, "Second")
}
