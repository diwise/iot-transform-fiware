package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/matryer/is"
)

func TestLoadModelsDisabledByDefault(t *testing.T) {
	is := is.New(t)

	rules, err := loadModels(flagMap{dynamicEngine: "false", modelsDir: t.TempDir()})
	is.NoErr(err)
	is.Equal(len(rules), 0)
}

func TestLoadModelsRejectsBrokenConfig(t *testing.T) {
	is := is.New(t)

	dir := t.TempDir()
	is.NoErr(os.WriteFile(filepath.Join(dir, "a.yaml"),
		[]byte("rules:\n  - match: { kind: nope }\n    entities: []\n"), 0o644))

	_, err := loadModels(flagMap{dynamicEngine: "true", modelsDir: dir})
	is.True(err != nil)
}

func TestLoadModelsLoadsValidConfig(t *testing.T) {
	is := is.New(t)

	dir := t.TempDir()
	is.NoErr(os.WriteFile(filepath.Join(dir, "a.yaml"),
		[]byte("rules:\n  - match: { kind: thing }\n    entities:\n      - id: urn:ngsi-ld:Thing:x\n        type: Thing\n        properties: []\n"), 0o644))

	rules, err := loadModels(flagMap{dynamicEngine: "true", modelsDir: dir})
	is.NoErr(err)
	is.Equal(len(rules), 1)
}
