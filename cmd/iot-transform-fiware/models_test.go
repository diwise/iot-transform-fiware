package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/matryer/is"
)

func TestLoadModelsRejectsBrokenConfig(t *testing.T) {
	is := is.New(t)

	dir := t.TempDir()
	is.NoErr(os.WriteFile(filepath.Join(dir, "a.yaml"),
		[]byte("rules:\n  - match: { kind: nope }\n    entities: []\n"), 0o644))

	_, err := loadModels(flagMap{modelsDir: dir})
	is.True(err != nil)
}

func TestLoadModelsMissingDirFails(t *testing.T) {
	is := is.New(t)

	_, err := loadModels(flagMap{modelsDir: filepath.Join(t.TempDir(), "does-not-exist")})
	is.True(err != nil)
}

func TestLoadModelsLoadsValidConfig(t *testing.T) {
	is := is.New(t)

	dir := t.TempDir()
	is.NoErr(os.WriteFile(filepath.Join(dir, "a.yaml"),
		[]byte("rules:\n  - match: { kind: thing }\n    entities:\n      - id: urn:ngsi-ld:Thing:x\n        type: Thing\n        properties: []\n"), 0o644))

	rules, err := loadModels(flagMap{modelsDir: dir})
	is.NoErr(err)
	is.Equal(len(rules), 1)
}
