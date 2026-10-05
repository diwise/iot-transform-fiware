package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/matryer/is"
)

// Saknad eller tom MODELS_DIR ska betyda tom regeluppsättning vid start,
// inte startspärr. Trasiga YAML-filer stoppas fortfarande av SeedDir.
func TestModelsDirEmpty(t *testing.T) {
	is := is.New(t)

	empty, _ := modelsDirEmpty(filepath.Join(t.TempDir(), "missing"))
	is.True(empty)

	dir := t.TempDir()
	empty, _ = modelsDirEmpty(dir)
	is.True(empty)

	is.NoErr(os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("not a model"), 0o644))
	empty, _ = modelsDirEmpty(dir)
	is.True(empty)

	is.NoErr(os.WriteFile(filepath.Join(dir, "10-test.yaml"), []byte("rules: []\n"), 0o644))
	empty, _ = modelsDirEmpty(dir)
	is.True(!empty)
}
