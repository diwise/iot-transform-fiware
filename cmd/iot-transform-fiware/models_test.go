package main

import (
	"testing"

	"github.com/matryer/is"
)

// storageConfigFromFlags är den enda vägen till lagringskonfiguration
// (samma mönster som övriga tjänster).
func TestStorageConfigFromFlags(t *testing.T) {
	is := is.New(t)

	flags := defaultFlags()
	flags[dbHost] = "pg"
	flags[dbUser] = "u"
	flags[dbPassword] = "p@ss:word"
	flags[dbPort] = "5432"
	flags[dbName] = "diwise"
	flags[dbSSLMode] = "disable"

	cfg := storageConfigFromFlags(flags)
	conn := cfg.ConnStr()
	is.True(len(conn) > 0)
}
