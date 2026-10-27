package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRebuildSafetyFlagsBeforeDatabaseAccess(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "-tenant"},
		{[]string{"-tenant", "t", "-import", "links.json", "-apply"}, "-quiesced"},
		{[]string{"-tenant", "t", "-import", "links.json", "-relation", "partOf"}, "complete tenant"},
		{[]string{"-tenant", "t", "-models-dir", "models", "-apply"}, "installed DB"},
		{[]string{"-tenant", "t", "-timeout", "0"}, "positive"},
		{[]string{"-tenant", "t", "-things-export", "things.json", "-apply"}, "-quiesced"},
		{[]string{"-tenant", "t", "-things-export", "things.json", "-import", "links.json"}, "either"},
	} {
		var out bytes.Buffer
		if err := run(t.Context(), tc.args, &out); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%v: %v", tc.args, err)
		}
		if out.Len() != 0 {
			t.Fatal("unsafe request produced output")
		}
	}
}
