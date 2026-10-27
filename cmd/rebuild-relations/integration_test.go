package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/projection"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/rules"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestRebuildCommandDryRunApplyAndAuthoritativeImport(t *testing.T) {
	get := func(k, fallback string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return fallback
	}
	host, user, password, port := get("PG_TEST_HOST", "127.0.0.1"), get("PG_TEST_USER", "postgres"), get("PG_TEST_PASSWORD", "password"), get("PG_TEST_PORT", "5434")
	admin, err := pgx.Connect(t.Context(), rules.NewConfig(host, user, password, port, "postgres", "disable").ConnStr())
	if err != nil {
		t.Skipf("no isolated test Postgres: %v", err)
	}
	name := "diwise_test_rebuild_" + uuid.NewString()[:8]
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer cancel()
		_, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize())
		if err != nil {
			t.Errorf("drop isolated DB: %v", err)
		}
		_ = admin.Close(ctx)
	})
	store, err := rules.NewPostgres(t.Context(), rules.NewConfig(host, user, password, port, name, "disable"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	rs, err := config.LoadModelsDir("../../deployments/examples/tank-greenhouse")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rs {
		if _, err := store.InsertRule(t.Context(), r); err != nil {
			t.Fatal(err)
		}
	}
	for key, value := range map[string]string{"POSTGRES_HOST": host, "POSTGRES_USER": user, "POSTGRES_PASSWORD": password, "POSTGRES_PORT": port, "POSTGRES_DBNAME": name, "POSTGRES_SSLMODE": "disable"} {
		t.Setenv(key, value)
	}
	links := filepath.Join("..", "..", "deployments", "examples", "tank-greenhouse", "links.json")
	projected := projection.NewForPool(store.Pool())
	invoke := func(args ...string) map[string]any {
		t.Helper()
		var out bytes.Buffer
		if err := run(t.Context(), args, &out); err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		if err := json.Unmarshal(out.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	preview := invoke("-tenant", "example", "-import", links)
	if preview["plannedWrites"] != float64(5) || preview["applied"] != false {
		t.Fatal(preview)
	}
	if n, err := projected.PendingCount(t.Context(), "example"); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if got, err := projected.ExportTenant(t.Context(), "example"); err != nil || len(got) != 0 {
		t.Fatal(got, err)
	}
	applied := invoke("-tenant", "example", "-import", links, "-quiesced", "-apply")
	if applied["enqueuedWrites"] != float64(5) || applied["applied"] != true {
		t.Fatal(applied)
	}
	if got, err := projected.ExportTenant(t.Context(), "example"); err != nil || len(got) != 4 {
		t.Fatal(got, err)
	}
	rebuild := invoke("-tenant", "example", "-apply")
	if rebuild["enqueuedWrites"] != float64(1) {
		t.Fatal(rebuild)
	}
	// Destructive empty import needs a second explicit confirmation.
	empty := filepath.Join(t.TempDir(), "empty.json")
	if err := os.WriteFile(empty, []byte("[]"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(t.Context(), []string{"-tenant", "example", "-import", empty, "-quiesced", "-apply"}, &bytes.Buffer{}); err == nil {
		t.Fatal("empty import was not guarded")
	}
	cleared := invoke("-tenant", "example", "-import", empty, "-quiesced", "-allow-empty", "-apply")
	if cleared["enqueuedWrites"] != float64(5) {
		t.Fatal(cleared)
	}
	if got, err := projected.ExportTenant(t.Context(), "example"); err != nil || len(got) != 0 {
		t.Fatal(fmt.Sprint(got), err)
	}
}
