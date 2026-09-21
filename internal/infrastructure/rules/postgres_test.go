package rules

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// pgTestRules ansluter mot lokal testdatabas och tömmer transform_rules.
// Utan nåbar databas skippas testerna (husets mönster).
//
// Starta: docker run -d --name pg -e POSTGRES_PASSWORD=password
// -e POSTGRES_DB=diwise -p 5434:5432 postgres:16-alpine
// Avvikande anslutning: PG_TEST_HOST/PORT/DBNAME/USER/PASSWORD/SSLMODE.
func pgTestRules(t *testing.T) *PgStore {
	t.Helper()
	get := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	cfg := NewConfig(
		get("PG_TEST_HOST", "localhost"),
		get("PG_TEST_USER", "postgres"),
		get("PG_TEST_PASSWORD", "password"),
		get("PG_TEST_PORT", "5434"),
		get("PG_TEST_DBNAME", "diwise"),
		get("PG_TEST_SSLMODE", "disable"),
	)
	s, err := NewPostgres(context.Background(), cfg)
	if err != nil {
		t.Skipf("no test database: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.pool.Exec(ctx, `DELETE FROM transform_rules`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
}

func testRule() transform.Rule {
	return transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Type: "room"},
		Entities: []transform.Entity{{
			ID:   "urn:ngsi-ld:Room:{{nameOrID}}",
			Type: "Room",
			Properties: []transform.Property{{
				Target: "name", Type: transform.TypeText, ObservedAt: transform.ObservedAtNone,
				Source: transform.Source{Field: "name"},
			}},
		}},
	}
}

func TestRuleCRUD(t *testing.T) {
	s := pgTestRules(t)

	stored, err := s.InsertRule(testRule())
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if stored.ID == "" || stored.Revision != 1 || stored.Source != SourceAPI {
		t.Fatalf("stored=%+v, want id/rev 1/api", stored)
	}
	if stored.Kind != "thing" {
		t.Fatalf("kind=%q, want thing", stored.Kind)
	}

	got, err := s.GetRule(stored.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Revision != 1 || got.Rule.Match.Type != "room" {
		t.Fatalf("got=%+v", got)
	}

	r := testRule()
	r.Match.Type = "hall"
	updated, err := s.UpdateRule(stored.ID, r, 1)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Revision != 2 || updated.Rule.Match.Type != "hall" {
		t.Fatalf("updated=%+v", updated)
	}

	if _, err := s.UpdateRule(stored.ID, r, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale revision must conflict, got %v", err)
	}
	if _, err := s.UpdateRule("00000000-0000-0000-0000-000000000000", r, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown id must be not found, got %v", err)
	}

	list, err := s.ListRules()
	if err != nil || len(list) != 1 {
		t.Fatalf("list=%v err=%v, want 1", list, err)
	}

	ok, err := s.DeleteRule(stored.ID)
	if err != nil || !ok {
		t.Fatalf("delete=%v err=%v", ok, err)
	}
	ok, err = s.DeleteRule(stored.ID)
	if err != nil || ok {
		t.Fatalf("second delete must be idempotent false, got %v %v", ok, err)
	}
	if _, err := s.GetRule(stored.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete must be not found, got %v", err)
	}
}

func TestRuleSeedIdempotent(t *testing.T) {
	s := pgTestRules(t)
	r := testRule()
	entries := []SeedEntry{
		{SeedKey: "a.yaml#0", Kind: "thing", Rule: r},
		{SeedKey: "b.yaml#0", Kind: "thing", Rule: r},
	}

	res, err := s.SeedRules(entries)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if res.Added != 2 || res.Updated != 0 || res.Skipped != 0 {
		t.Fatalf("seed=%+v, want 2 added", res)
	}

	res, err = s.SeedRules(entries)
	if err != nil {
		t.Fatalf("reseed: %v", err)
	}
	if res.Added != 0 || res.Updated != 0 || res.Skipped != 2 {
		t.Fatalf("reseed=%+v, want 2 skipped", res)
	}

	// Ändrad seed-fil uppdaterar seed-raden.
	r2 := testRule()
	r2.Match.Type = "hall"
	res, err = s.SeedRules([]SeedEntry{{SeedKey: "a.yaml#0", Kind: "thing", Rule: r2}})
	if err != nil {
		t.Fatalf("reseed edited: %v", err)
	}
	if res.Updated != 1 || res.Skipped != 0 {
		t.Fatalf("reseed edited=%+v, want 1 updated", res)
	}

	// API-ändring skyddar raden mot framtida seed.
	list, err := s.ListRules()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var target StoredRule
	for _, st := range list {
		if st.SeedKey == "a.yaml#0" {
			target = st
		}
	}
	if target.ID == "" {
		t.Fatal("seeded rule missing")
	}
	r3 := testRule()
	r3.Match.Type = "api-edited"
	if _, err := s.UpdateRule(target.ID, r3, target.Revision); err != nil {
		t.Fatalf("api update: %v", err)
	}
	res, err = s.SeedRules([]SeedEntry{{SeedKey: "a.yaml#0", Kind: "thing", Rule: r2}})
	if err != nil {
		t.Fatalf("reseed after api edit: %v", err)
	}
	if res.Skipped != 1 || res.Updated != 0 {
		t.Fatalf("reseed after api edit=%+v, want 1 skipped", res)
	}
	got, err := s.GetRule(target.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Rule.Match.Type != "api-edited" {
		t.Fatalf("api edit must survive reseed: %+v", got.Rule.Match)
	}
}

func TestRuleSeedDirShippedModels(t *testing.T) {
	s := pgTestRules(t)
	res, err := SeedDir(s, "../../../deployments/models")
	if err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if res.Added == 0 {
		t.Fatal("shipped models must seed rows")
	}
	list, err := s.ListRules()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != res.Added {
		t.Fatalf("listed=%d, added=%d", len(list), res.Added)
	}
	seen := map[string]bool{}
	for _, st := range list {
		if st.ID == "" || st.Revision != 1 || st.Source != SourceSeed || st.SeedKey == "" {
			t.Fatalf("bad row: %+v", st)
		}
		if seen[st.SeedKey] {
			t.Fatalf("duplicate seed key %q", st.SeedKey)
		}
		seen[st.SeedKey] = true
	}
	res2, err := SeedDir(s, "../../../deployments/models")
	if err != nil {
		t.Fatalf("reseed dir: %v", err)
	}
	if res2.Added != 0 || res2.Updated != 0 || res2.Skipped != res.Added {
		t.Fatalf("reseed=%+v, want all skipped", res2)
	}
}

// tieRule ger två överlappande regler med identisk matchning (lika poäng)
// som bara skiljer på entitets-ID: motorn ska välja den första listade.
func tieRule(id string) transform.Rule {
	return transform.Rule{
		Match: transform.Match{Kind: transform.KindMeasurement},
		Entities: []transform.Entity{{
			ID:   id,
			Type: "Thing",
			Properties: []transform.Property{{
				Target: "tag", Type: transform.TypeText, ObservedAt: transform.ObservedAtNone,
				Source: transform.Source{Const: "x"},
			}},
		}},
	}
}

func tieInput() transform.Measurement {
	return transform.Measurement{
		DeviceID:  "d",
		Tenant:    "t",
		Timestamp: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}
}

func listedIDs(t *testing.T, s *PgStore) []string {
	t.Helper()
	list, err := s.ListRules()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	ids := make([]string, 0, len(list))
	for i, st := range list {
		if st.Position != int64(i) {
			t.Fatalf("position=%d at index %d, want dense 0..N", st.Position, i)
		}
		ids = append(ids, st.Rule.Entities[0].ID)
	}
	return ids
}

// TF-02: seed-ordning definierar precedens vid lika poäng — genom seed,
// omladdning (ListRules) och omstart (ny anslutning).
func TestSeedOrderDefinesPrecedence(t *testing.T) {
	s := pgTestRules(t)
	a := tieRule("urn:ngsi-ld:Thing:first")
	b := tieRule("urn:ngsi-ld:Thing:second")

	res, err := s.SeedRules([]SeedEntry{
		{SeedKey: "a.yaml#0", Kind: "measurement", Rule: a},
		{SeedKey: "b.yaml#0", Kind: "measurement", Rule: b},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if res.Added != 2 {
		t.Fatalf("seed=%+v, want 2 added", res)
	}
	if got := listedIDs(t, s); len(got) != 2 || got[0] != "urn:ngsi-ld:Thing:first" || got[1] != "urn:ngsi-ld:Thing:second" {
		t.Fatalf("order=%v, want [first second]", got)
	}

	// Omladdning: motorn byggd från ListRules väljer första regeln.
	list, err := s.ListRules()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	rs := make([]transform.Rule, 0, len(list))
	for _, st := range list {
		rs = append(rs, st.Rule)
	}
	writes, err := transform.NewEngine(rs).Transform(tieInput())
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if len(writes) != 1 || writes[0].EntityID != "urn:ngsi-ld:Thing:first" {
		t.Fatalf("writes=%+v, want first rule to win", writes)
	}

	// Omstart: ny anslutning mot samma DB ger samma ordning.
	s2 := pgTestRulesNoTruncate(t)
	defer s2.Close()
	if got := listedIDs(t, s2); len(got) != 2 || got[0] != "urn:ngsi-ld:Thing:first" || got[1] != "urn:ngsi-ld:Thing:second" {
		t.Fatalf("order after reopen=%v, want [first second]", got)
	}
}

// pgTestRulesNoTruncate öppnar lagret utan att tömma (för omstartstest).
func pgTestRulesNoTruncate(t *testing.T) *PgStore {
	t.Helper()
	get := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	cfg := NewConfig(
		get("PG_TEST_HOST", "localhost"),
		get("PG_TEST_USER", "postgres"),
		get("PG_TEST_PASSWORD", "password"),
		get("PG_TEST_PORT", "5434"),
		get("PG_TEST_DBNAME", "diwise"),
		get("PG_TEST_SSLMODE", "disable"),
	)
	s, err := NewPostgres(context.Background(), cfg)
	if err != nil {
		t.Skipf("no test database: %v", err)
	}
	return s
}

// TF-02: API-skapade regler hamnar sist i skapandeordning; uppdatering
// flyttar aldrig regeln.
func TestAPIAppendAfterSeed(t *testing.T) {
	s := pgTestRules(t)
	a := tieRule("urn:ngsi-ld:Thing:first")
	b := tieRule("urn:ngsi-ld:Thing:second")
	if _, err := s.SeedRules([]SeedEntry{
		{SeedKey: "a.yaml#0", Kind: "measurement", Rule: a},
		{SeedKey: "b.yaml#0", Kind: "measurement", Rule: b},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	c, err := s.InsertRule(tieRule("urn:ngsi-ld:Thing:third"))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := listedIDs(t, s); len(got) != 3 || got[2] != "urn:ngsi-ld:Thing:third" {
		t.Fatalf("order=%v, want third last", got)
	}

	d := tieRule("urn:ngsi-ld:Thing:third-v2")
	updated, err := s.UpdateRule(c.ID, d, c.Revision)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := listedIDs(t, s); len(got) != 3 || got[2] != "urn:ngsi-ld:Thing:third-v2" {
		t.Fatalf("order=%v, update must not reorder", got)
	}
	if updated.Revision != c.Revision+1 {
		t.Fatalf("revision=%d, want %d", updated.Revision, c.Revision+1)
	}
}

// TF-02: infogad seed-fil i mitten skiftar efterföljande positioner (med
// revision-bump, eftersom ordning är en del av konfigurationen).
func TestSeedShiftReorders(t *testing.T) {
	s := pgTestRules(t)
	a := tieRule("urn:ngsi-ld:Thing:first")
	b := tieRule("urn:ngsi-ld:Thing:second")
	if _, err := s.SeedRules([]SeedEntry{
		{SeedKey: "a.yaml#0", Kind: "measurement", Rule: a},
		{SeedKey: "b.yaml#0", Kind: "measurement", Rule: b},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	z := tieRule("urn:ngsi-ld:Thing:zeroth")
	res, err := s.SeedRules([]SeedEntry{
		{SeedKey: "0.yaml#0", Kind: "measurement", Rule: z},
		{SeedKey: "a.yaml#0", Kind: "measurement", Rule: a},
		{SeedKey: "b.yaml#0", Kind: "measurement", Rule: b},
	})
	if err != nil {
		t.Fatalf("reseed: %v", err)
	}
	if res.Added != 1 || res.Updated != 2 || res.Skipped != 0 {
		t.Fatalf("reseed=%+v, want 1 added 2 updated", res)
	}
	if got := listedIDs(t, s); len(got) != 3 ||
		got[0] != "urn:ngsi-ld:Thing:zeroth" ||
		got[1] != "urn:ngsi-ld:Thing:first" ||
		got[2] != "urn:ngsi-ld:Thing:second" {
		t.Fatalf("order=%v, want [zeroth first second]", got)
	}
}
