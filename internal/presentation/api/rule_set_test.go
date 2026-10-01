package api

import (
	"net/http"
	"strings"
	"testing"
)

func TestTenantAwareModelMutations(t *testing.T) {
	m := pgTestModels(t)
	h := &handler{models: m}
	ctx := fullAccess("a", "b")
	a, b := testRule(), testRule()
	a.Match.Tenant, b.Match.Tenant = "a", "b"
	b.Entities[0].Type = "Different"
	create := func(ruleBody any) Model {
		t.Helper()
		rr := doModelAs(h.createModel, http.MethodPost, "", ruleBody, nil, ctx)
		if rr.Code != http.StatusCreated {
			t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
		}
		return decodeModel(t, rr)
	}
	first, second := create(a), create(b)
	before := m.snap.Load()
	for _, tt := range []struct{ tenant, want string }{{"a", a.Entities[0].Type}, {"b", b.Entities[0].Type}} {
		in := stubThing()
		in.Tenant = tt.tenant
		writes, err := m.Snapshot().Things.Transform(in)
		if err != nil || len(writes) != 1 || writes[0].TypeName != tt.want {
			t.Fatalf("tenant %s: %v, %v", tt.tenant, writes, err)
		}
	}
	b.Match.Tenant = "a"
	rr := doModelAs(h.updateModel, http.MethodPut, second.ID, b, map[string]string{"If-Match": `"rev-1"`}, ctx)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), first.ID) || !strings.Contains(rr.Body.String(), second.ID) {
		t.Fatalf("expected contextual conflict: %d %s", rr.Code, rr.Body.String())
	}
	if m.snap.Load() != before {
		t.Fatal("failed mutation changed active snapshot")
	}
	if err := m.Refresh(t.Context()); err != nil || m.Snapshot().Version != before.Version {
		t.Fatalf("rollback/reload: %v", err)
	}
}

func TestTenantAwareSnapshotRejectsOverlappingCandidate(t *testing.T) {
	store := &stubRuleStore{}
	a, b := testRule(), testRule()
	a.Match.Tenant, b.Match.Tenant = "a", "b"
	b.Entities[0].Type = "Different"
	if _, err := store.InsertRule(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	if _, err := store.InsertRule(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	m := NewModels(store, nil)
	if err := m.Refresh(t.Context()); err != nil {
		t.Fatalf("separate tenants snapshot: %v", err)
	}
	before := m.snap.Load()
	b.Match.Tenant = "a"
	if _, err := store.InsertRule(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	if err := m.Refresh(t.Context()); err == nil {
		t.Fatal("overlapping candidate accepted")
	}
	if m.snap.Load() != before {
		t.Fatal("invalid candidate replaced working snapshot")
	}
}
