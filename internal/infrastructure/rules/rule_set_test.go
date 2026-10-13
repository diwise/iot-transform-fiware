package rules

import (
	"errors"
	"strings"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

func TestTenantAwareCandidateMutations(t *testing.T) {
	s := pgTestRules(t)
	a, b := testRule(), testRule()
	a.Match.Tenant, b.Match.Tenant = "a", "b"
	b.Entities[0].Type = "Different"
	first, err := s.InsertRule(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.InsertRule(t.Context(), b)
	if err != nil {
		t.Fatalf("separate tenant insert: %v", err)
	}
	b.Match.Tenant = "a"
	if _, err := s.UpdateRule(t.Context(), second.ID, b, second.Revision); !errors.Is(err, ErrInvalidRule) || !strings.Contains(err.Error(), first.ID) || !strings.Contains(err.Error(), second.ID) {
		t.Fatalf("overlapping tenant update must fail with both rule IDs: %v", err)
	}
	unchanged, err := s.GetRule(t.Context(), second.ID)
	if err != nil || unchanged.Revision != second.Revision || unchanged.Rule.Match.Tenant != "b" {
		t.Fatalf("failed update changed row: %+v, %v", unchanged, err)
	}
	if version, err := s.RuleVersion(t.Context()); err != nil || version != 2 {
		t.Fatalf("failed update bumped version: %d, %v", version, err)
	}

	seed := testRule()
	seed.Match.Tenant = "c"
	seed.Entities[0].Type = "Third"
	entries := []SeedEntry{{SeedKey: "tenant#0", Rule: seed}}
	if _, err := s.SeedRules(t.Context(), entries); err != nil {
		t.Fatalf("separate tenant seed: %v", err)
	}
	before, version, err := s.LoadRules(t.Context())
	if err != nil || len(before) != 3 || version != 3 {
		t.Fatalf("seed snapshot: %v %d %v", before, version, err)
	}
	entries[0].Rule.Match.Tenant = "a"
	if _, err := s.SeedRules(t.Context(), entries); !errors.Is(err, ErrInvalidRule) {
		t.Fatalf("conflicting seed update: %v", err)
	}
	after, afterVersion, err := s.LoadRules(t.Context())
	if err != nil || len(after) != len(before) || afterVersion != version {
		t.Fatalf("seed rollback changed snapshot: %v %d %v", after, afterVersion, err)
	}
	for i := range before {
		if after[i].ID != before[i].ID || after[i].Revision != before[i].Revision || after[i].Rule.Match.Tenant != before[i].Rule.Match.Tenant {
			t.Fatalf("seed rollback changed row %d", i)
		}
	}
	// Same tenant but disjoint source matches are also valid candidates.
	disjoint := testRule()
	disjoint.Match.Tenant, disjoint.Match.Type = "a", "OtherSource"
	disjoint.Entities[0].Type = "Fourth"
	if _, err := s.InsertRule(t.Context(), disjoint); err != nil {
		t.Fatalf("disjoint match insert: %v", err)
	}
	if _, err := s.InsertRule(t.Context(), a); err != nil {
		t.Fatalf("same type remains valid: %v", err)
	}
}

func TestTenantAwareConcurrentCandidates(t *testing.T) {
	s := pgTestRules(t)
	a, b := testRule(), testRule()
	a.Match.Tenant, b.Match.Tenant = "a", "b"
	b.Entities[0].Type = "Different"
	first, err := s.InsertRule(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.InsertRule(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	a.Match.Tenant, b.Match.Tenant = "shared", "shared"
	start, done := make(chan struct{}), make(chan error, 2)
	for _, update := range []struct {
		stored StoredRule
		rule   transform.Rule
	}{{first, a}, {second, b}} {
		go func() {
			<-start
			_, err := s.UpdateRule(t.Context(), update.stored.ID, update.rule, update.stored.Revision)
			done <- err
		}()
	}
	close(start)
	success, invalid := 0, 0
	for _, err := range []error{<-done, <-done} {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrInvalidRule):
			invalid++
		default:
			t.Fatalf("unexpected concurrent result: %v", err)
		}
	}
	if success != 1 || invalid != 1 {
		t.Fatalf("expected one winner and one invalid candidate, got %d/%d", success, invalid)
	}
	stored, version, err := s.LoadRules(t.Context())
	if err != nil || len(stored) != 2 || version != 3 {
		t.Fatalf("concurrent snapshot: %v %d %v", stored, version, err)
	}
	if err := transform.Validate([]transform.Rule{stored[0].Rule, stored[1].Rule}); err != nil {
		t.Fatalf("committed inconsistent candidate: %v", err)
	}
}
