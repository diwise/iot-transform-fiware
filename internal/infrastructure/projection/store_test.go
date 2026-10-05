package projection

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// testDBName isolerar paketets tester i egen databas: go test kör paket
// parallellt och delad tabell ger interferens (samma mönster som
// regellagret). Kan överstyras med PG_TEST_DBNAME.
const testDBName = "diwise_test_projection"

func ensureTestDB(t *testing.T, cfg Config) {
	t.Helper()
	admin := cfg
	admin.dbname = "postgres"
	conn, err := pgx.Connect(context.Background(), admin.ConnStr())
	if err != nil {
		t.Skipf("no test database server: %v", err)
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), fmt.Sprintf(`CREATE DATABASE %s`, pgx.Identifier{cfg.dbname}.Sanitize()))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P04" {
			return
		}
		t.Skipf("cannot create test database: %v", err)
	}
}

func pgTestConfig(t *testing.T) Config {
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
		get("PG_TEST_DBNAME", testDBName),
		get("PG_TEST_SSLMODE", "disable"),
	)
	ensureTestDB(t, cfg)
	return cfg
}

// pgTestStore öppnar lagret mot isolerad testdatabas och tömmer
// projektionstabellerna (inte regellagrets 005-sökväg — här verifieras
// lagrets egen EnsureSchema).
func pgTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewPostgres(context.Background(), pgTestConfig(t))
	if err != nil {
		t.Skipf("no test database: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, table := range []string{"pending_broker_work", "projection_seen_changes", "source_links", "source_link_specs"} {
		if _, err := s.pool.Exec(ctx, `DELETE FROM `+table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
	return s
}

// pgTestStoreNoTruncate öppnar lagret utan att tömma (omstartstest).
func pgTestStoreNoTruncate(t *testing.T) *Store {
	t.Helper()
	s, err := NewPostgres(context.Background(), pgTestConfig(t))
	if err != nil {
		t.Skipf("no test database: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// recordingPlanner är en ren testplanerare: registrerar snapshoten och
// returnerar ett deterministiskt arbete per länk.
type recordingPlanner struct {
	mu        sync.Mutex
	snapshots []relations.Snapshot
	writes    func(relations.Snapshot) []relations.PendingWrite
	fail      error
}

func (p *recordingPlanner) plan() relations.PlanFunc {
	return func(snap relations.Snapshot) ([]relations.PendingWrite, error) {
		p.mu.Lock()
		p.snapshots = append(p.snapshots, snap)
		p.mu.Unlock()
		if p.fail != nil {
			return nil, p.fail
		}
		if p.writes != nil {
			return p.writes(snap), nil
		}
		var out []relations.PendingWrite
		for _, l := range snap.Outgoing {
			out = append(out, relations.PendingWrite{
				IdempotencyKey: "set:" + l.FromID + ":" + l.ToID,
				Operation:      relations.OpMerge,
				EntityID:       "entity:" + l.FromID,
				Payload:        []byte(`{"to":"` + l.ToID + `"}`),
			})
		}
		for _, l := range snap.Removed {
			out = append(out, relations.PendingWrite{
				IdempotencyKey: "removed:" + l.FromID + ":" + l.ToID,
				Operation:      relations.OpRemoveAttributes,
				EntityID:       "entity:" + l.FromID,
				Payload:        []byte(`{"removed":"` + l.ToID + `"}`),
			})
		}
		if snap.EndpointDeleted != "" {
			out = append(out, relations.PendingWrite{
				IdempotencyKey: "endpoint-deleted:" + snap.EndpointDeleted,
				Operation:      relations.OpDelete,
				EntityID:       "entity:" + snap.EndpointDeleted,
				Payload:        []byte(`{"lifecycle":"deleted"}`),
			})
		}
		return out, nil
	}
}

func (p *recordingPlanner) last() relations.Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.snapshots) == 0 {
		return relations.Snapshot{}
	}
	return p.snapshots[len(p.snapshots)-1]
}

func setChange(tenant, from, to string, rev int64) relations.Change {
	return relations.Change{
		RelationID: relations.BuildRelationID(tenant, from, "contains", to, rev, false),
		Tenant:     tenant, Relation: "contains", FromID: from, ToID: to,
		Revision: rev, ChangedAt: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC),
		FromKind: "kind-a", FromKindVersion: "1", ToKind: "kind-b", ToKindVersion: "2",
	}
}

func mustApply(t *testing.T, s *Store, c relations.Change, trace map[string]string, planner relations.PlanFunc) Result {
	t.Helper()
	res, err := s.ApplyChange(t.Context(), c, trace, planner)
	if err != nil {
		t.Fatalf("apply %+v: %v", c, err)
	}
	return res
}

func outgoingToIDs(t *testing.T, s *Store, tenant, relation, from string) []string {
	t.Helper()
	links, err := s.Outgoing(t.Context(), tenant, relation, from)
	if err != nil {
		t.Fatalf("outgoing: %v", err)
	}
	var out []string
	for _, l := range links {
		out = append(out, l.ToID)
	}
	return out
}

func TestProjectionSetRemoveLastLink(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{}

	// Flera mål: komplett utgående mängd efter varje set.
	mustApply(t, s, setChange("tenant-a", "from-1", "to-1", 1), nil, planner.plan())
	mustApply(t, s, setChange("tenant-a", "from-1", "to-2", 2), nil, planner.plan())
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "from-1"); len(got) != 2 || got[0] != "to-1" || got[1] != "to-2" {
		t.Fatalf("outgoing=%v, want [to-1 to-2]", got)
	}
	// Omvänd mängd per mål är komplett (båda sidor läsbara).
	rev, err := s.Reverse(t.Context(), "tenant-a", "contains", "to-1")
	if err != nil || len(rev) != 1 || rev[0].FromID != "from-1" {
		t.Fatalf("reverse=%+v err=%v", rev, err)
	}
	// Planeraren såg kompletta mängder, inte enstaka länk.
	snap := planner.last()
	if len(snap.Outgoing) != 2 || len(snap.ReverseNew) != 2 {
		t.Fatalf("planner snapshot incomplete: %+v", snap)
	}

	// Remove av ett mål lämnar resten; sista länken flaggas.
	mustApply(t, s, relations.Change{
		RelationID: relations.BuildRelationID("tenant-a", "from-1", "contains", "to-1", 3, true),
		Tenant:     "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1",
		Removed: true, Revision: 3,
	}, nil, planner.plan())
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "from-1"); len(got) != 1 || got[0] != "to-2" {
		t.Fatalf("after remove outgoing=%v", got)
	}
	snap = planner.last()
	if len(snap.Removed) != 1 {
		t.Fatalf("removed not reported: %+v", snap)
	}
	if _, ok := snap.ReversePrevious["to-1"]; !ok {
		t.Fatalf("previous reverse set missing: %+v", snap.ReversePrevious)
	}
	if snap.IsLastLink {
		t.Fatal("one link remains, must not be last link")
	}

	res := mustApply(t, s, relations.Change{
		RelationID: relations.BuildRelationID("tenant-a", "from-1", "contains", "to-2", 4, true),
		Tenant:     "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-2",
		Removed: true, Revision: 4,
	}, nil, planner.plan())
	if !res.IsLastLink || len(res.Outgoing) != 0 {
		t.Fatalf("last link not flagged: %+v", res)
	}
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "from-1"); len(got) != 0 {
		t.Fatalf("want empty outgoing, got %v", got)
	}
	rev, err = s.Reverse(t.Context(), "tenant-a", "contains", "to-2")
	if err != nil || len(rev) != 0 {
		t.Fatalf("reverse after last remove=%+v err=%v", rev, err)
	}
}

func TestProjectionMoveAtomicBothSides(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{}

	mustApply(t, s, setChange("tenant-a", "from-1", "to-1", 1), nil, planner.plan())
	move := setChange("tenant-a", "from-1", "to-2", 2)
	move.PreviousToID = "to-1"
	move.RelationID = "v1/tenant-a/from-1/relations/contains/2/set"
	mustApply(t, s, move, nil, planner.plan())

	if got := outgoingToIDs(t, s, "tenant-a", "contains", "from-1"); len(got) != 1 || got[0] != "to-2" {
		t.Fatalf("after move outgoing=%v", got)
	}
	snap := planner.last()
	if len(snap.Outgoing) != 1 || len(snap.Removed) != 1 {
		t.Fatalf("move snapshot must carry new outgoing + removed: %+v", snap)
	}
	if _, ok := snap.ReverseNew["to-2"]; !ok {
		t.Fatalf("new reverse set missing: %+v", snap.ReverseNew)
	}
	if _, ok := snap.ReversePrevious["to-1"]; !ok {
		t.Fatalf("previous reverse set missing: %+v", snap.ReversePrevious)
	}
	rev, _ := s.Reverse(t.Context(), "tenant-a", "contains", "to-1")
	if len(rev) != 0 {
		t.Fatalf("previous target must be empty: %+v", rev)
	}

	// Upprepad flytt med ny identitet till samma mål: idempotent upsert.
	again := move
	again.RelationID = "v1/tenant-a/from-1/relations/contains/3/set"
	again.Revision = 3
	mustApply(t, s, again, nil, planner.plan())
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "from-1"); len(got) != 1 {
		t.Fatalf("re-move changed set: %v", got)
	}
}

func TestProjectionDuplicateIsIdempotent(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{}

	first := mustApply(t, s, setChange("tenant-a", "from-1", "to-1", 1), nil, planner.plan())
	if first.Duplicate || first.Pending != 1 {
		t.Fatalf("first apply=%+v, want 1 pending", first)
	}
	second := mustApply(t, s, setChange("tenant-a", "from-1", "to-1", 1), nil, planner.plan())
	if !second.Duplicate || second.Pending != 0 {
		t.Fatalf("replay=%+v, want duplicate without new work", second)
	}
	count, err := s.PendingCount(t.Context(), "tenant-a")
	if err != nil || count != 1 {
		t.Fatalf("pending=%d err=%v, want exactly 1", count, err)
	}
}

func TestProjectionNoRecencyFiltering(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{}

	// Nyare revision först, äldre sedan: båda appliceras (ingen
	// recency-filtrering). Sent event återställer äldre mängdstate —
	// dokumenterad avgränsning, inte ett fel.
	mustApply(t, s, setChange("tenant-a", "from-1", "to-new", 5), nil, planner.plan())
	late := setChange("tenant-a", "from-1", "to-old", 3)
	mustApply(t, s, late, nil, planner.plan())
	got := outgoingToIDs(t, s, "tenant-a", "contains", "from-1")
	if len(got) != 2 {
		t.Fatalf("late event was filtered: outgoing=%v", got)
	}
}

func TestProjectionTenantIsolation(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{}

	mustApply(t, s, setChange("tenant-a", "from-1", "to-1", 1), nil, planner.plan())
	mustApply(t, s, setChange("tenant-b", "from-1", "to-1", 1), nil, planner.plan())
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "from-1"); len(got) != 1 {
		t.Fatalf("tenant-a=%v", got)
	}
	if got := outgoingToIDs(t, s, "tenant-b", "contains", "from-1"); len(got) != 1 {
		t.Fatalf("tenant-b=%v", got)
	}
	rev, _ := s.Reverse(t.Context(), "tenant-a", "contains", "to-1")
	if len(rev) != 1 || rev[0].Tenant != "tenant-a" {
		t.Fatalf("reverse leaks across tenants: %+v", rev)
	}
	// Länk med avvikande mål-tenant nekas.
	bad := setChange("tenant-a", "from-1", "to-2", 2)
	bad.ToTenant = "tenant-b"
	if _, err := s.ApplyChange(t.Context(), bad, nil, planner.plan()); !errors.Is(err, ErrConflict) {
		t.Fatalf("cross-tenant link must conflict, got %v", err)
	}
}

func TestProjectionCardinalityAndCycle(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{}

	if err := s.SetSpec(t.Context(), relations.Spec{Name: "contains", AllowedTargets: []string{"kind-b"}, MaxLinks: 1}); err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, setChange("tenant-a", "from-1", "to-1", 1), nil, planner.plan())
	second := setChange("tenant-a", "from-1", "to-2", 2)
	if _, err := s.ApplyChange(t.Context(), second, nil, planner.plan()); !errors.Is(err, ErrConflict) {
		t.Fatalf("max exceeded must conflict, got %v", err)
	}
	// Duplikat passerar trots max (idempotent upsert).
	mustApply(t, s, setChange("tenant-a", "from-1", "to-1", 1), nil, planner.plan())
	// Otillåten måltyp nekas.
	other := setChange("tenant-a", "from-2", "to-9", 1)
	other.ToKind = "kind-c"
	if _, err := s.ApplyChange(t.Context(), other, nil, planner.plan()); !errors.Is(err, ErrConflict) {
		t.Fatalf("disallowed target must conflict, got %v", err)
	}

	// Hierarki: cykel nekas (a->b, b->a).
	a := relations.Change{
		RelationID: "v1/tenant-a/a/relations/partOf/1/set",
		Tenant:     "tenant-a", Relation: "partOf", FromID: "a", ToID: "b", Revision: 1,
		FromKind: "kind-a", ToKind: "kind-a",
	}
	if _, err := s.ApplyChange(t.Context(), a, nil, planner.plan()); err != nil {
		t.Fatalf("first hierarchy link: %v", err)
	}
	b := relations.Change{
		RelationID: "v1/tenant-a/b/relations/partOf/1/set",
		Tenant:     "tenant-a", Relation: "partOf", FromID: "b", ToID: "a", Revision: 1,
		FromKind: "kind-a", ToKind: "kind-a",
	}
	if _, err := s.ApplyChange(t.Context(), b, nil, planner.plan()); !errors.Is(err, ErrConflict) {
		t.Fatalf("cycle must conflict, got %v", err)
	}
	// Hierarki max-en: andra föräldern nekas utan flytt.
	c := relations.Change{
		RelationID: "v1/tenant-a/a/relations/partOf/2/set",
		Tenant:     "tenant-a", Relation: "partOf", FromID: "a", ToID: "c", Revision: 2,
		FromKind: "kind-a", ToKind: "kind-a",
	}
	if _, err := s.ApplyChange(t.Context(), c, nil, planner.plan()); !errors.Is(err, ErrConflict) {
		t.Fatalf("second parent must conflict, got %v", err)
	}
}

func TestProjectionPlannerErrorRollsBack(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{fail: errors.New("planner exploded")}
	if _, err := s.ApplyChange(t.Context(), setChange("tenant-a", "from-1", "to-1", 1), nil, planner.plan()); err == nil {
		t.Fatal("planner error must propagate")
	}
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "from-1"); len(got) != 0 {
		t.Fatalf("projection must roll back: %v", got)
	}
	count, _ := s.PendingCount(t.Context(), "tenant-a")
	if count != 0 {
		t.Fatalf("pending must roll back: %d", count)
	}
	// Samma identitet kan appliceras igen efter rollback (ingen seen-rad).
	planner.fail = nil
	mustApply(t, s, setChange("tenant-a", "from-1", "to-1", 1), nil, planner.plan())
}

func TestProjectionCancellation(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{}

	canceled, stop := context.WithCancel(t.Context())
	stop()
	checks := map[string]func() error{
		"apply": func() error {
			_, err := s.ApplyChange(canceled, setChange("t", "f", "o", 1), nil, planner.plan())
			return err
		},
		"out":     func() error { _, err := s.Outgoing(canceled, "t", "contains", "f"); return err },
		"reverse": func() error { _, err := s.Reverse(canceled, "t", "contains", "o"); return err },
		"export":  func() error { _, err := s.ExportTenant(canceled, "t"); return err },
		"claim":   func() error { _, err := s.ClaimPending(canceled, "", 1, time.Minute); return err },
		"spec":    func() error { return s.SetSpec(canceled, relations.Spec{Name: "x"}) },
		"prune":   func() error { _, err := s.BootstrapPrune(canceled, "t", nil, time.Now()); return err },
		"rebuild": func() error { _, err := s.Rebuild(canceled, "t", "", nil, planner.plan(), 0); return err },
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			if err := check(); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
		})
	}
	if got := outgoingToIDs(t, s, "t", "contains", "f"); len(got) != 0 {
		t.Fatalf("canceled apply mutated state: %v", got)
	}
}

func TestProjectionCloseIdempotent(t *testing.T) {
	s := pgTestStore(t)
	s.Close()
	s.Close()
	if !s.IsClosed() {
		t.Fatal("must report closed")
	}
	if _, err := s.ApplyChange(t.Context(), setChange("t", "f", "o", 1), nil, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ops after close must fail, got %v", err)
	}
	var nilStore *Store
	nilStore.Close()
	if !nilStore.IsClosed() {
		t.Fatal("nil store must report closed")
	}
}

func TestProjectionConcurrentWorkers(t *testing.T) {
	s := pgTestStore(t)

	// 8 workers x 25 unika länkar + dubblettleveranser av samma identitet.
	const workers = 8
	const links = 25
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			planner := &recordingPlanner{}
			for i := range links {
				c := setChange("tenant-a", fmt.Sprintf("from-%d", w), fmt.Sprintf("to-%d", i), int64(i+1))
				if _, err := s.ApplyChange(t.Context(), c, nil, planner.plan()); err != nil {
					errs[w] = err
					return
				}
				// Dubblettleverans av samma identitet (replay).
				if _, err := s.ApplyChange(t.Context(), c, nil, planner.plan()); err != nil {
					errs[w] = err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	for w, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", w, err)
		}
	}
	// Inga tappade länkar trots lokal samtidighet (ingen last-wins-förlust).
	for w := range workers {
		got := outgoingToIDs(t, s, "tenant-a", "contains", fmt.Sprintf("from-%d", w))
		if len(got) != links {
			t.Fatalf("worker %d links=%d, want %d", w, len(got), links)
		}
	}
}

func TestProjectionConcurrentInstances(t *testing.T) {
	a := pgTestStore(t)
	b := pgTestStoreNoTruncate(t)

	// Två instanser mot samma DB skriver samma nyckel samtidigt: en vinner
	// per leverans, båda utan fel, mängden konvergerar utan förlust.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	stores := []*Store{a, b}
	for i, st := range stores {
		wg.Add(1)
		go func(i int, st *Store) {
			defer wg.Done()
			planner := &recordingPlanner{}
			for j := range 20 {
				c := setChange("tenant-a", "shared", fmt.Sprintf("to-%d", j), int64(j+1))
				if _, err := st.ApplyChange(t.Context(), c, nil, planner.plan()); err != nil {
					errs[i] = err
					return
				}
			}
		}(i, st)
	}
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("instances failed: %v %v", errs[0], errs[1])
	}
	if got := outgoingToIDs(t, a, "tenant-a", "contains", "shared"); len(got) != 20 {
		t.Fatalf("shared set=%d, want 20 (lost update)", len(got))
	}
}
