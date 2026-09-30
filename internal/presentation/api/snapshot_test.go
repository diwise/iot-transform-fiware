package api

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/rules"
	"github.com/matryer/is"
)

// stubRuleStore är ett minneslager för synktester: två Models-instanser
// delar ett stub och agerar två tjänsteinstanser mot samma databas.
// failList injicerar laddningsfel för återhämtningstester.
type stubRuleStore struct {
	mu        sync.Mutex
	envelopes []rules.StoredRule
	version   int64
	failList  error
}

func (s *stubRuleStore) snapshot() []rules.StoredRule {
	out := append([]rules.StoredRule(nil), s.envelopes...)
	sort.Slice(out, func(i, j int) bool { return out[i].Revision < out[j].Revision })
	return out
}

func (s *stubRuleStore) ListRules(ctx context.Context) ([]rules.StoredRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failList != nil {
		return nil, s.failList
	}
	return s.snapshot(), nil
}

func (s *stubRuleStore) LoadRules(ctx context.Context) ([]rules.StoredRule, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if s.failList != nil {
		return nil, 0, s.failList
	}
	return s.snapshot(), s.version, nil
}

func (s *stubRuleStore) GetRule(ctx context.Context, id string) (rules.StoredRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.envelopes {
		if st.ID == id {
			return st, nil
		}
	}
	return rules.StoredRule{}, rules.ErrNotFound
}

func (s *stubRuleStore) InsertRule(ctx context.Context, r transform.Rule) (rules.StoredRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.version++
	st := rules.StoredRule{
		ID:       "stub-" + r.Match.Type,
		Revision: 1,
		Source:   rules.SourceAPI,
		Kind:     string(r.Match.Kind),
		Rule:     r,
	}
	s.envelopes = append(s.envelopes, st)
	return st, nil
}

func (s *stubRuleStore) UpdateRule(ctx context.Context, id string, r transform.Rule, rev int64) (rules.StoredRule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, st := range s.envelopes {
		if st.ID != id {
			continue
		}
		if st.Revision != rev {
			return rules.StoredRule{}, rules.ErrConflict
		}
		st.Revision++
		st.Rule = r
		s.envelopes[i] = st
		s.version++
		return st, nil
	}
	return rules.StoredRule{}, rules.ErrNotFound
}

func (s *stubRuleStore) DeleteRule(ctx context.Context, id string, rev int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, st := range s.envelopes {
		if st.ID != id {
			continue
		}
		if st.Revision != rev {
			return rules.ErrConflict
		}
		s.envelopes = append(s.envelopes[:i], s.envelopes[i+1:]...)
		s.version++
		return nil
	}
	return rules.ErrNotFound
}

func (s *stubRuleStore) RuleVersion(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failList != nil {
		return 0, s.failList
	}
	return s.version, nil
}

func stubThing() transform.Thing {
	return transform.Thing{
		ID: "r1", Type: "Room", TypeName: "Room", NameOrID: "r1",
		Tenant: "t", Event: transform.EventThingsV1Values,
		Fields: map[string]any{"name": "n"},
	}
}

// Två instanser mot samma lager: mutation via A syns i B efter SyncOnce,
// med både version och beteende (samma snapshot för saker+mätvärden).
func TestSnapshotSyncAcrossInstances(t *testing.T) {
	is := is.New(t)
	store := &stubRuleStore{}

	a := NewModels(store, nil)
	b := NewModels(store, nil)
	is.NoErr(a.Refresh(t.Context()))
	is.NoErr(b.Refresh(t.Context()))

	// Båda tomma: ingen matchning någonstans.
	_, err := b.Snapshot().Things.Transform(stubThing())
	is.True(errors.Is(err, transform.ErrNoMatchingRule))

	// Mutation via A (direkt i lagret + lokal refresh, som mutate gör).
	r := testRule()
	st, err := store.InsertRule(t.Context(), r)
	is.NoErr(err)
	is.NoErr(a.Refresh(t.Context()))
	is.Equal(a.Snapshot().Version, int64(1))

	// B ser inget förrän synk.
	_, err = b.Snapshot().Things.Transform(stubThing())
	is.True(errors.Is(err, transform.ErrNoMatchingRule))

	is.NoErr(b.SyncOnce(t.Context()))
	is.Equal(b.Snapshot().Version, int64(1))
	writes, err := b.Snapshot().Things.Transform(stubThing())
	is.NoErr(err)
	is.Equal(len(writes), 1)

	// Samma snapshot-objekt för båda motorerna (ingen mix).
	is.True(b.Snapshot().Things != nil)
	is.True(b.Snapshot().Measurements != nil)

	_ = st
}

// Injicerat laddningsfel: synk felar utan att röra snapshoten; efter
// läkning konvergerar nästa synk (återhämtning).
func TestSnapshotSyncRecoversAfterFailure(t *testing.T) {
	is := is.New(t)
	store := &stubRuleStore{}
	m := NewModels(store, nil)
	is.NoErr(m.Refresh(t.Context()))
	is.Equal(m.Snapshot().Version, int64(0))

	if _, err := store.InsertRule(t.Context(), testRule()); err != nil {
		t.Fatalf("insert: %v", err)
	}

	store.mu.Lock()
	store.failList = errors.New("database unavailable")
	store.mu.Unlock()

	if err := m.SyncOnce(t.Context()); err == nil {
		t.Fatal("SyncOnce must fail while store fails")
	}
	is.Equal(m.Snapshot().Version, int64(0))
	_, err := m.Snapshot().Things.Transform(stubThing())
	is.True(errors.Is(err, transform.ErrNoMatchingRule))

	store.mu.Lock()
	store.failList = nil
	store.mu.Unlock()

	is.NoErr(m.SyncOnce(t.Context()))
	is.Equal(m.Snapshot().Version, int64(1))
	writes, err := m.Snapshot().Things.Transform(stubThing())
	is.NoErr(err)
	is.Equal(len(writes), 1)
}

// Idempotent synk utan ändring: ingen rebuild, samma snapshot-pekare.
func TestSnapshotSyncNoopWithoutChange(t *testing.T) {
	is := is.New(t)
	store := &stubRuleStore{}
	m := NewModels(store, nil)
	is.NoErr(m.Refresh(t.Context()))
	before := m.snap.Load()
	is.NoErr(m.SyncOnce(t.Context()))
	if m.snap.Load() != before {
		t.Fatal("SyncOnce without change must keep snapshot pointer")
	}
}

// Samtidig Transform + SyncOnce/Refresh under -race: ingen datarace,
// alltid komplett snapshot.
func TestSnapshotConcurrentAccess(t *testing.T) {
	store := &stubRuleStore{}
	if _, err := store.InsertRule(t.Context(), testRule()); err != nil {
		t.Fatalf("insert: %v", err)
	}
	m := NewModels(store, nil)
	if err := m.Refresh(t.Context()); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				snap := m.Snapshot()
				if snap == nil || snap.Things == nil || snap.Measurements == nil {
					errs <- errors.New("nil snapshot or engine")
					return
				}
				_, _ = snap.Things.Transform(stubThing())
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := m.SyncOnce(t.Context()); err != nil {
					errs <- err
					return
				}
				if err := m.Refresh(t.Context()); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent access: %v", err)
	}
}

// RunSync plockar upp ändringar utan omstart.
func TestRunSyncPicksUpChanges(t *testing.T) {
	is := is.New(t)
	store := &stubRuleStore{}
	m := NewModels(store, nil)
	is.NoErr(m.Refresh(t.Context()))

	stop := m.RunSync(context.Background(), 5*time.Millisecond, slog.Default())
	defer stop()

	if _, err := store.InsertRule(t.Context(), testRule()); err != nil {
		t.Fatalf("insert: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if m.Snapshot().Version >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("RunSync did not pick up change within deadline")
		}
		time.Sleep(5 * time.Millisecond)
	}
	writes, err := m.Snapshot().Things.Transform(stubThing())
	is.NoErr(err)
	is.Equal(len(writes), 1)
}

type delayedSnapshotStore struct {
	*stubRuleStore
	captured chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (s *delayedSnapshotStore) LoadRules(ctx context.Context) ([]rules.StoredRule, int64, error) {
	r, v, err := s.stubRuleStore.LoadRules(ctx)
	block := false
	s.once.Do(func() { block = true })
	if block {
		close(s.captured)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}
	return r, v, err
}

func TestRefreshCannotPublishOlderSnapshot(t *testing.T) {
	store := &delayedSnapshotStore{stubRuleStore: &stubRuleStore{}, captured: make(chan struct{}), release: make(chan struct{})}
	m := NewModels(store, nil)
	older := make(chan error, 1)
	go func() { older <- m.Refresh(t.Context()) }()
	<-store.captured
	if _, err := store.InsertRule(t.Context(), testRule()); err != nil {
		t.Fatal(err)
	}
	if err := m.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	newer := m.snap.Load()
	close(store.release)
	if err := <-older; err != nil {
		t.Fatal(err)
	}
	if m.snap.Load() != newer || m.Snapshot().Version != 1 {
		t.Fatal("older reload replaced newer published snapshot")
	}
	if _, err := m.Snapshot().Things.Transform(stubThing()); err != nil {
		t.Fatalf("newer rule lost: %v", err)
	}
}

func TestRefreshRejectsInvalidCandidate(t *testing.T) {
	store := &stubRuleStore{}
	st, err := store.InsertRule(t.Context(), testRule())
	if err != nil {
		t.Fatal(err)
	}
	m := NewModels(store, nil)
	if err := m.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := m.snap.Load()
	bad := testRule()
	bad.Entities[0].Properties[0].Type = "unsupported"
	if _, err := store.UpdateRule(t.Context(), st.ID, bad, st.Revision); err != nil {
		t.Fatal(err)
	}
	if err := m.Refresh(t.Context()); err == nil {
		t.Fatal("invalid candidate was published")
	}
	if m.snap.Load() != before {
		t.Fatal("working snapshot lost on validation error")
	}
	if _, err := m.Snapshot().Things.Transform(stubThing()); err != nil {
		t.Fatal(err)
	}
}

func TestRefreshCancellationKeepsSnapshot(t *testing.T) {
	store := &stubRuleStore{}
	m := NewModels(store, nil)
	if err := m.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := m.snap.Load()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := m.Refresh(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if m.snap.Load() != before {
		t.Fatal("canceled load replaced snapshot")
	}
}

func TestSnapshotMetadataAndDefensiveView(t *testing.T) {
	store := &stubRuleStore{}
	st, err := store.InsertRule(t.Context(), testRule())
	if err != nil {
		t.Fatal(err)
	}
	m := NewModels(store, nil)
	if err := m.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	view := m.Snapshot()
	view.Version = 999
	view.Things = nil
	view.Measurements = nil
	if m.Snapshot().Version != 1 || m.ThingsTransformer() == nil || m.MeasurementsTransformer() == nil {
		t.Fatal("caller modified published snapshot")
	}
	writes, err := m.ThingsTransformer().Transform(stubThing())
	if err != nil {
		t.Fatal(err)
	}
	if len(writes) != 1 || writes[0].RuleID != st.ID || writes[0].RuleRevision != st.Revision || writes[0].SnapshotVersion != 1 {
		t.Fatalf("missing execution metadata: %+v", writes)
	}
}

func TestSnapshotInitialFailureThenRestart(t *testing.T) {
	store := &stubRuleStore{failList: errors.New("startup database unavailable")}
	m := NewModels(store, nil)
	if err := m.Refresh(t.Context()); err == nil {
		t.Fatal("startup error hidden")
	}
	if m.Snapshot().Version != -1 {
		t.Fatal("failed startup published a snapshot")
	}
	store.failList = nil
	bad := testRule()
	bad.Match.Kind = "unsupported"
	st, err := store.InsertRule(t.Context(), bad)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Refresh(t.Context()); err == nil {
		t.Fatal("invalid persisted rule accepted on startup")
	}
	if _, err := store.UpdateRule(t.Context(), st.ID, testRule(), st.Revision); err != nil {
		t.Fatal(err)
	}
	restarted := NewModels(store, nil)
	if err := restarted.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.ThingsTransformer().Transform(stubThing()); err != nil {
		t.Fatal(err)
	}
}
