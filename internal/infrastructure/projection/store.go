package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("iot-transform-fiware/projection")

// queryTimeout gränsar varje enskild databasoperation (samma budget som
// övriga lager i tjänsten).
const queryTimeout = 5 * time.Second

var (
	// ErrNotFound betyder saknad rad (länk, spec eller väntande arbete).
	ErrNotFound = errors.New("projection: not found")
	// ErrConflict betyder deterministisk gränskonflikt (kardinalitet,
	// måltyp, hierarkicykel, tenantbrott eller revisionsmismatch vid
	// spec/byten). Aldrig ett dolt ordningsfilter.
	ErrConflict = errors.New("projection: conflict")
	// ErrInvalid betyder strukturfel som ingen retry läker (C2: Permanent).
	ErrInvalid = errors.New("projection: invalid")
)

// Store persisterar den neutrala relationsprojektionen (mängder per
// berört mål) och återställbart väntande brokerarbete i PostgreSQL.
//
// Nycklar: (tenant, relation, from, to); tenant ingår i samtliga nycklar
// och WHERE-villkor. Duplikat är idempotent upsert på samma nyckel.
// Revision lagras som metadata och filtrerar aldrig (sena event är en
// dokumenterad avgränsning).
//
// Transaktionsgränser: varje ApplyChange/DeleteEndpoint/Bootstrap/Rebuild-
// batch är en transaktion där projektion + väntande arbete ändras atomärt.
// Serialisering sker med transaktionella advisory-lås i sorterad ordning
// (gäller även mellan instanser); inget lokalt mutex används för
// korrekthet.
//
// Ägda resurser: poolen endast om Store skapats via NewPostgres. A äger
// start/stopp/wiring och bör dela pool via NewForPool; Close är
// idempotent (sync.Once), nil-säker och stänger endast ägd pool.
type Store struct {
	pool      *pgxpool.Pool
	owned     bool
	closeOnce sync.Once
	closed    atomic.Bool
}

// NewPostgres ansluter, verifierar nåbarhet (Ping) och applicerar
// projektionsschemat idempotent under advisory-lås. Används fristående
// och i tester; produktion bör dela A:s pool via NewForPool.
func NewPostgres(ctx context.Context, cfg Config) (*Store, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.ConnStr())
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(timeoutCtx); err != nil {
		pool.Close()
		return nil, err
	}
	s := &Store{pool: pool, owned: true}
	if err := s.ensureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// NewForPool lindar en delad pool (ägd av A) utan att ta över stängning.
// Schemat förutsätts migrerat (005 via regellagret eller EnsureSchema);
// IsReady verifierar att tabellerna finns.
func NewForPool(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// EnsureSchema applicerar projektionsschemat idempotent under advisory-lås.
// A kan anropa den vid wiring mot delad pool i stället för att förlita sig
// på regellagrets 005-körning.
func (s *Store) EnsureSchema(ctx context.Context) (err error) {
	ctx, span := tracer.Start(ctx, "projection-ensure-schema")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	if err := s.ready(); err != nil {
		return err
	}
	return s.ensureSchema(ctx)
}

// IsReady verifierar att projektionstabellerna finns (A:s readiness kan
// använda den; ändrar inte befintlig probesemantik).
func (s *Store) IsReady(ctx context.Context) (err error) {
	ctx, span := tracer.Start(ctx, "projection-ready")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	if err := s.ready(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	for _, table := range []string{"source_links", "source_link_specs", "projection_seen_changes", "pending_broker_work"} {
		var exists bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=$1)`, table).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: table %s", ErrNotFound, table)
		}
	}
	return nil
}

// Close stänger ägd pool. Idempotent (sync.Once), nil-säker och säker att
// anropa före/efter partiellt lyckad init. Delad pool (NewForPool) stängs
// aldrig här — det äger A.
func (s *Store) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		if s.owned && s.pool != nil {
			s.pool.Close()
		}
	})
}

// IsClosed rapporterar om Close anropats.
func (s *Store) IsClosed() bool {
	return s == nil || s.closed.Load()
}

func (s *Store) ready() error {
	if s == nil || s.pool == nil || s.closed.Load() {
		return fmt.Errorf("%w: projection store is closed", ErrNotFound)
	}
	return nil
}

func (s *Store) ensureSchema(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, schemaLock); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), queryTimeout)
		defer cancel()
		if _, err := conn.Exec(cleanup, `SELECT pg_advisory_unlock($1)`, schemaLock); err != nil {
			_ = conn.Conn().Close(cleanup)
		}
	}()
	if _, err := conn.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("projection: schema: %w", err)
	}
	return nil
}

func (s *Store) ctx(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, queryTimeout)
}

// beginOp startar span och kopplar trace-ID till loggern enligt husets
// mönster (samma som presentation/messaging Begin). Ärrade slog-fält bär
// aldrig payload eller hemligheter — endast tenant/relation/ändpunkter.
func beginOp(ctx context.Context, name string, attrs ...any) (context.Context, func(error)) {
	ctx, span := tracer.Start(ctx, name)
	_, ctx, _ = o11y.AddTraceIDToLoggerAndStoreInContext(span, logging.GetFromContext(ctx).With(attrs...), ctx)
	return ctx, func(err error) { tracing.RecordAnyErrorAndEndSpan(err, span) }
}

func rollback(ctx context.Context, tx pgx.Tx) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), queryTimeout)
	defer cancel()
	_ = tx.Rollback(cleanup)
}

// lockKeys serialiserar berörda mängder mellan workers och instanser med
// transaktionella advisory-lås i sorterad ordning (dödlägesfria). Låsen
// ersätter lokalt mutex, som inte gäller mellan instanser, och ersätter
// inte recency-filter: de ordnar samtidighet, de filtrerar inte event.
func lockKeys(ctx context.Context, tx pgx.Tx, keys []string) error {
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	prev := ""
	for _, k := range sorted {
		if k == "" || k == prev {
			continue
		}
		prev = k
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, k); err != nil {
			return err
		}
	}
	return nil
}

// fromKey låser utgående mängd, reverseKey låser omvänd mängd per mål.
func fromKey(tenant, relation, from string) string {
	return "proj:" + tenant + "/" + relation + "/" + from
}

func reverseKey(tenant, relation, to string) string {
	return "proj-rev:" + tenant + "/" + relation + "/" + to
}

// SetSpec lagrar/ersätter kardinalitetsspecen för ett relationsnamn.
// Spec-författandet äger D; F upprätthåller gränserna vid skrivning.
func (s *Store) SetSpec(ctx context.Context, spec relations.Spec) (err error) {
	ctx, span := tracer.Start(ctx, "projection-set-spec")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	if err := s.ready(); err != nil {
		return err
	}
	if spec.Name == "" {
		return fmt.Errorf("%w: spec name is required", ErrInvalid)
	}
	raw, err := json.Marshal(spec.AllowedTargets)
	if err != nil {
		return fmt.Errorf("%w: spec targets are not encodable: %v", ErrInvalid, err)
	}
	log := logging.GetFromContext(ctx).With("relation", spec.Name)
	ctx = logging.NewContextWithLogger(ctx, log)

	ctx, cancel := s.ctx(ctx)
	defer cancel()
	_, err = s.pool.Exec(ctx,
		`INSERT INTO source_link_specs(relation, allowed_targets, min_links, max_links, hierarchical, updated_at)
		 VALUES($1, $2, $3, $4, $5, now())
		 ON CONFLICT (relation) DO UPDATE SET allowed_targets=$2, min_links=$3, max_links=$4, hierarchical=$5, updated_at=now()`,
		spec.Name, string(raw), spec.MinLinks, spec.MaxLinks, spec.Hierarchical)
	return err
}

// GetSpec hämtar specen för ett relationsnamn (ErrNotFound vid saknad).
func (s *Store) GetSpec(ctx context.Context, relation string) (_ relations.Spec, err error) {
	ctx, span := tracer.Start(ctx, "projection-get-spec")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	if err := s.ready(); err != nil {
		return relations.Spec{}, err
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	var spec relations.Spec
	var raw []byte
	err = s.pool.QueryRow(ctx,
		`SELECT relation, allowed_targets, min_links, max_links, hierarchical FROM source_link_specs WHERE relation=$1`,
		relation).Scan(&spec.Name, &raw, &spec.MinLinks, &spec.MaxLinks, &spec.Hierarchical)
	if errors.Is(err, pgx.ErrNoRows) {
		return relations.Spec{}, fmt.Errorf("%w: spec %s", ErrNotFound, relation)
	}
	if err != nil {
		return relations.Spec{}, err
	}
	if err := json.Unmarshal(raw, &spec.AllowedTargets); err != nil {
		return relations.Spec{}, fmt.Errorf("projection: spec %s is not decodable: %w", relation, err)
	}
	return spec, nil
}

// loadSpecTx läser spec i transaktion; nil spec = permissiv.
func loadSpecTx(ctx context.Context, tx pgx.Tx, relation string) (*relations.Spec, error) {
	var spec relations.Spec
	var raw []byte
	err := tx.QueryRow(ctx,
		`SELECT relation, allowed_targets, min_links, max_links, hierarchical FROM source_link_specs WHERE relation=$1`,
		relation).Scan(&spec.Name, &raw, &spec.MinLinks, &spec.MaxLinks, &spec.Hierarchical)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &spec.AllowedTargets); err != nil {
		return nil, fmt.Errorf("projection: spec %s is not decodable: %w", relation, err)
	}
	return &spec, nil
}

const linkColumns = `tenant, relation, from_id, to_id, revision, changed_at, from_kind, from_kind_version, to_kind, to_kind_version`

func scanLink(row pgx.Row) (relations.Link, error) {
	var l relations.Link
	var changed *time.Time
	err := row.Scan(&l.Tenant, &l.Relation, &l.FromID, &l.ToID, &l.Revision, &changed,
		&l.FromKind, &l.FromKindVersion, &l.ToKind, &l.ToKindVersion)
	if err != nil {
		return relations.Link{}, err
	}
	if changed != nil {
		l.ChangedAt = *changed
	}
	return l, nil
}

func queryLinks(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, sql string, args ...any) ([]relations.Link, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []relations.Link
	for rows.Next() {
		l, err := scanLink(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []relations.Link{}
	}
	return out, nil
}

// Outgoing läser den kompletta utgående mängden för (tenant, relation, from).
func (s *Store) Outgoing(ctx context.Context, tenant, relation, from string) (_ []relations.Link, err error) {
	ctx, span := tracer.Start(ctx, "projection-outgoing")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	if err := s.ready(); err != nil {
		return nil, err
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	return queryLinks(ctx, s.pool,
		`SELECT `+linkColumns+` FROM source_links WHERE tenant=$1 AND relation=$2 AND from_id=$3 ORDER BY to_id`,
		tenant, relation, from)
}

// Reverse läser den kompletta omvända mängden för (tenant, relation, to).
func (s *Store) Reverse(ctx context.Context, tenant, relation, to string) (_ []relations.Link, err error) {
	ctx, span := tracer.Start(ctx, "projection-reverse")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	if err := s.ready(); err != nil {
		return nil, err
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	return queryLinks(ctx, s.pool,
		`SELECT `+linkColumns+` FROM source_links WHERE tenant=$1 AND relation=$2 AND to_id=$3 ORDER BY from_id`,
		tenant, relation, to)
}

// ExportTenant läser samtliga länkar för en tenant (bootstrap/återsynk-läsning).
func (s *Store) ExportTenant(ctx context.Context, tenant string) (_ []relations.Link, err error) {
	ctx, span := tracer.Start(ctx, "projection-export")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	if err := s.ready(); err != nil {
		return nil, err
	}
	if tenant == "" {
		return nil, fmt.Errorf("%w: tenant is required", ErrInvalid)
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	return queryLinks(ctx, s.pool,
		`SELECT `+linkColumns+` FROM source_links WHERE tenant=$1 ORDER BY relation, from_id, to_id`,
		tenant)
}

// BootstrapUpsert läser in ett komplett underlag (A/B: export + per-mål
// relationer) idempotent utan brokerarbete. Returnerar inlagda mot
// uppdaterade rader. Samtidiga live-ändringar under bootstrap bevaras:
// BootstrapPrune tar bara bort rader äldre än anroparens watermark som
// saknas i snapshot.
func (s *Store) BootstrapUpsert(ctx context.Context, links []relations.Link) (inserted, updated int, err error) {
	ctx, span := tracer.Start(ctx, "projection-bootstrap-upsert")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	if err := s.ready(); err != nil {
		return 0, 0, err
	}
	for i, l := range links {
		if l.Tenant == "" || l.Relation == "" || l.FromID == "" || l.ToID == "" {
			return 0, 0, fmt.Errorf("%w: bootstrap link %d lacks tenant/relation/from/to", ErrInvalid, i)
		}
	}
	log := logging.GetFromContext(ctx).With("links", len(links))
	ctx = logging.NewContextWithLogger(ctx, log)

	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer rollback(ctx, tx)
	tenants := map[string]bool{}
	for _, l := range links {
		tenants[l.Tenant] = true
	}
	keys := make([]string, 0, len(tenants))
	for tenant := range tenants {
		keys = append(keys, "proj-bs:"+tenant)
	}
	if err := lockKeys(ctx, tx, keys); err != nil {
		return 0, 0, err
	}
	for _, l := range links {
		var fresh bool
		err := tx.QueryRow(ctx,
			`INSERT INTO source_links(tenant, relation, from_id, to_id, revision, changed_at, from_kind, from_kind_version, to_kind, to_kind_version, updated_at)
			 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now())
			 ON CONFLICT (tenant, relation, from_id, to_id) DO UPDATE SET
			   revision=$5, changed_at=$6, from_kind=$7, from_kind_version=$8, to_kind=$9, to_kind_version=$10, updated_at=now()
			 RETURNING (xmax = 0)`,
			l.Tenant, l.Relation, l.FromID, l.ToID, l.Revision, nullableTime(l.ChangedAt),
			l.FromKind, l.FromKindVersion, l.ToKind, l.ToKindVersion).Scan(&fresh)
		if err != nil {
			return 0, 0, err
		}
		if fresh {
			inserted++
		} else {
			updated++
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return inserted, updated, nil
}

// BootstrapPrune tar bort länkar som varken finns i snapshot eller har
// rört sig sedan watermark (samtidiga ändringar under bootstrap överlever).
func (s *Store) BootstrapPrune(ctx context.Context, tenant string, keep []relations.LinkKey, watermark time.Time) (pruned int64, err error) {
	ctx, span := tracer.Start(ctx, "projection-bootstrap-prune")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	if err := s.ready(); err != nil {
		return 0, err
	}
	if tenant == "" {
		return 0, fmt.Errorf("%w: tenant is required", ErrInvalid)
	}
	rels := make([]string, 0, len(keep))
	froms := make([]string, 0, len(keep))
	tos := make([]string, 0, len(keep))
	for _, k := range keep {
		rels = append(rels, k.Relation)
		froms = append(froms, k.FromID)
		tos = append(tos, k.ToID)
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM source_links
		 WHERE tenant=$1 AND updated_at < $2
		   AND NOT EXISTS (
		     SELECT 1 FROM unnest($3::text[], $4::text[], $5::text[]) AS k(rel, frm, tto)
		     WHERE k.rel = source_links.relation AND k.frm = source_links.from_id AND k.tto = source_links.to_id
		   )`,
		tenant, watermark.UTC(), rels, froms, tos)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// RebuildResult räknar återuppbyggnad efter regeländring.
type RebuildResult struct {
	Groups  int
	Pending int
}

// Rebuild planerar om brokerarbete från befintlig projektion utan nya
// mätningar (regeländringsåterbyggnad). Grupperar per from (relationen
// avgränsad vid behov), läser kompletta mängder och persisterar planerat
// arbete gruppvis atomärt. maxFroms 0 = alla. Regeluppsättningen bärs av
// planner-closuren (E bygger den från aktuell regelversion); broker-I/O sker
// aldrig här — endast pending-kö/DrainOnce levererar.
func (s *Store) Rebuild(ctx context.Context, tenant, relation string, trace map[string]string, planner relations.PlanFunc, maxFroms int) (res RebuildResult, err error) {
	ctx, span := tracer.Start(ctx, "projection-rebuild")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	if err := s.ready(); err != nil {
		return RebuildResult{}, err
	}
	if tenant == "" {
		return RebuildResult{}, fmt.Errorf("%w: tenant is required", ErrInvalid)
	}
	if planner == nil {
		return RebuildResult{}, fmt.Errorf("%w: planner is required", ErrInvalid)
	}
	log := logging.GetFromContext(ctx).With("tenant", tenant, "relation", relation)
	ctx = logging.NewContextWithLogger(ctx, log)

	froms, err := s.distinctFroms(ctx, tenant, relation, maxFroms)
	if err != nil {
		return RebuildResult{}, err
	}
	for _, f := range froms {
		pending, err := s.rebuildGroup(ctx, tenant, f.relation, f.from, trace, planner)
		if err != nil {
			return res, err
		}
		res.Groups++
		res.Pending += pending
	}
	return res, nil
}

type fromGroup struct {
	relation string
	from     string
}

func (s *Store) distinctFroms(ctx context.Context, tenant, relation string, max int) ([]fromGroup, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	sql := `SELECT relation, from_id FROM source_links WHERE tenant=$1`
	args := []any{tenant}
	if relation != "" {
		sql += ` AND relation=$2`
		args = append(args, relation)
	}
	sql += ` GROUP BY relation, from_id ORDER BY relation, from_id`
	if max > 0 {
		sql += ` LIMIT $` + fmt.Sprint(len(args)+1)
		args = append(args, max)
	}
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []fromGroup
	for rows.Next() {
		var g fromGroup
		if err := rows.Scan(&g.relation, &g.from); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func (s *Store) rebuildGroup(ctx context.Context, tenant, relation, from string, trace map[string]string, planner relations.PlanFunc) (int, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer rollback(ctx, tx)
	if err := lockKeys(ctx, tx, []string{fromKey(tenant, relation, from)}); err != nil {
		return 0, err
	}
	snap, err := snapshotTx(ctx, tx, tenant, relation, from, nil)
	if err != nil {
		return 0, err
	}
	writes, err := planner(snap)
	if err != nil {
		return 0, err
	}
	n, err := insertPendingTx(ctx, tx, tenant, trace, writes)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

// RebuildFromExport planerar om brokerarbete direkt från ett
// ExportTenant-underlag (Våg 2: regeluppsättning via planner + projektion via
// exported) utan nya mätningar och utan broker-I/O. Projektionen lämnas
// orörd — endast pending-kön fylls (leverans via DrainOnce). Tenantfrämmande
// länkar i exported filtreras strikt; tom tenant eller nil planner nekas.
// Grupperingen är densamma som SnapshotsForRebuild (sorterad, deterministisk)
// och persistensen är en transaktion: antingen planeras allt eller inget.
func (s *Store) RebuildFromExport(ctx context.Context, tenant, relation string, exported []relations.Link, trace map[string]string, planner relations.PlanFunc) (res RebuildResult, err error) {
	ctx, span := tracer.Start(ctx, "projection-rebuild-export")
	defer func() { tracing.RecordAnyErrorAndEndSpan(err, span) }()
	if err := s.ready(); err != nil {
		return RebuildResult{}, err
	}
	if tenant == "" {
		return RebuildResult{}, fmt.Errorf("%w: tenant is required", ErrInvalid)
	}
	if planner == nil {
		return RebuildResult{}, fmt.Errorf("%w: planner is required", ErrInvalid)
	}
	log := logging.GetFromContext(ctx).With("tenant", tenant, "relation", relation, "exported", len(exported))
	ctx = logging.NewContextWithLogger(ctx, log)

	snaps := relations.SnapshotsForRebuild(exported, tenant, relation)
	var writes []relations.PendingWrite
	for _, snap := range snaps {
		planned, err := planner(snap)
		if err != nil {
			return RebuildResult{}, fmt.Errorf("projection: planner: %w", err)
		}
		writes = append(writes, planned...)
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return RebuildResult{}, err
	}
	defer rollback(ctx, tx)
	n, err := insertPendingTx(ctx, tx, tenant, trace, writes)
	if err != nil {
		return RebuildResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RebuildResult{}, err
	}
	return RebuildResult{Groups: len(snaps), Pending: n}, nil
}

// snapshotTx bygger den kompletta snapshoten för ett from inom transaktion.
// previousTos anges vid remove/flytt så att båda sidornas omvända mängder
// ingår; detached/endpointDelete används av delete-policy.
func snapshotTx(ctx context.Context, tx pgx.Tx, tenant, relation, from string, previousTos []string) (relations.Snapshot, error) {
	snap := relations.Snapshot{
		Tenant:          tenant,
		Relation:        relation,
		FromID:          from,
		ReverseNew:      map[string][]relations.Link{},
		ReversePrevious: map[string][]relations.Link{},
	}
	var err error
	snap.Outgoing, err = queryLinks(ctx, tx,
		`SELECT `+linkColumns+` FROM source_links WHERE tenant=$1 AND relation=$2 AND from_id=$3 ORDER BY to_id`,
		tenant, relation, from)
	if err != nil {
		return snap, err
	}
	snap.IsLastLink = len(snap.Outgoing) == 0
	seen := map[string]bool{}
	for _, l := range snap.Outgoing {
		if seen[l.ToID] {
			continue
		}
		seen[l.ToID] = true
		snap.ReverseNew[l.ToID], err = queryLinks(ctx, tx,
			`SELECT `+linkColumns+` FROM source_links WHERE tenant=$1 AND relation=$2 AND to_id=$3 ORDER BY from_id`,
			tenant, relation, l.ToID)
		if err != nil {
			return snap, err
		}
	}
	for _, to := range previousTos {
		if seen[to] {
			continue
		}
		seen[to] = true
		snap.ReversePrevious[to], err = queryLinks(ctx, tx,
			`SELECT `+linkColumns+` FROM source_links WHERE tenant=$1 AND relation=$2 AND to_id=$3 ORDER BY from_id`,
			tenant, relation, to)
		if err != nil {
			return snap, err
		}
	}
	return snap, nil
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}
