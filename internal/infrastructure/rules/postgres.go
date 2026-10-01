package rules

import (
	"context"
	"embed"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// queryTimeout gränsar varje enskild databasoperation (samma budget som
// övriga tjänster).
const queryTimeout = 5 * time.Second

// Källor för en regelrad: seed (YAML-filer, får uppdateras av seed) eller
// api (skapad/ändrad via API, seed rör den aldrig).
const (
	SourceSeed = "seed"
	SourceAPI  = "api"
)

var (
	ErrNotFound    = errors.New("rule not found")
	ErrConflict    = errors.New("revision conflict")
	ErrInvalidRule = errors.New("invalid rule set")
)

// StoredRule är en persisterad regel med identitet och livscykel.
// Position är den stabila ordningen motorn använder vid lika-poäng
// (seed i filordning, API-regler sist i skapandeordning); aldrig
// omnumrerad, luckor vid DELETE är avsiktliga.
type StoredRule struct {
	ID       string
	SeedKey  string
	Kind     string
	Rule     transform.Rule
	Source   string
	Revision int64
	Position int64
}

// SeedEntry är en regel från seed-filer med stabil nyckel (<fil>#<index>).
type SeedEntry struct {
	SeedKey string
	Kind    string
	Rule    transform.Rule
}

// SeedResult räknar seed-utfallet: nya rader, uppdaterade seed-rader,
// borttagna seed-rader (borttagna filer) och överhoppade (oförändrade
// seed-rader + API-ägda rader).
type SeedResult struct {
	Added   int
	Updated int
	Pruned  int
	Skipped int
}

// PgStore persisterar transform-regler i PostgreSQL (enda backend,
// samma mönster som övriga tjänster).
type PgStore struct {
	pool *pgxpool.Pool
}

// NewPostgres ansluter, verifierar nåbarhet (Ping) och applicerar
// migrationer idempotent under ett advisory-lås.
func NewPostgres(ctx context.Context, cfg Config) (*PgStore, error) {
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
	s := &PgStore{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close stänger poolen.
func (s *PgStore) Close() {
	s.pool.Close()
}

func (s *PgStore) migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(6193027748113459)`); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), queryTimeout)
		defer cancel()
		if _, err := conn.Exec(cleanup, `SELECT pg_advisory_unlock(6193027748113459)`); err != nil {
			// En anslutning med kvarvarande sessionslås får inte återgå till poolen.
			_ = conn.Conn().Close(cleanup)
		}
	}()
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		sql, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("rules: migration %s: %w", name, err)
		}
	}
	return nil
}

func (s *PgStore) ctx(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, queryTimeout)
}

func scanRule(id, seedKey, kind, source string, revision, position int64, raw []byte) (StoredRule, error) {
	var r transform.Rule
	if err := json.Unmarshal(raw, &r); err != nil {
		return StoredRule{}, fmt.Errorf("rules: stored rule %s is not decodable: %w", id, err)
	}
	return StoredRule{ID: id, SeedKey: seedKey, Kind: kind, Rule: r, Source: source, Revision: revision, Position: position}, nil
}

// ListRules returnerar alla regler i stabil ordning (position, sedan id
// som deterministisk tiebreak): seed i filordning, API-regler sist.
// Motorn väljer första regeln vid lika poäng, så denna ordning definierar
// precedens oberoende av UUID.
func (s *PgStore) ListRules(ctx context.Context) ([]StoredRule, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	return listRules(ctx, s.pool)
}

type ruleQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func listRules(ctx context.Context, q ruleQuerier) ([]StoredRule, error) {
	rows, err := q.Query(ctx, `SELECT id::text, COALESCE(seed_key,''), kind, source, revision, position, rule FROM transform_rules ORDER BY position, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoredRule
	for rows.Next() {
		var st StoredRule
		var raw []byte
		if err := rows.Scan(&st.ID, &st.SeedKey, &st.Kind, &st.Source, &st.Revision, &st.Position, &raw); err != nil {
			return nil, err
		}
		st, err = scanRule(st.ID, st.SeedKey, st.Kind, st.Source, st.Revision, st.Position, raw)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	if out == nil {
		out = []StoredRule{}
	}
	return out, rows.Err()
}

// LoadRules läser version och rader ur samma MVCC-snapshot. Read Committed
// räcker inte: separata SELECT kan annars se olika committade mutationer.
func (s *PgStore) LoadRules(ctx context.Context) ([]StoredRule, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), queryTimeout)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	var version int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE((SELECT version FROM rule_version WHERE id=1), 0)`).Scan(&version); err != nil {
		return nil, 0, err
	}
	stored, err := listRules(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, 0, err
	}
	return stored, version, nil
}

// GetRule hämtar en regel på id.
func (s *PgStore) GetRule(ctx context.Context, id string) (StoredRule, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	var st StoredRule
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT id::text, COALESCE(seed_key,''), kind, source, revision, position, rule FROM transform_rules WHERE id=$1`, id).
		Scan(&st.ID, &st.SeedKey, &st.Kind, &st.Source, &st.Revision, &st.Position, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return StoredRule{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return StoredRule{}, err
	}
	return scanRule(st.ID, st.SeedKey, st.Kind, st.Source, st.Revision, st.Position, raw)
}

func marshalRule(r transform.Rule) ([]byte, error) {
	if err := transform.Validate([]transform.Rule{r}); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidRule, err)
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("rules: rule is not encodable: %w", err)
	}
	return raw, nil
}

func validateCandidate(ctx context.Context, tx pgx.Tx) error {
	stored, err := listRules(ctx, tx)
	if err != nil {
		return err
	}
	candidate := make([]transform.Rule, len(stored))
	for i, st := range stored {
		candidate[i] = st.Rule
		candidate[i].ID = st.ID
	}
	if err := transform.Validate(candidate); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRule, err)
	}
	return nil
}

// Alla skrivare använder samma transaktionslås för validering av hela
// kandidatuppsättningen även mellan processer.
func lockRuleSet(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(6193027748113460)`)
	return err
}

func rollback(ctx context.Context, tx pgx.Tx) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), queryTimeout)
	defer cancel()
	_ = tx.Rollback(cleanup)
}

// RuleVersion returnerar den globala versionsräknaren för regelregistret.
// Saknad rad betyder version 0. Varje mutation bumpar den i samma
// transaktion, så pollande instanser ser ändringar utan att missa.
func (s *PgStore) RuleVersion(ctx context.Context) (int64, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	var v int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE((SELECT version FROM rule_version WHERE id=1), 0)`).Scan(&v)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// bumpVersionTx ökar versionsräknaren. Måste anropas i samma transaktion
// som mutationen den versionerar.
func bumpVersionTx(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `INSERT INTO rule_version(id, version) VALUES(1, 1)
		ON CONFLICT (id) DO UPDATE SET version = rule_version.version + 1`)
	return err
}

// InsertRule lagrar en ny API-regel (källa api, revision 1) och returnerar
// den med genererat UUID. Ogiltiga regler nekas före skrivning.
func (s *PgStore) InsertRule(ctx context.Context, r transform.Rule) (StoredRule, error) {
	raw, err := marshalRule(r)
	if err != nil {
		return StoredRule{}, err
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return StoredRule{}, err
	}
	defer rollback(ctx, tx)
	if err := lockRuleSet(ctx, tx); err != nil {
		return StoredRule{}, err
	}
	var st StoredRule
	var stored []byte
	err = tx.QueryRow(ctx,
		`INSERT INTO transform_rules(kind, rule, source) VALUES($1, $2, $3) RETURNING id::text, COALESCE(seed_key,''), kind, source, revision, position, rule`,
		string(r.Match.Kind), raw, SourceAPI).
		Scan(&st.ID, &st.SeedKey, &st.Kind, &st.Source, &st.Revision, &st.Position, &stored)
	if err != nil {
		return StoredRule{}, err
	}
	if err := validateCandidate(ctx, tx); err != nil {
		return StoredRule{}, err
	}
	if err := bumpVersionTx(ctx, tx); err != nil {
		return StoredRule{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return StoredRule{}, err
	}
	return scanRule(st.ID, st.SeedKey, st.Kind, st.Source, st.Revision, st.Position, stored)
}

// UpdateRule ersätter en regel vid matchande revision (optimistisk låsning:
// fel revision ger ErrConflict, okänt id ErrNotFound) och flippar källan
// till api så att framtida seed aldrig skriver över manuella ändringar.
func (s *PgStore) UpdateRule(ctx context.Context, id string, r transform.Rule, expectedRev int64) (StoredRule, error) {
	raw, err := marshalRule(r)
	if err != nil {
		return StoredRule{}, err
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return StoredRule{}, err
	}
	defer rollback(ctx, tx)
	if err := lockRuleSet(ctx, tx); err != nil {
		return StoredRule{}, err
	}
	var st StoredRule
	var stored []byte
	err = tx.QueryRow(ctx,
		`UPDATE transform_rules SET kind=$1, rule=$2, source=$3, revision=revision+1, updated_at=now()
		 WHERE id=$4 AND revision=$5
		 RETURNING id::text, COALESCE(seed_key,''), kind, source, revision, position, rule`,
		string(r.Match.Kind), raw, SourceAPI, id, expectedRev).
		Scan(&st.ID, &st.SeedKey, &st.Kind, &st.Source, &st.Revision, &st.Position, &stored)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if qerr := tx.QueryRow(ctx, `SELECT true FROM transform_rules WHERE id=$1`, id).Scan(&exists); qerr != nil {
			if errors.Is(qerr, pgx.ErrNoRows) {
				return StoredRule{}, fmt.Errorf("%w: %s", ErrNotFound, id)
			}
			return StoredRule{}, qerr
		}
		return StoredRule{}, fmt.Errorf("%w: %s", ErrConflict, id)
	}
	if err != nil {
		return StoredRule{}, err
	}
	if err := validateCandidate(ctx, tx); err != nil {
		return StoredRule{}, err
	}
	if err := bumpVersionTx(ctx, tx); err != nil {
		return StoredRule{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return StoredRule{}, err
	}
	return scanRule(st.ID, st.SeedKey, st.Kind, st.Source, st.Revision, st.Position, stored)
}

// DeleteRule tar bort en regel vid matchande revision (optimistisk låsning
// i SQL: fel revision ger ErrConflict, okänt id ErrNotFound — ingen
// TOCTOU-glipa mellan läsning och radering, även mellan instanser).
// Borttagning av en seed-rad registreras som tombstone så att seed inte
// återupplivar den (TF-03); API-rader utan seed_key lämnar inget spår.
func (s *PgStore) DeleteRule(ctx context.Context, id string, expectedRev int64) error {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	if err := lockRuleSet(ctx, tx); err != nil {
		return err
	}
	var seedKey *string
	err = tx.QueryRow(ctx,
		`DELETE FROM transform_rules WHERE id=$1 AND revision=$2 RETURNING seed_key`,
		id, expectedRev).Scan(&seedKey)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if qerr := tx.QueryRow(ctx, `SELECT true FROM transform_rules WHERE id=$1`, id).Scan(&exists); qerr != nil {
			if errors.Is(qerr, pgx.ErrNoRows) {
				return fmt.Errorf("%w: %s", ErrNotFound, id)
			}
			return qerr
		}
		return fmt.Errorf("%w: %s", ErrConflict, id)
	}
	if err != nil {
		return err
	}
	if seedKey != nil {
		if _, err := tx.Exec(ctx, `INSERT INTO seed_tombstones(seed_key) VALUES($1) ON CONFLICT DO NOTHING`, *seedKey); err != nil {
			return err
		}
	}
	if err := bumpVersionTx(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// SeedRules lägger in saknade seed-nycklar, uppdaterar ändrade seed-rader
// och hoppar över oförändrade seed-rader samt API-ägda rader (aldrig
// överskrivning av manuella ändringar). Ogiltiga seed-regler felar hela
// seedningen (fail fast, samma som startup-validering idag).
//
// Positioner tilldelas efter inmatningsordning (0..N): seed i filordning
// först, API-regler därefter i skapandeordning. Revision bumpas vid varje
// ändring av raden — även ren omordning — eftersom ordning är en del av
// den effektiva konfigurationen.
//
// Seed och API serialiseras med samma transaktionslås. Hela kandidaten
// valideras innan commit; seed-rader, prune och versionsbump är atomiska.
// API-ägda och tombstonade seed-nycklar skrivs aldrig över.
func (s *PgStore) SeedRules(ctx context.Context, entries []SeedEntry) (SeedResult, error) {
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SeedResult{}, err
	}
	defer rollback(ctx, tx)
	if err := lockRuleSet(ctx, tx); err != nil {
		return SeedResult{}, err
	}
	res, err := s.seedRules(ctx, tx, entries)
	if err != nil {
		return SeedResult{}, err
	}
	if err := validateCandidate(ctx, tx); err != nil {
		return SeedResult{}, err
	}
	if res.Added+res.Updated+res.Pruned > 0 {
		if err := bumpVersionTx(ctx, tx); err != nil {
			return SeedResult{}, err
		}
	}
	// Sekvensen är inte transaktionell. Justera endast efter lyckad
	// kandidatvalidering; rollback kan ge luckor men aldrig delvisa regler.
	if _, err := tx.Exec(ctx, `SELECT setval('transform_rules_position_seq', GREATEST((SELECT COALESCE(MAX(position), -1) + 1 FROM transform_rules), 1), false)`); err != nil {
		return SeedResult{}, fmt.Errorf("seed sequence: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return SeedResult{}, err
	}
	return res, nil
}

func (s *PgStore) seedRules(ctx context.Context, tx pgx.Tx, entries []SeedEntry) (SeedResult, error) {
	var res SeedResult
	for i, e := range entries {
		raw, err := marshalRule(e.Rule)
		if err != nil {
			return res, fmt.Errorf("seed %s: %w", e.SeedKey, err)
		}
		tag, err := tx.Exec(ctx,
			`INSERT INTO transform_rules(seed_key, kind, rule, source, position)
			 SELECT $1, $2, $3, $4, $5
			 WHERE NOT EXISTS (SELECT 1 FROM seed_tombstones WHERE seed_key=$1)
			 ON CONFLICT (seed_key) DO NOTHING`,
			e.SeedKey, string(e.Rule.Match.Kind), raw, SourceSeed, i)
		if err != nil {
			return res, fmt.Errorf("seed %s: %w", e.SeedKey, err)
		}
		if tag.RowsAffected() > 0 {
			res.Added++
			continue
		}
		var id, source string
		var position int64
		var stored []byte
		err = tx.QueryRow(ctx, `SELECT id::text, source, position, rule FROM transform_rules WHERE seed_key=$1`, e.SeedKey).
			Scan(&id, &source, &position, &stored)
		if errors.Is(err, pgx.ErrNoRows) {
			// Tombstonad (eller tävlande borttagning): återuppliva aldrig.
			res.Skipped++
			continue
		}
		if err != nil {
			return res, fmt.Errorf("seed %s: %w", e.SeedKey, err)
		}
		if source == SourceAPI {
			res.Skipped++
			continue
		}
		var cur transform.Rule
		if err := json.Unmarshal(stored, &cur); err != nil {
			return res, fmt.Errorf("seed %s: stored rule is not decodable: %w", e.SeedKey, err)
		}
		if reflect.DeepEqual(cur, e.Rule) && position == int64(i) {
			res.Skipped++
			continue
		}
		tag, err = tx.Exec(ctx,
			`UPDATE transform_rules
			 SET kind=$1, rule=$2, position=$3, revision=revision+1, updated_at=now()
			 WHERE id=$4 AND source=$5
			   AND (rule IS DISTINCT FROM $2::jsonb OR position IS DISTINCT FROM $3)`,
			string(e.Rule.Match.Kind), string(raw), i, id, SourceSeed)
		if err != nil {
			return res, fmt.Errorf("seed %s: %w", e.SeedKey, err)
		}
		if tag.RowsAffected() == 0 {
			res.Skipped++
			continue
		}
		res.Updated++
	}
	if len(entries) > 0 {
		keys := make([]string, 0, len(entries))
		for _, e := range entries {
			keys = append(keys, e.SeedKey)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM seed_tombstones WHERE NOT (seed_key = ANY($1))`, keys); err != nil {
			return res, fmt.Errorf("seed tombstone prune: %w", err)
		}
		// Borttagna seed-filer: radera föräldralösa seed-rader (7.4).
		// API-övertagna rader har source='api' och bevaras — även med samma
		// seed_key. Verifiera på staging att endast förväntade rader
		// försvinner vid deploy.
		tag, err := tx.Exec(ctx, `DELETE FROM transform_rules WHERE source=$1 AND NOT (seed_key = ANY($2))`, SourceSeed, keys)
		if err != nil {
			return res, fmt.Errorf("seed prune: %w", err)
		}
		res.Pruned = int(tag.RowsAffected())
	}
	return res, nil
}
