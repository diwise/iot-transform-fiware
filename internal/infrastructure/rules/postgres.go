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
	ErrNotFound = errors.New("rule not found")
	ErrConflict = errors.New("revision conflict")
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
	defer conn.Exec(ctx, `SELECT pg_advisory_unlock(6193027748113459)`)
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

func (s *PgStore) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), queryTimeout)
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
func (s *PgStore) ListRules() ([]StoredRule, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT id::text, COALESCE(seed_key,''), kind, source, revision, position, rule FROM transform_rules ORDER BY position, id`)
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

// GetRule hämtar en regel på id.
func (s *PgStore) GetRule(id string) (StoredRule, error) {
	ctx, cancel := s.ctx()
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
		return nil, err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("rules: rule is not encodable: %w", err)
	}
	return raw, nil
}

// RuleVersion returnerar den globala versionsräknaren för regelregistret.
// Saknad rad betyder version 0. Varje mutation bumpar den i samma
// transaktion, så pollande instanser ser ändringar utan att missa.
func (s *PgStore) RuleVersion() (int64, error) {
	ctx, cancel := s.ctx()
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
func (s *PgStore) InsertRule(r transform.Rule) (StoredRule, error) {
	raw, err := marshalRule(r)
	if err != nil {
		return StoredRule{}, err
	}
	ctx, cancel := s.ctx()
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return StoredRule{}, err
	}
	defer tx.Rollback(ctx)
	var st StoredRule
	var stored []byte
	err = tx.QueryRow(ctx,
		`INSERT INTO transform_rules(kind, rule, source) VALUES($1, $2, $3) RETURNING id::text, COALESCE(seed_key,''), kind, source, revision, position, rule`,
		string(r.Match.Kind), raw, SourceAPI).
		Scan(&st.ID, &st.SeedKey, &st.Kind, &st.Source, &st.Revision, &st.Position, &stored)
	if err != nil {
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
func (s *PgStore) UpdateRule(id string, r transform.Rule, expectedRev int64) (StoredRule, error) {
	raw, err := marshalRule(r)
	if err != nil {
		return StoredRule{}, err
	}
	ctx, cancel := s.ctx()
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return StoredRule{}, err
	}
	defer tx.Rollback(ctx)
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
			return StoredRule{}, fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		return StoredRule{}, fmt.Errorf("%w: %s", ErrConflict, id)
	}
	if err != nil {
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
func (s *PgStore) DeleteRule(id string, expectedRev int64) error {
	ctx, cancel := s.ctx()
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var seedKey *string
	err = tx.QueryRow(ctx,
		`DELETE FROM transform_rules WHERE id=$1 AND revision=$2 RETURNING seed_key`,
		id, expectedRev).Scan(&seedKey)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if qerr := tx.QueryRow(ctx, `SELECT true FROM transform_rules WHERE id=$1`, id).Scan(&exists); qerr != nil {
			return fmt.Errorf("%w: %s", ErrNotFound, id)
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
// Samtidighet (TF-03): varje skrivning är atomär och villkorad.
// Samtidiga seed-körningar konvergerar (ON CONFLICT + IS DISTINCT
// FROM-kontroll); en API-ändring mellan seed-läsning och seed-skrivning
// vinner eftersom uppdateringen kräver source='seed'. Tombstonade nycklar
// (borttagna via API) återupplivas aldrig. Sekvensen ställs alltid förbi
// max — även vid partiellt fel — så API-skapanden aldrig interfolieras
// med seed-blocket.
func (s *PgStore) SeedRules(entries []SeedEntry) (SeedResult, error) {
	res, err := s.seedRules(entries)
	if serr := s.advanceSequence(); serr != nil && err == nil {
		err = fmt.Errorf("seed sequence: %w", serr)
	}
	return res, err
}

func (s *PgStore) advanceSequence() error {
	ctx, cancel := s.ctx()
	defer cancel()
	_, err := s.pool.Exec(ctx, `SELECT setval('transform_rules_position_seq', GREATEST((SELECT COALESCE(MAX(position), -1) + 1 FROM transform_rules), 1), false)`)
	return err
}

func (s *PgStore) seedRules(entries []SeedEntry) (SeedResult, error) {
	var res SeedResult
	for i, e := range entries {
		raw, err := marshalRule(e.Rule)
		if err != nil {
			return res, fmt.Errorf("seed %s: %w", e.SeedKey, err)
		}
		ctx, cancel := s.ctx()
		tag, err := s.pool.Exec(ctx,
			`INSERT INTO transform_rules(seed_key, kind, rule, source, position)
			 SELECT $1, $2, $3, $4, $5
			 WHERE NOT EXISTS (SELECT 1 FROM seed_tombstones WHERE seed_key=$1)
			 ON CONFLICT (seed_key) DO NOTHING`,
			e.SeedKey, string(e.Rule.Match.Kind), raw, SourceSeed, i)
		cancel()
		if err != nil {
			return res, fmt.Errorf("seed %s: %w", e.SeedKey, err)
		}
		if tag.RowsAffected() > 0 {
			res.Added++
			continue
		}
		ctx2, cancel2 := s.ctx()
		var id, source string
		var position int64
		var stored []byte
		err = s.pool.QueryRow(ctx2, `SELECT id::text, source, position, rule FROM transform_rules WHERE seed_key=$1`, e.SeedKey).
			Scan(&id, &source, &position, &stored)
		cancel2()
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
		ctx3, cancel3 := s.ctx()
		tag, err = s.pool.Exec(ctx3,
			`UPDATE transform_rules
			 SET kind=$1, rule=$2, position=$3, revision=revision+1, updated_at=now()
			 WHERE id=$4 AND source=$5
			   AND (rule IS DISTINCT FROM $2::jsonb OR position IS DISTINCT FROM $3)`,
			string(e.Rule.Match.Kind), string(raw), i, id, SourceSeed)
		cancel3()
		if err != nil {
			return res, fmt.Errorf("seed %s: %w", e.SeedKey, err)
		}
		if tag.RowsAffected() == 0 {
			res.Skipped++
			continue
		}
		res.Updated++
	}
	// Versionsbump vid faktisk ändring (TF-05): pollande instanser laddar
	// om. Egen sats utanför radtransaktionerna — monoton och konvergent;
	// värsta fallet är en extra bump som nästa seed konstaterar onödig.
	// Prune nedan bumpar separat vid borttagna rader.
	if res.Added+res.Updated > 0 {
		ctx, cancel := s.ctx()
		defer cancel()
		if _, err := s.pool.Exec(ctx, `INSERT INTO rule_version(id, version) VALUES(1, 1)
			ON CONFLICT (id) DO UPDATE SET version = rule_version.version + 1`); err != nil {
			return res, fmt.Errorf("seed version bump: %w", err)
		}
	}
	if len(entries) > 0 {
		keys := make([]string, 0, len(entries))
		for _, e := range entries {
			keys = append(keys, e.SeedKey)
		}
		ctx, cancel := s.ctx()
		defer cancel()
		if _, err := s.pool.Exec(ctx, `DELETE FROM seed_tombstones WHERE NOT (seed_key = ANY($1))`, keys); err != nil {
			return res, fmt.Errorf("seed tombstone prune: %w", err)
		}
		// Borttagna seed-filer: radera föräldralösa seed-rader (7.4).
		// API-övertagna rader har source='api' och bevaras — även med samma
		// seed_key. Verifiera på staging att endast förväntade rader
		// försvinner vid deploy.
		tag, err := s.pool.Exec(ctx, `DELETE FROM transform_rules WHERE source=$1 AND NOT (seed_key = ANY($2))`, SourceSeed, keys)
		if err != nil {
			return res, fmt.Errorf("seed prune: %w", err)
		}
		res.Pruned = int(tag.RowsAffected())
		if res.Pruned > 0 {
			if _, err := s.pool.Exec(ctx, `INSERT INTO rule_version(id, version) VALUES(1, 1)
				ON CONFLICT (id) DO UPDATE SET version = rule_version.version + 1`); err != nil {
				return res, fmt.Errorf("seed prune version bump: %w", err)
			}
		}
	}
	return res, nil
}
