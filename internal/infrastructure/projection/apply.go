package projection

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/jackc/pgx/v5"
)

// Result beskriver en applicerad ändring för C2 (slutlig handlerinkoppling).
type Result struct {
	// Duplicate är sann vid exakt replay av samma relationsidentitet:
	// projektion och arbete lämnades orörda.
	Duplicate bool
	// Outgoing är den fullständiga utgående mängden efter mutationen.
	Outgoing []relations.Link
	// Removed är borttagna länkar (remove/flytt/delete-policy).
	Removed []relations.Link
	// IsLastLink är sann när from inte längre har någon länk kvar.
	IsLastLink bool
	// Pending är antalet nya väntande brokerrader.
	Pending int
}

// ApplyChange applicerar en relationsändring (set/remove/flytt) och
// persisterar projektion + planerat brokerarbete atomärt.
//
// Semantik:
//   - set lägger till länken (duplikat = idempotent upsert, ingen ny
//     planering utöver idempotensnyckelns skydd).
//   - remove tar bort länken (saknad länk = idempotent framgång).
//   - move (PreviousToID) tar atomärt bort tidigare mål och lägger till
//     det nya i samma revision; snapshoten bär kompletta mängder för
//     både nytt och tidigare mål.
//   - Exakt replay av samma RelationID är idempotent (seen-tabell) och
//     ger inget nytt arbete. Revision jämförs aldrig för filtrering:
//     sena event appliceras (dokumenterad avgränsning).
//   - Tenantbrott, kardinalitet, måltyp och hierarkicykel ger ErrConflict;
//     strukturfel ger ErrInvalid (C2: Permanent).
//
// Planeraren (E/C2-ägd, ren funktion) får kompletta mängder och ska bygga
// fullständiga skrivningar — aldrig enstaka array-element i stället för
// mängd. Fel från planeraren rullar tillbaka både projektion och arbete.
func (s *Store) ApplyChange(ctx context.Context, change relations.Change, trace map[string]string, planner relations.PlanFunc) (res Result, err error) {
	ctx, end := beginOp(ctx, "projection-apply",
		"tenant", change.Tenant, "relation", change.Relation, "from", change.FromID, "to", change.ToID,
		"removed", change.Removed, "revision", change.Revision)
	defer func() { end(err) }()

	if err := s.ready(); err != nil {
		return Result{}, err
	}
	if verr := relations.ValidateChange(change); verr != nil {
		return Result{}, mapDomainError(verr)
	}
	if verr := relations.CheckRelationID(change); verr != nil {
		return Result{}, mapDomainError(verr)
	}

	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer rollback(ctx, tx)

	keys := []string{fromKey(change.Tenant, change.Relation, change.FromID)}
	if change.ToID != "" {
		keys = append(keys, reverseKey(change.Tenant, change.Relation, change.ToID))
	}
	if change.PreviousToID != "" {
		keys = append(keys, reverseKey(change.Tenant, change.Relation, change.PreviousToID))
	}
	spec, err := loadSpecTx(ctx, tx, change.Relation)
	if err != nil {
		return Result{}, err
	}
	if hierarchical(change.Relation, spec) {
		keys = append(keys, "proj-h:"+change.Tenant+"/"+change.Relation)
	}
	if err := lockKeys(ctx, tx, keys); err != nil {
		return Result{}, err
	}

	if change.RelationID != "" {
		var fresh bool
		err := tx.QueryRow(ctx,
			`INSERT INTO projection_seen_changes(relation_id, tenant, revision) VALUES($1,$2,$3)
			 ON CONFLICT (relation_id) DO NOTHING RETURNING true`,
			change.RelationID, change.Tenant, change.Revision).Scan(&fresh)
		if errors.Is(err, pgx.ErrNoRows) {
			dup, rerr := s.duplicateResult(ctx, tx, change)
			if rerr != nil {
				return Result{}, rerr
			}
			// Read-only: låt deferred rollback avsluta.
			logging.GetFromContext(ctx).Debug("duplicate relation change skipped",
				"relation_id", change.RelationID)
			return dup, nil
		}
		if err != nil {
			return Result{}, err
		}
		_ = fresh
	}

	var removed []relations.Link
	var previousTos []string
	switch {
	case change.IsMove():
		removed, err = s.applyMoveTx(ctx, tx, change, spec)
		previousTos = []string{change.PreviousToID}
	case change.Removed:
		removed, err = s.applyRemoveTx(ctx, tx, change, spec)
		previousTos = []string{change.ToID}
	default:
		err = s.applySetTx(ctx, tx, change, spec)
	}
	if err != nil {
		return Result{}, err
	}

	snap, err := snapshotTx(ctx, tx, change.Tenant, change.Relation, change.FromID, previousTos)
	if err != nil {
		return Result{}, err
	}
	snap.Removed = removed

	var writes []relations.PendingWrite
	if planner != nil {
		writes, err = planner(snap)
		if err != nil {
			return Result{}, fmt.Errorf("projection: planner: %w", err)
		}
	}
	pending, err := insertPendingTx(ctx, tx, change.Tenant, trace, writes)
	if err != nil {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	return Result{Outgoing: snap.Outgoing, Removed: removed, IsLastLink: snap.IsLastLink, Pending: pending}, nil
}

func (s *Store) duplicateResult(ctx context.Context, tx pgx.Tx, change relations.Change) (Result, error) {
	snap, err := snapshotTx(ctx, tx, change.Tenant, change.Relation, change.FromID, nil)
	if err != nil {
		return Result{}, err
	}
	return Result{Duplicate: true, Outgoing: snap.Outgoing, IsLastLink: snap.IsLastLink}, nil
}

func hierarchical(relation string, spec *relations.Spec) bool {
	return relation == relations.HierarchicalRelation || (spec != nil && spec.Hierarchical)
}

// applySetTx lägger till en länk idempotent. Metadata skrivs alltid om
// (ingen recency-jämförelse).
func (s *Store) applySetTx(ctx context.Context, tx pgx.Tx, change relations.Change, spec *relations.Spec) error {
	count, member, err := s.memberCountTx(ctx, tx, change.Tenant, change.Relation, change.FromID, change.ToID)
	if err != nil {
		return err
	}
	if err := relations.CheckSet(spec, change.Relation, change.ToKind, count, member); err != nil {
		return mapDomainError(err)
	}
	if hierarchical(change.Relation, spec) && !member {
		cycle, err := cycleExistsTx(ctx, tx, change.Tenant, change.Relation, change.FromID, change.ToID)
		if err != nil {
			return err
		}
		if cycle {
			return fmt.Errorf("%w: hierarchy cycle denied for relation %q from %q to %q: %w",
				ErrConflict, change.Relation, change.FromID, change.ToID, relations.ErrCycle)
		}
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO source_links(tenant, relation, from_id, to_id, revision, changed_at, from_kind, from_kind_version, to_kind, to_kind_version, updated_at)
		 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now())
		 ON CONFLICT (tenant, relation, from_id, to_id) DO UPDATE SET
		   revision=$5, changed_at=$6, from_kind=$7, from_kind_version=$8, to_kind=$9, to_kind_version=$10, updated_at=now()`,
		change.Tenant, change.Relation, change.FromID, change.ToID, change.Revision, nullableTime(change.ChangedAt),
		change.FromKind, change.FromKindVersion, change.ToKind, change.ToKindVersion)
	return err
}

// applyRemoveTx tar bort en länk idempotent (saknad länk = framgång).
func (s *Store) applyRemoveTx(ctx context.Context, tx pgx.Tx, change relations.Change, spec *relations.Spec) ([]relations.Link, error) {
	count, err := s.countTx(ctx, tx, change.Tenant, change.Relation, change.FromID)
	if err != nil {
		return nil, err
	}
	if err := relations.CheckRemove(spec, change.Relation, count); err != nil {
		return nil, mapDomainError(err)
	}
	var gone []relations.Link
	rows, err := tx.Query(ctx,
		`DELETE FROM source_links WHERE tenant=$1 AND relation=$2 AND from_id=$3 AND to_id=$4
		 RETURNING `+linkColumns,
		change.Tenant, change.Relation, change.FromID, change.ToID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		l, err := scanLink(rows)
		if err != nil {
			return nil, err
		}
		gone = append(gone, l)
	}
	return gone, rows.Err()
}

// applyMoveTx tar atomärt bort tidigare mål och lägger till det nya.
// Raderna låses/skrivs i sorterad ordning (dödlägesfria).
func (s *Store) applyMoveTx(ctx context.Context, tx pgx.Tx, change relations.Change, spec *relations.Spec) ([]relations.Link, error) {
	count, member, err := s.memberCountTx(ctx, tx, change.Tenant, change.Relation, change.FromID, change.ToID)
	if err != nil {
		return nil, err
	}
	// Borttaget mål räknas inte mot max för det nya.
	if adjust, aerr := s.isMemberTx(ctx, tx, change.Tenant, change.Relation, change.FromID, change.PreviousToID); aerr != nil {
		return nil, aerr
	} else if adjust && !member && count > 0 {
		count--
	}
	if err := relations.CheckSet(spec, change.Relation, change.ToKind, count, member); err != nil {
		return nil, mapDomainError(err)
	}
	if hierarchical(change.Relation, spec) && !member {
		cycle, err := cycleExistsTx(ctx, tx, change.Tenant, change.Relation, change.FromID, change.ToID)
		if err != nil {
			return nil, err
		}
		if cycle {
			return nil, fmt.Errorf("%w: hierarchy cycle denied for relation %q from %q to %q: %w",
				ErrConflict, change.Relation, change.FromID, change.ToID, relations.ErrCycle)
		}
	}
	firstRemove := change.PreviousToID < change.ToID
	var gone []relations.Link
	remove := func() error {
		rows, err := tx.Query(ctx,
			`DELETE FROM source_links WHERE tenant=$1 AND relation=$2 AND from_id=$3 AND to_id=$4
			 RETURNING `+linkColumns,
			change.Tenant, change.Relation, change.FromID, change.PreviousToID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			l, err := scanLink(rows)
			if err != nil {
				return err
			}
			gone = append(gone, l)
		}
		return rows.Err()
	}
	set := func() error {
		_, err := tx.Exec(ctx,
			`INSERT INTO source_links(tenant, relation, from_id, to_id, revision, changed_at, from_kind, from_kind_version, to_kind, to_kind_version, updated_at)
			 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now())
			 ON CONFLICT (tenant, relation, from_id, to_id) DO UPDATE SET
			   revision=$5, changed_at=$6, from_kind=$7, from_kind_version=$8, to_kind=$9, to_kind_version=$10, updated_at=now()`,
			change.Tenant, change.Relation, change.FromID, change.ToID, change.Revision, nullableTime(change.ChangedAt),
			change.FromKind, change.FromKindVersion, change.ToKind, change.ToKindVersion)
		return err
	}
	if firstRemove {
		if err := remove(); err != nil {
			return nil, err
		}
		if err := set(); err != nil {
			return nil, err
		}
	} else {
		if err := set(); err != nil {
			return nil, err
		}
		if err := remove(); err != nil {
			return nil, err
		}
	}
	return gone, nil
}

// DeleteEndpoint lossar atomärt samtliga länkar där målet är from- eller
// to-ändpunkt (delete-policy): varje länk rapporteras som removed:true i
// snapshoten och målet självt planeras av C2/E som lifecycle deleted.
// Min-gränser kringgås avsiktligt — ett borttaget mål kan inte behålla
// länkar. Raderna låses i sorterad ordning (dödlägesfria).
func (s *Store) DeleteEndpoint(ctx context.Context, tenant, endpoint string, trace map[string]string, planner relations.PlanFunc) (res Result, err error) {
	ctx, end := beginOp(ctx, "projection-delete-endpoint", "tenant", tenant, "endpoint", endpoint)
	defer func() { end(err) }()

	if err := s.ready(); err != nil {
		return Result{}, err
	}
	if tenant == "" || endpoint == "" {
		return Result{}, fmt.Errorf("%w: tenant and endpoint are required", ErrInvalid)
	}

	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer rollback(ctx, tx)
	if err := lockKeys(ctx, tx, []string{"proj-del:" + tenant, "proj-ep:" + tenant + "/" + endpoint}); err != nil {
		return Result{}, err
	}
	rows, err := tx.Query(ctx,
		`SELECT `+linkColumns+` FROM source_links WHERE tenant=$1 AND (from_id=$2 OR to_id=$2)
		 ORDER BY relation, from_id, to_id FOR UPDATE`,
		tenant, endpoint)
	if err != nil {
		return Result{}, err
	}
	var detached []relations.Link
	for rows.Next() {
		l, err := scanLink(rows)
		if err != nil {
			rows.Close()
			return Result{}, err
		}
		detached = append(detached, l)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Result{}, err
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM source_links WHERE tenant=$1 AND (from_id=$2 OR to_id=$2)`,
		tenant, endpoint); err != nil {
		return Result{}, err
	}
	snap := relations.Snapshot{
		Tenant: tenant, FromID: endpoint,
		Outgoing:        []relations.Link{},
		ReverseNew:      map[string][]relations.Link{},
		ReversePrevious: map[string][]relations.Link{},
		Removed:         detached,
		IsLastLink:      true,
		Detached:        detached,
		EndpointDeleted: endpoint,
	}
	var writes []relations.PendingWrite
	if planner != nil {
		writes, err = planner(snap)
		if err != nil {
			return Result{}, fmt.Errorf("projection: planner: %w", err)
		}
	}
	pending, err := insertPendingTx(ctx, tx, tenant, trace, writes)
	if err != nil {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	return Result{Removed: detached, IsLastLink: true, Pending: pending}, nil
}

// cycleExistsTx följer föräldrakedjan från newParent med rekursiv CTE:
// sann om child nås (dvs. länken child->newParent skulle sluta cirkeln).
// Hierarkiska skrivningar är serialiserade per tenant+relation via
// advisory-lås, så kedjeläsningen är stabil inom transaktionen.
func cycleExistsTx(ctx context.Context, tx pgx.Tx, tenant, relation, child, newParent string) (bool, error) {
	if child == newParent {
		return true, nil
	}
	var exists bool
	err := tx.QueryRow(ctx,
		`WITH RECURSIVE chain(id) AS (
		   SELECT to_id FROM source_links WHERE tenant=$1 AND relation=$2 AND from_id=$3
		   UNION
		   SELECT l.to_id FROM source_links l JOIN chain c ON l.from_id = c.id
		   WHERE l.tenant=$1 AND l.relation=$2
		 )
		 SELECT EXISTS(SELECT 1 FROM chain WHERE id=$4)`,
		tenant, relation, newParent, child).Scan(&exists)
	return exists, err
}

func (s *Store) countTx(ctx context.Context, tx pgx.Tx, tenant, relation, from string) (int, error) {
	var count int
	err := tx.QueryRow(ctx,
		`SELECT COUNT(*) FROM source_links WHERE tenant=$1 AND relation=$2 AND from_id=$3`,
		tenant, relation, from).Scan(&count)
	return count, err
}

func (s *Store) isMemberTx(ctx context.Context, tx pgx.Tx, tenant, relation, from, to string) (bool, error) {
	var member bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM source_links WHERE tenant=$1 AND relation=$2 AND from_id=$3 AND to_id=$4)`,
		tenant, relation, from, to).Scan(&member)
	return member, err
}

func (s *Store) memberCountTx(ctx context.Context, tx pgx.Tx, tenant, relation, from, to string) (int, bool, error) {
	var count int
	var member bool
	err := tx.QueryRow(ctx,
		`SELECT COUNT(*), COUNT(*) FILTER (WHERE to_id=$4) > 0 FROM source_links WHERE tenant=$1 AND relation=$2 AND from_id=$3`,
		tenant, relation, from, to).Scan(&count, &member)
	return count, member, err
}

// mapDomainError översätter domänfel till lagrets felkontrakt.
func mapDomainError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, relations.ErrInvalidChange):
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	case errors.Is(err, relations.ErrTenantMismatch),
		errors.Is(err, relations.ErrCardinality),
		errors.Is(err, relations.ErrAllowedTarget),
		errors.Is(err, relations.ErrCycle):
		return fmt.Errorf("%w: %v", ErrConflict, err)
	default:
		return err
	}
}

// sortLinkOps ordnar radoperationer för dödlägesfri låsning (används av
// tester för deterministiska jämförelser).
func sortLinkOps(links []relations.Link) {
	sort.Slice(links, func(i, j int) bool {
		if links[i].Relation != links[j].Relation {
			return links[i].Relation < links[j].Relation
		}
		if links[i].FromID != links[j].FromID {
			return links[i].FromID < links[j].FromID
		}
		return links[i].ToID < links[j].ToID
	})
}
