package projection

import (
	"context"
	"fmt"
	"reflect"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	"github.com/google/uuid"
)

// ImportTenant replaces ONE tenant's projection from a complete authoritative
// snapshot, atomically with the resulting pending writes. Requires quiesced
// source writers/consumers (the CLI requires explicit confirmation): source
// revisions are metadata, not a merge watermark. A stale export can resurrect
// absent links; the legacy BootstrapUpsert/Prune pair cannot solve that.
// Other tenants and the seen-event history are never changed.
func (s *Store) ImportTenant(ctx context.Context, tenant string, links []relations.Link, trace map[string]string, planner relations.PlanFunc) (res RebuildResult, err error) {
	ctx, end := beginOp(ctx, "projection-import", "tenant", tenant, "links", len(links))
	defer func() { end(err) }()
	if err := s.ready(); err != nil {
		return res, err
	}
	if tenant == "" || planner == nil {
		return res, fmt.Errorf("%w: tenant and planner are required", ErrInvalid)
	}
	unique := map[relations.LinkKey]relations.Link{}
	for _, l := range links {
		if l.Tenant != tenant || l.FromID == "" || l.ToID == "" || l.Relation == "" || l.FromKind == "" || l.ToKind == "" {
			return res, fmt.Errorf("%w: import requires same-tenant links with IDs, relation and both source kinds", ErrInvalid)
		}
		if old, ok := unique[l.Key()]; ok && !reflect.DeepEqual(old, l) {
			return res, fmt.Errorf("%w: conflicting duplicate import link", ErrInvalid)
		}
		unique[l.Key()] = l
	}
	ordered := make([]relations.Link, 0, len(unique))
	for _, l := range unique {
		ordered = append(ordered, l)
	}
	sortLinkOps(ordered)
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer rollback(ctx, tx)
	if err := lockTenantTx(ctx, tx, tenant); err != nil {
		return res, err
	}
	before, err := queryLinks(ctx, tx, `SELECT `+linkColumns+` FROM source_links WHERE tenant=$1 ORDER BY relation,from_id,to_id`, tenant)
	if err != nil {
		return res, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM source_links WHERE tenant=$1`, tenant); err != nil {
		return res, err
	}
	for _, l := range ordered {
		spec, err := loadSpecTx(ctx, tx, l.Relation)
		if err != nil {
			return res, err
		}
		c := relations.Change{Tenant: l.Tenant, Relation: l.Relation, FromID: l.FromID, ToID: l.ToID, Revision: l.Revision,
			ChangedAt: l.ChangedAt, FromKind: l.FromKind, FromKindVersion: l.FromKindVersion, ToKind: l.ToKind, ToKindVersion: l.ToKindVersion}
		if err := s.applySetTx(ctx, tx, c, spec); err != nil {
			return res, err
		}
	}
	groups := relations.SnapshotsForReplacement(before, ordered, tenant)
	writes, err := planner(relations.Snapshot{Tenant: tenant, Groups: groups, Trigger: uuid.NewString(), Reconcile: true})
	if err != nil {
		return res, fmt.Errorf("projection: import planner: %w", err)
	}
	n, err := insertPendingTx(ctx, tx, tenant, trace, writes)
	if err != nil {
		return res, err
	}
	if err := tx.Commit(ctx); err != nil {
		return res, err
	}
	return RebuildResult{Groups: len(groups), Pending: n}, nil
}
