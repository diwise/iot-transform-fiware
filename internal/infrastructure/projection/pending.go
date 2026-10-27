package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/tracing"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// PendingItem är en återställbar väntande brokerleverans. Payload är
// ogenomskinlig (modellmappat innehåll från E/C2 tolkas aldrig av F);
// Trace är W3C-carriern separat från payloaden.
type PendingItem struct {
	ID           string
	Tenant       string
	Operation    string
	EntityID     string
	Payload      []byte
	Trace        map[string]string
	RuleID       string
	RuleRevision int64
	Attempts     int
	CreatedAt    time.Time
}

// BrokerFunc utför en väntande leverans. C2 adapterar contextbroker-klienten
// till denna signatur; tester använder fakes. Brokeroperationer måste vara
// idempotenta (merge/remove/delete tolererar redan-utfört), eftersom ett
// lyckat anrop följt av misslyckad MarkDone kan levereras igen efter lease.
type BrokerFunc func(ctx context.Context, item PendingItem) error

// encodeTrace serialiserar W3C trace-carrier separat från domänpayload.
// Tom map ger "" (spårlös händelse, ingen länk).
func encodeTrace(tc map[string]string) string {
	if len(tc) == 0 {
		return ""
	}
	raw, err := json.Marshal(tc)
	if err != nil {
		return ""
	}
	return string(raw)
}

// decodeTrace återställer lagrad trace-carrier. Tom/ogiltig ger nil.
func decodeTrace(s string) map[string]string {
	if s == "" {
		return nil
	}
	var tc map[string]string
	if err := json.Unmarshal([]byte(s), &tc); err != nil || len(tc) == 0 {
		return nil
	}
	return tc
}

// carrierPropagator är W3C TraceContext: samma bärare som transporten
// injicerar/extraherar, så att lagrad context kan länkas efter omstart.
var carrierPropagator = propagation.TraceContext{}

// CaptureTrace fångar aktuell propageringscarrier för beständig lagring.
// C2-gränssnitt (Våg 2): handlern anropar den med transportens context före
// ApplyChange/DeleteEndpoint/Rebuild/RebuildFromExport och skickar resultatet
// som trace-argumentet; lagret persisterar det separat från domänpayloaden
// och länkar det vid DrainOnce (aldrig tyst ersatt ursprung).
// Returnerar nil vid spårlöst context.
func CaptureTrace(ctx context.Context) map[string]string {
	return captureTrace(ctx)
}

// captureTrace fångar aktuell propageringscarrier för beständig lagring.
// Returnerar nil vid spårlöst context.
func captureTrace(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	carrierPropagator.Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	out := make(map[string]string, len(carrier))
	maps.Copy(out, carrier)
	return out
}

// linkSpan startar span med länk till lagrad trace-context (aldrig som tyst
// ersatt ursprung). Spårlös post ger vanlig span utan länk.
func linkSpan(ctx context.Context, name string, stored map[string]string) (context.Context, trace.Span) {
	if len(stored) > 0 {
		remote := carrierPropagator.Extract(ctx, propagation.MapCarrier(stored))
		if sc := trace.SpanContextFromContext(remote); sc.IsValid() {
			return tracer.Start(ctx, name, trace.WithLinks(trace.Link{SpanContext: sc}))
		}
	}
	return tracer.Start(ctx, name)
}

// insertPendingTx persisterar planerat arbete idempotent per
// (tenant, idempotency_key). Duplikat (samma nyckel) ger inga nya rader —
// replay av samma planering är ofarlig. Tom nyckel är planeringsfel
// (ErrInvalid, ingen retry läker det).
func insertPendingTx(ctx context.Context, tx pgx.Tx, tenant string, trace map[string]string, writes []relations.PendingWrite) (int, error) {
	if len(writes) == 0 {
		return 0, nil
	}
	carrier := encodeTrace(trace)
	n := 0
	for i, w := range writes {
		if w.IdempotencyKey == "" {
			return 0, fmt.Errorf("%w: pending write %d lacks idempotency key", ErrInvalid, i)
		}
		if w.Operation == "" {
			return 0, fmt.Errorf("%w: pending write %d lacks operation", ErrInvalid, i)
		}
		payload := w.Payload
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		tag, err := tx.Exec(ctx,
			`INSERT INTO pending_broker_work(tenant, idempotency_key, operation, entity_id, payload, trace_context, rule_id, rule_revision)
			 VALUES($1,$2,$3,$4,$5,$6,$7,$8)
			 ON CONFLICT (tenant, idempotency_key) DO NOTHING`,
			tenant, w.IdempotencyKey, w.Operation, w.EntityID, payload, carrier, w.RuleID, w.RuleRevision)
		if err != nil {
			return 0, err
		}
		n += int(tag.RowsAffected())
	}
	return n, nil
}

// ClaimPending hämtar upp till limit väntande poster (äldst först) med
// SELECT FOR UPDATE SKIP LOCKED: samtidiga workers/instanser får disjunkta
// mängder. Leasen (claimed_at) gör kraschade workers omstartssäkra —
// förfallna claims kan hämtas igen. Tom tenant = alla tenants.
func (s *Store) ClaimPending(ctx context.Context, tenant string, limit int, lease time.Duration) (items []PendingItem, err error) {
	ctx, end := beginOp(ctx, "projection-claim", "tenant", tenant, "limit", limit)
	defer func() { end(err) }()

	if err := s.ready(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, fmt.Errorf("%w: limit must be positive", ErrInvalid)
	}
	leaseSecs := lease.Seconds()
	if leaseSecs <= 0 {
		leaseSecs = 60
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`WITH picked AS (
		   SELECT id FROM pending_broker_work
		   WHERE ($1 = '' OR tenant = $1)
		     AND (claimed_at IS NULL OR claimed_at < now() - ($2 || ' seconds')::interval)
		   ORDER BY created_at, id LIMIT $3 FOR UPDATE SKIP LOCKED
		 )
		 UPDATE pending_broker_work p SET claimed_at = now(), attempts = p.attempts + 1
		 FROM picked WHERE p.id = picked.id
		 RETURNING p.id::text, p.tenant, p.operation, p.entity_id, p.payload, p.trace_context, p.rule_id, p.rule_revision, p.attempts, p.created_at`,
		tenant, fmt.Sprintf("%f", leaseSecs), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var it PendingItem
		var rawPayload []byte
		var rawTrace string
		if err := rows.Scan(&it.ID, &it.Tenant, &it.Operation, &it.EntityID, &rawPayload, &rawTrace, &it.RuleID, &it.RuleRevision, &it.Attempts, &it.CreatedAt); err != nil {
			return nil, err
		}
		it.Payload = append([]byte(nil), rawPayload...)
		it.Trace = decodeTrace(rawTrace)
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if items == nil {
		items = []PendingItem{}
	}
	return items, nil
}

// MarkDone tar bort en levererad post (ErrNotFound vid saknad).
func (s *Store) MarkDone(ctx context.Context, id string) (err error) {
	ctx, end := beginOp(ctx, "projection-mark-done", "pending_id", id)
	defer func() { end(err) }()

	if err := s.ready(); err != nil {
		return err
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `DELETE FROM pending_broker_work WHERE id=$1::uuid`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: pending work %s", ErrNotFound, id)
	}
	return nil
}

// MarkFailed släpper claimen så att posten kan hämtas igen (omedelbar
// retry; attempts behålls för diagnostik). ErrNotFound vid saknad.
func (s *Store) MarkFailed(ctx context.Context, id string) (err error) {
	ctx, end := beginOp(ctx, "projection-mark-failed", "pending_id", id)
	defer func() { end(err) }()

	if err := s.ready(); err != nil {
		return err
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `UPDATE pending_broker_work SET claimed_at=NULL WHERE id=$1::uuid`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: pending work %s", ErrNotFound, id)
	}
	return nil
}

// PendingCount räknar väntande poster (diagnostik/bootstrap-verifiering).
func (s *Store) PendingCount(ctx context.Context, tenant string) (count int, err error) {
	ctx, end := beginOp(ctx, "projection-pending-count", "tenant", tenant)
	defer func() { end(err) }()

	if err := s.ready(); err != nil {
		return 0, err
	}
	ctx, cancel := s.ctx(ctx)
	defer cancel()
	err = s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM pending_broker_work WHERE ($1 = '' OR tenant=$1)`, tenant).Scan(&count)
	return count, err
}

// DrainOnce delivers oldest-first per (tenant,entity), with the row locked
// until HTTP and acknowledgement finish. A lease alone is not sufficient:
// an expired worker could otherwise finish an older list after a newer one.
// Failed entities block their successors, but not other tenants/entities.
// ClaimPending/MarkDone remain low-level legacy helpers, not this worker's
// delivery protocol. No broker read-modify-write or source recency filtering.
func (s *Store) DrainOnce(ctx context.Context, tenant string, limit int, lease time.Duration, broker BrokerFunc) (completed, failed int, err error) {
	ctx, end := beginOp(ctx, "projection-drain", "tenant", tenant, "limit", limit)
	defer func() { end(err) }()

	if broker == nil {
		return 0, 0, fmt.Errorf("%w: broker func is required", ErrInvalid)
	}
	if err := s.ready(); err != nil {
		return 0, 0, err
	}
	if limit <= 0 {
		return 0, 0, fmt.Errorf("%w: limit must be positive", ErrInvalid)
	}
	if lease <= 0 {
		lease = time.Minute
	}
	attempted := make([]string, 0, limit)
	for range limit {
		id, delivered, derr := s.deliverNext(ctx, tenant, lease, attempted, broker)
		if derr != nil {
			return completed, failed, derr
		}
		if id == "" {
			break
		}
		attempted = append(attempted, id)
		if delivered {
			completed++
		} else {
			failed++
		}
	}
	return completed, failed, nil
}

func (s *Store) deliverNext(ctx context.Context, tenant string, lease time.Duration, attempted []string, broker BrokerFunc) (id string, delivered bool, err error) {
	queryCtx, queryCancel := s.ctx(ctx)
	defer queryCancel()
	tx, err := s.pool.Begin(queryCtx)
	if err != nil {
		return "", false, err
	}
	defer rollback(ctx, tx)
	var item PendingItem
	var rawTrace string
	err = tx.QueryRow(queryCtx, `SELECT p.id::text,p.tenant,p.operation,p.entity_id,p.payload,p.trace_context,p.rule_id,p.rule_revision,p.attempts,p.created_at
		FROM pending_broker_work p
		WHERE ($1='' OR p.tenant=$1)
		AND NOT (p.id::text = ANY($2::text[]))
		AND (p.claimed_at IS NULL OR p.claimed_at < now()-($3 * interval '1 second'))
		AND NOT EXISTS (SELECT 1 FROM pending_broker_work prior
			WHERE prior.tenant=p.tenant AND prior.entity_id=p.entity_id AND prior.delivery_order<p.delivery_order)
		ORDER BY p.delivery_order LIMIT 1 FOR UPDATE OF p SKIP LOCKED`, tenant, attempted, lease.Seconds()).
		Scan(&item.ID, &item.Tenant, &item.Operation, &item.EntityID, &item.Payload, &rawTrace, &item.RuleID, &item.RuleRevision, &item.Attempts, &item.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	item.Trace = decodeTrace(rawTrace)
	item.Attempts++
	if _, err := tx.Exec(queryCtx, `UPDATE pending_broker_work SET attempts=attempts+1 WHERE id=$1::uuid`, item.ID); err != nil {
		return "", false, err
	}
	queryCancel() // no DB query timer runs during the separately budgeted HTTP call
	itemCtx, span := linkSpan(ctx, "projection-deliver", item.Trace)
	span.SetAttributes(attribute.String("tenant", item.Tenant), attribute.String("operation", item.Operation), attribute.String("entity_id", item.EntityID))
	var deliveryErr error
	defer func() { tracing.RecordAnyErrorAndEndSpan(errors.Join(deliveryErr, err), span) }()
	deliveryErr = broker(itemCtx, item)
	if deliveryErr != nil {
		logging.GetFromContext(ctx).Error("pending work delivery failed", "pending_id", item.ID, "tenant", item.Tenant, "entity_id", item.EntityID, "operation", item.Operation, "err", deliveryErr)
		// Persist attempts and keep the same oldest item for a later retry.
		failedCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), queryTimeout)
		defer cancel()
		if _, err := tx.Exec(failedCtx, `UPDATE pending_broker_work SET claimed_at=NULL WHERE id=$1::uuid`, item.ID); err != nil {
			return "", false, err
		}
		if err := tx.Commit(failedCtx); err != nil {
			return "", false, err
		}
		return item.ID, false, nil
	}
	// Own budget if cancellation arrived after successful HTTP. The
	// transaction/row lock remains held; no younger item can pass it.
	doneCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), queryTimeout)
	defer cancel()
	if _, err := tx.Exec(doneCtx, `DELETE FROM pending_broker_work WHERE id=$1::uuid`, item.ID); err != nil {
		return "", false, err
	}
	if err := tx.Commit(doneCtx); err != nil {
		return "", false, err
	}
	return item.ID, true, nil
}

// Worker dränerar väntande arbete i poll-loop. A äger start/stopp:
// Run blockerar tills ctx avbryts, stoppar då inflöde (ingen ny claim),
// avslutar pågående batch och gör en sista dränering inom stopBudget
// innan den returnerar. Worker äger aldrig lagret — Close sköter A.
type Worker struct {
	store      *Store
	broker     BrokerFunc
	interval   time.Duration
	batch      int
	lease      time.Duration
	stopBudget time.Duration
}

// NewWorker bygger dräneringsworker. interval/lease/budget måste vara
// positiva; batch > 0.
func NewWorker(store *Store, broker BrokerFunc, interval time.Duration, batch int, lease, stopBudget time.Duration) (*Worker, error) {
	if store == nil || store.IsClosed() {
		return nil, fmt.Errorf("%w: worker requires an open store", ErrInvalid)
	}
	if broker == nil {
		return nil, fmt.Errorf("%w: worker requires a broker func", ErrInvalid)
	}
	if interval <= 0 || batch <= 0 || lease <= 0 || stopBudget <= 0 {
		return nil, fmt.Errorf("%w: worker intervals/batch/budget must be positive", ErrInvalid)
	}
	return &Worker{store: store, broker: broker, interval: interval, batch: batch, lease: lease, stopBudget: stopBudget}, nil
}

// Run dränerar tills ctx avbryts. Returnerar ctx.Err() efter avslutad
// slutdränering inom budget.
func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.stopBudget)
			completed, failed, derr := w.store.DrainOnce(stopCtx, "", w.batch, w.lease, w.broker)
			cancel()
			logging.GetFromContext(ctx).Debug("projection worker stopped",
				"completed", completed, "failed", failed, "drain_err", derr)
			return ctx.Err()
		case <-ticker.C:
			completed, failed, derr := w.store.DrainOnce(ctx, "", w.batch, w.lease, w.broker)
			if derr != nil && ctx.Err() == nil {
				logging.GetFromContext(ctx).Error("projection worker drain failed", "err", derr.Error())
			} else if completed+failed > 0 {
				logging.GetFromContext(ctx).Debug("projection worker drained",
					"completed", completed, "failed", failed)
			}
		}
	}
}
