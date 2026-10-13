package things

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/projection"
	delivery "github.com/diwise/iot-transform-fiware/internal/presentation/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"

	ngsilderrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
)

// TransformerProvider levererar aktuell snapshot-motor per meddelande så
// att omladdade regler slår igenom utan omregistrering (TF-05). Används av
// values-, relations- och lifecycle-handlers (thing.updated-vägen är
// borttagen, PLAN001 steg 7.6).
type TransformerProvider interface {
	ThingsTransformer() *appthings.DynamicThingTransformer
}

// diagnosticProvider är ett valfritt tillägg för C2-diagnostik via E:s
// MatchedRule/ExplainSkipped. Produktionens Models exponerar ännu endast
// TransformerProvider (A äger wiring); handlers förmedlar diagnostik via
// typassertion och faller tillbaka till generisk skip-logg utan
// brokerkontakt. Tester använder en provider som även bär Engine.
type diagnosticProvider interface {
	MatchedRule(transform.Thing) (transform.Rule, bool)
	ExplainSkipped(transform.Thing) []transform.Diagnostic
}

// RelationStore är den minimala F-ytan C2 behöver i handlers: persistens av
// projektion + väntande arbete atomärt. Konkreta *projection.Store uppfyller
// den; tester använder verklig DB (finns på 5434) eller nil för direktväg.
// A äger start/stopp/wiring och bakgrundsdränering via DrainOnce.
type RelationStore interface {
	ApplyChange(ctx context.Context, change relations.Change, trace map[string]string, planner relations.PlanFunc) (projection.Result, error)
	DeleteEndpoint(ctx context.Context, tenant, endpoint string, trace map[string]string, planner relations.PlanFunc) (projection.Result, error)
}

// plannerFromWrites bygger F:s PlanFunc-closure från E:s validerade
// skrivplan. Ren funktion (ingen I/O, ingen context): lagret anropar den
// inom transaktionsgränsen och rullar tillbaka både projektion och arbete
// vid fel. Förvaliderar hela batchen före I/O via EntityWritesToPending
// (faktisk serialiserad NGSI-LD); ogiltig output ger fel före brokerkontakt.
// Snapshoten bärs för komplett mängdplanering och tenant-avgränsning;
// nuvarande E-planering är per-händelse (direkta writes) — kompletta
// mängdskrivningar för fler-mål array:er förfinas när E exponerar
// snapshot-till-writes utan modellnamn. Ingen recency-filtrering sker här.
func plannerFromWrites(tenant string, writes []transform.EntityWrite, defaultContext string) relations.PlanFunc {
	return func(snap relations.Snapshot) ([]relations.PendingWrite, error) {
		// Tenant-avgränsning bevaras: planeraren blandar aldrig in länkar
		// från annan tenant (lagret garanterar Snapshot.Tenant).
		if snap.Tenant != "" && snap.Tenant != tenant {
			// Defensivt: fel före persistens, ingen brokerkontakt.
			return nil, delivery.DataError(fmt.Errorf("planner tenant %q does not match handler tenant %q", snap.Tenant, tenant))
		}
		return transform.EntityWritesToPending(tenant, writes, defaultContext)
	}
}

// logNoWrites rapporterar no-match/matchad-men-undertryckt utan
// brokerkontakt och utan payloads/hemligheter. Vid matchad regel loggas
// vald regel/revision/snapshotversion + ExplainSkipped-koder via E när
// providern bär dem; annars generisk skip-diagnostik. Pure diagnostik,
// ingen I/O.
func logNoWrites(log *slog.Logger, provider TransformerProvider, th transform.Thing) {
	if diag, ok := any(provider).(diagnosticProvider); ok {
		if rule, matched := diag.MatchedRule(th); matched {
			skipped := diag.ExplainSkipped(th)
			// matched=true med tomma writes = undertryckt (required saknas
			// m.fl.). Logga regelidentitet + koder, aldrig payloads.
			log.Debug("matched rule produced no entities, skipping",
				"rule_id", rule.ID, "rule_revision", rule.Revision,
				"snapshot_version", rule.SnapshotVersion,
				"skipped", skippedCodes(skipped))
			return
		}
	}
	log.Debug("no matching rule, skipping")
}

// skippedCodes extraherar D:s diagnoskoder för logg (inga payloads).
func skippedCodes(diags []transform.Diagnostic) []string {
	out := make([]string, 0, len(diags))
	for _, d := range diags {
		out = append(out, string(d.Code))
	}
	return out
}

// validateBeforeIO förvaliderar relevanta skrivningar före I/O: hela batchen
// via ValidateEntityWrites (faktisk serialiserad NGSI-LD) och därefter
// EntityWritesToPending (stabila nycklar/payloads) utan brokerkontakt.
// Fel är Permanent via DataError (ingen retry läker dem).
func validateBeforeIO(writes []transform.EntityWrite, tenant, defaultContext string) error {
	if err := transform.ValidateEntityWrites(writes, defaultContext); err != nil {
		return delivery.DataError(err)
	}
	// Samma validering som projektionens PlanFunc använder — säkerställer
	// produktions/preview-paritet och att fallback-create (404-till-create)
	// validerats före dess anrop (BuildEntity i brokerlagret).
	if _, err := transform.EntityWritesToPending(tenant, writes, defaultContext); err != nil {
		return delivery.DataError(err)
	}
	return nil
}

// brokerError bevarar slutförda operationer i felrapporten (WriteBatchError
// Completed/Operation loggas, aldrig payloads) och klassificerar via
// delivery.DependencyError: Permanent för 400/422/413, Temporary för
// 401/403/429/500/503 och PartialUpdateError. Replay från början är
// idempotent (merge/remove/delete tolererar redan-utfört).
func brokerError(ctx context.Context, err error) error {
	log := logging.GetFromContext(ctx)
	if completed, operation, ok := delivery.WriteBatchProgress(err); ok {
		log.Error("broker write failed, completed preserved for replay",
			"completed", completed, "operation", operation, "err", err.Error())
	} else {
		log.Error("broker write failed", "err", err.Error())
	}
	return delivery.DependencyError(err)
}

// rawFragment är en ogenomskinlig NGSI-LD-payload för pending-leverans via
// BrokerClient.MergeEntity (endast MarshalJSON används av http-klienten;
// ForEachAttribute är no-op likt brokerlagrets null-fragment).
type rawFragment struct {
	raw []byte
}

func (f rawFragment) ForEachAttribute(func(attributeType, attributeName string, contents any)) error {
	return nil
}

func (f rawFragment) MarshalJSON() ([]byte, error) {
	out := append([]byte(nil), f.raw...)
	if len(out) == 0 {
		out = []byte(`{}`)
	}
	return out, nil
}

// rawEntity är en ogenomskinlig entitet för pending-leverans via
// BrokerClient.CreateEntity (endast ID/Type/MarshalJSON används av
// http-klienten; ForEachAttribute/KeyValues är stubbar).
type rawEntity struct {
	id  string
	typ string
	raw []byte
}

func (e rawEntity) ForEachAttribute(func(attributeType, attributeName string, contents any)) error {
	return nil
}

func (e rawEntity) MarshalJSON() ([]byte, error) {
	out := append([]byte(nil), e.raw...)
	if len(out) == 0 {
		out = []byte(`{}`)
	}
	return out, nil
}

func (e rawEntity) ID() string { return e.id }

func (e rawEntity) Type() string { return e.typ }

func (e rawEntity) KeyValues() types.EntityKeyValueMapper { return nil }

// nullFragment bygger PATCH-null för removeAttributes (saknad entitet är
// framgång vid leverans, likt brokerlagrets removeAttributes).
type nullFragment struct {
	attrs []string
}

func (f nullFragment) ForEachAttribute(func(attributeType, attributeName string, contents any)) error {
	return nil
}

func (f nullFragment) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, len(f.attrs))
	for _, a := range f.attrs {
		m[a] = nil
	}
	return json.Marshal(m)
}

func nullFragmentFor(attrs []string) nullFragment {
	return nullFragment{attrs: append([]string(nil), attrs...)}
}

func isNotFound(err error) bool {
	return errors.Is(err, ngsilderrors.ErrNotFound)
}

func isAlreadyExists(err error) bool {
	return errors.Is(err, ngsilderrors.ErrAlreadyExists)
}

// pendingBrokerFunc adapterar en BrokerClient till F:s BrokerFunc för
// DrainOnce-replay efter omstart. Ogenomskinlig payload tolkas aldrig som
// modell: operation styr verb (create/merge/removeAttributes/delete),
// EntityID styr mål, Payload bär redan validerad NGSI-LD-JSON från E.
// Delete/remove tolererar 404 (idempotent); merge faller tillbaka till
// create vid 404 och hanterar 409-race via återmmerge; partiella 207-fel
// propageras för retry med slutfört bevarat uppströms.
func pendingBrokerFunc(cbClient contextbroker.BrokerClient) func(ctx context.Context, item projection.PendingItem) error {
	return func(ctx context.Context, item projection.PendingItem) error {
		return deliverPendingItem(ctx, cbClient, item)
	}
}

func deliverPendingItem(ctx context.Context, cbClient contextbroker.BrokerClient, item projection.PendingItem) error {
	switch item.Operation {
	case relations.OpDelete:
		_, err := cbClient.DeleteEntity(ctx, item.EntityID)
		if err != nil && isNotFound(err) {
			return nil
		}
		return err
	case relations.OpRemoveAttributes:
		attrs := pendingRemoveAttributes(item.Payload)
		if len(attrs) == 0 {
			return nil
		}
		frag := nullFragmentFor(attrs)
		_, err := cbClient.MergeEntity(ctx, item.EntityID, frag, map[string][]string{"Content-Type": {"application/ld+json"}})
		if err != nil && isNotFound(err) {
			return nil
		}
		return err
	case relations.OpCreate:
		ent := rawEntity{id: item.EntityID, typ: pendingEntityType(item.Payload), raw: item.Payload}
		_, err := cbClient.CreateEntity(ctx, ent, map[string][]string{"Content-Type": {"application/ld+json"}})
		if err != nil && isAlreadyExists(err) {
			// 409-race: samtidiga creates — fall tillbaka till merge.
			_, merr := cbClient.MergeEntity(ctx, item.EntityID, rawFragment{raw: item.Payload}, map[string][]string{"Content-Type": {"application/ld+json"}})
			if merr != nil && isNotFound(merr) {
				return nil
			}
			return merr
		}
		return err
	default: // OpMerge och okända verb behandlas som merge-or-create.
		_, err := cbClient.MergeEntity(ctx, item.EntityID, rawFragment{raw: item.Payload}, map[string][]string{"Content-Type": {"application/ld+json"}})
		if err != nil && isNotFound(err) {
			ent := rawEntity{id: item.EntityID, typ: pendingEntityType(item.Payload), raw: item.Payload}
			_, cerr := cbClient.CreateEntity(ctx, ent, map[string][]string{"Content-Type": {"application/ld+json"}})
			if cerr != nil && isAlreadyExists(cerr) {
				_, merr := cbClient.MergeEntity(ctx, item.EntityID, rawFragment{raw: item.Payload}, map[string][]string{"Content-Type": {"application/ld+json"}})
				return merr
			}
			return cerr
		}
		return err
	}
}

func pendingRemoveAttributes(payload []byte) []string {
	var doc struct {
		RemoveAttributes []string `json:"removeAttributes"`
	}
	if err := json.Unmarshal(payload, &doc); err != nil {
		return nil
	}
	return doc.RemoveAttributes
}

func pendingEntityType(payload []byte) string {
	var doc struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &doc); err != nil {
		return ""
	}
	return doc.Type
}

// errPartialDelivery signalerar delvis lyckad leverans via DrainOnce:
// slutförda bevaras, återstående kräver retry (Temporary, idempotent
// replay från början). Ingen payload i felet.
var errPartialDelivery = errors.New("broker partially delivered batch")

// storeError klassificerar F-lagrets fel utan att ändra ack/retry-kontrakt
// i smyg: struktur/identitet (ErrInvalid, inkl. strikt v1-relationId med To
// per F, dubblett/mismatch per C1) och deterministiska gränskonflikter
// (ErrConflict: kardinalitet, måltyp, hierarkicykel, tenantbrott) är
// Permanent via DataError; DB/infra-fel är Temporary via DependencyError.
// Tom tenant nekas av lagret men ska redan ha skippats före ApplyChange
// utan brokerkontakt.
func storeError(err error) error {
	if errors.Is(err, projection.ErrInvalid) || errors.Is(err, projection.ErrConflict) {
		return delivery.DataError(err)
	}
	return delivery.DependencyError(err)
}

// WorkerBrokerFunc adapterar en klientfabrik till F:s BrokerFunc för A:s
// bakgrundsdränering (Worker + sista dränering vid shutdown). Klienten löses
// per post-tenant; fabriksfel propageras för retry. Ogenomskinlig payload
// tolkas aldrig som modell (se deliverPendingItem).
func WorkerBrokerFunc(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) func(ctx context.Context, item projection.PendingItem) error {
	return func(ctx context.Context, item projection.PendingItem) error {
		cbClient, err := cbClientFn(item.Tenant)
		if err != nil {
			return err
		}
		return deliverPendingItem(ctx, cbClient, item)
	}
}
