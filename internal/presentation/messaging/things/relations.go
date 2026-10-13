package things

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/projection"
	delivery "github.com/diwise/iot-transform-fiware/internal/presentation/messaging"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

// NewThingRelationsTopicMessageHandler hanterar versionerade
// relationshändelser (things.v1.relations) med samma konfigurationsstyrda
// motor som värdeseventen. Fel-/retry-semantiken är identisk: strukturskada
// är Permanent, tom tenant och saknad regel skippas utan broker-kontakt,
// brokerfel propageras för retry. Samma DynamicThingTransformer-instans
// återanvänds; händelsekällan (match.event) skiljer regeltyperna åt så att
// värderegler aldrig aktiveras av relationshändelser och tvärtom.
// C1: previousToId/revision/frysta ändpunktsdescriptors+målversioner
// exponeras separat av adaptern; ingen recency-filtrering, inga modellnamn.
// Context/trace bevaras via delivery.Begin; loggar bär aldrig payloads.
// C2: strikt relationId per F (ChangeFromJSON + CheckRelationID, opak
// identitet — parsas ej positionellt), tom-tenant-skip före ApplyChange,
// E:s skrivplan via PlanFunc-closure (EntityWritesToPending) och diagnostik
// via MatchedRule/ExplainSkipped. Med RelationStore persisters projektion +
// väntande arbete atomärt (ApplyChange med CaptureTrace) och levereras via
// DrainOnce; utan store sker direkt ApplyEntityWrites. Ingen
// recency-filtrering, inga modellnamn.
func NewThingRelationsTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc, provider TransformerProvider, defaultContext string) messaging.TopicMessageHandler {
	return NewThingRelationsTopicMessageHandlerWithStore(cbClientFn, provider, defaultContext, nil)
}

// NewThingRelationsTopicMessageHandlerWithStore är C2:s projektionskoppling:
// set/remove/flytt persisteras atomärt med F:s ApplyChange (trace via
// CaptureTrace från F, planering via PlanFunc-closure från E:s
// EntityWritesToPending) och levereras därefter. Tom tenant skippar före
// ApplyChange utan brokerkontakt. Exakt replay av samma RelationID är
// idempotent (seen-tabell, inget nytt arbete). Revision jämförs aldrig för
// filtrering: sena event appliceras (dokumenterad avgränsning).
func NewThingRelationsTopicMessageHandlerWithStore(cbClientFn contextbroker.ContextBrokerClientFactoryFunc, provider TransformerProvider, defaultContext string, store RelationStore) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) (err error) {
		ctx, l, end := delivery.Begin(ctx, l, "handle-thing-relations")
		defer func() { end(err) }()
		log := l.With("content_type", itm.ContentType())
		raw := itm.Body()

		th, err := appthings.ThingRelationsFromJSON(raw)
		if err != nil {
			log.Error("failed to unmarshal message body", "err", err.Error())
			return messaging.Permanent(err)
		}
		if ct := itm.ContentType(); ct != "" && ct != "application/vnd.diwise.things.v1+json" {
			log.Debug("unexpected content type, tolerating additively", "content_type", ct)
		}
		log = log.With("thing_id", th.ID, "tenant", th.Tenant, "topic_name", transform.EventThingsV1Relations)
		ctx = logging.NewContextWithLogger(ctx, log)

		if th.Tenant == "" {
			log.Debug("message contains no tenant, skipping")
			return nil
		}

		// Strikt F-validering av samma trådform (C1-paritet): saknat fromId,
		// motstridiga kuvert-/deskriptoridentiteter och relationId-mismatch
		// (CheckRelationID mot v1-identitet med To, legacy utan To läsbar)
		// är Permanent. RelationId är stabil identitet för seen-tabellen;
		// ToID hämtas ur kuvertets toId via Change.
		// Tom tenant har redan skippats ovan utan brokerkontakt; Change
		// med tom tenant skippar nedan före ApplyChange.
		change, cerr := relations.ChangeFromJSON(raw)
		if cerr != nil {
			log.Error("invalid relation change", "err", cerr.Error())
			return messaging.Permanent(cerr)
		}

		// Tom-tenant-skip före ApplyChange: ingen DB-kontakt, ingen
		// brokerkontakt. Lagret nekar tom tenant vid behov.
		if change.Tenant == "" {
			log.Debug("relation change contains no tenant, skipping")
			return nil
		}

		writes, err := provider.ThingsTransformer().Transform(th)
		if err != nil {
			if errors.Is(err, transform.ErrNoMatchingRule) {
				logNoWrites(log, provider, th)
				// Persistera länken även utan regel när store finns
				// (projektionen är källans sanning för framtida Rebuild;
				// ingen brokerkontakt utan skrivningar).
				if store != nil {
					trace := projection.CaptureTrace(ctx)
					planner := plannerFromWrites(change.Tenant, nil, defaultContext)
					if _, derr := store.ApplyChange(ctx, change, trace, planner); derr != nil {
						return storeError(derr)
					}
				}
				return nil
			}

			log.Error("transform failed", "err", err.Error())
			return delivery.DataError(err)
		}

		if len(writes) == 0 {
			// Matchad men undertryckt: MatchedRule/ExplainSkipped ger
			// precisa koder utan brokerkontakt.
			logNoWrites(log, provider, th)
			if store != nil {
				trace := projection.CaptureTrace(ctx)
				planner := plannerFromWrites(change.Tenant, nil, defaultContext)
				if _, derr := store.ApplyChange(ctx, change, trace, planner); derr != nil {
					return storeError(derr)
				}
			}
			return nil
		}
		if err := validateBeforeIO(writes, change.Tenant, defaultContext); err != nil {
			log.Error("entity writes invalid before broker call", "err", err.Error())
			return err
		}

		log = log.With(
			slog.String("entity_id", writes[0].EntityID),
			slog.String("type_name", writes[0].TypeName),
			slog.String("tenant", th.Tenant),
			slog.String("relation", th.Relation),
			slog.Bool("removed", th.RelationRemoved),
		)
		ctx = logging.NewContextWithLogger(ctx, log)

		if store != nil {
			// Atomärt: projektion + väntande brokerarbete i en transaktion.
			// Planeraren är ren (ingen I/O): EntityWritesToPending via
			// PlanFunc-closure, validerad NGSI-LD före persistens.
			trace := projection.CaptureTrace(ctx)
			planner := plannerFromWrites(change.Tenant, writes, defaultContext)
			res, derr := store.ApplyChange(ctx, change, trace, planner)
			if derr != nil {
				return storeError(derr)
			}
			if res.Duplicate {
				log.Debug("duplicate relation change skipped", "pending", res.Pending)
				return nil
			}
			// Omedelbar leverans via DrainOnce med pendingBrokerFunc så
			// att DB-framgång + brokerfel är replay-/omstartssäker via
			// pending-kön och trace länkas (parent/link efter omstart).
			if s, ok := store.(*projection.Store); ok && s != nil {
				cbClient, cerr := cbClientFn(change.Tenant)
				if cerr != nil {
					log.Error("failed to create context broker client", "err", cerr.Error())
					return delivery.DependencyError(cerr)
				}
				completed, failed, derr := s.DrainOnce(ctx, change.Tenant, 10, time.Minute, pendingBrokerFunc(cbClient))
				log.Debug("relation change drained", "completed", completed, "failed", failed)
				if derr != nil {
					return delivery.DependencyError(derr)
				}
				if failed > 0 {
					return delivery.DependencyError(errPartialDelivery)
				}
				return nil
			}
			// Fallback när store inte är Drain-bar: direktväg nedan efter
			// persistens (idempotent dubbel-leverans tolereras av
			// merge/remove/delete).
		}

		cbClient, err := cbClientFn(th.Tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return delivery.DependencyError(err)
		}

		if err := contextbroker.ApplyEntityWrites(ctx, cbClient, writes, defaultContext); err != nil {
			return brokerError(ctx, err)
		}

		log.Debug("thing relation handled successfully")
		return nil
	}
}
