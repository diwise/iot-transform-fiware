package things

import (
	"context"
	"errors"
	"log/slog"
	"time"

	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/projection"
	delivery "github.com/diwise/iot-transform-fiware/internal/presentation/messaging"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

// NewThingLifecycleTopicMessageHandler hanterar versionerade
// livscykelhändelser (things.v1.lifecycle) med samma konfigurationsstyrda
// motor som värdes- och relationshändelser. Fel-/retry-semantiken är
// identisk: strukturskada är Permanent, tom tenant och saknad regel skippas
// utan broker-kontakt, brokerfel propageras för retry. Samma
// DynamicThingTransformer-instans återanvänds; händelsekällan (match.event)
// + match.lifecycle skiljer regeltyperna åt så att livscykelregler aldrig
// aktiveras av andra händelser och tvärtom. Delete-skrivningar är
// idempotenta (404 = OK) via den delade skrivaren.
// C1: komplett descriptor-underlag för saker utan mätningar exponeras av
// adaptern; ingen recency-filtrering, inga modellnamn. Context/trace bevaras.
// C2: E:s skrivplan förvalideras före I/O (hel batch ValidateEntityWrites +
// EntityWritesToPending, fallback-create före dess anrop); diagnostik via
// MatchedRule/ExplainSkipped utan brokerkontakt. Med RelationStore kopplas
// F:s DeleteEndpoint (med CaptureTrace och PlanFunc-closure via
// EntityWritesToPending) för borttagna mål; utan store sker direkt
// ApplyEntityWrites. Ingen recency-filtrering, inga modellnamn, inga
// payloads/hemligheter i loggar.
func NewThingLifecycleTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc, provider TransformerProvider, defaultContext string) messaging.TopicMessageHandler {
	return NewThingLifecycleTopicMessageHandlerWithStore(cbClientFn, provider, defaultContext, nil)
}

// NewThingLifecycleTopicMessageHandlerWithStore är C2:s projektionskoppling
// för lifecycle: deleted lossar atomärt samtliga länkar där målet är
// from-/to-ändpunkt via F:s DeleteEndpoint (trace via CaptureTrace,
// planering via PlanFunc-closure från E:s EntityWritesToPending) och
// levererar därefter via DrainOnce med pendingBrokerFunc-adapter.
// Tom tenant skippar före DeleteEndpoint utan brokerkontakt. Nil store ger
// direktvägen (samma som utan projektion) så att A kan koppla in F utan att
// ändra ack/retry-kontrakt i smyg.
func NewThingLifecycleTopicMessageHandlerWithStore(cbClientFn contextbroker.ContextBrokerClientFactoryFunc, provider TransformerProvider, defaultContext string, store RelationStore) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) (err error) {
		ctx, l, end := delivery.Begin(ctx, l, "handle-thing-lifecycle")
		defer func() { end(err) }()
		log := l.With("content_type", itm.ContentType())
		raw := itm.Body()

		th, err := appthings.ThingLifecycleFromJSON(raw)
		if err != nil {
			log.Error("failed to unmarshal message body", "err", err.Error())
			return messaging.Permanent(err)
		}
		if ct := itm.ContentType(); ct != "" && ct != "application/vnd.diwise.things.v1+json" {
			log.Debug("unexpected content type, tolerating additively", "content_type", ct)
		}
		if eventID, ok := th.ResolvePath("event.eventId"); ok {
			log = log.With("message_id", eventID)
		}
		log = log.With("thing_id", th.ID, "tenant", th.Tenant, "topic_name", transform.EventThingsV1Lifecycle)
		ctx = logging.NewContextWithLogger(ctx, log)

		if th.Tenant == "" {
			log.Debug("message contains no tenant, skipping")
			return nil
		}

		writes, err := provider.ThingsTransformer().Transform(th)
		if err != nil {
			if errors.Is(err, transform.ErrNoMatchingRule) {
				logNoWrites(log, provider, th)
				// Även utan regel ska borttaget mål rensas i projektionen
				// när store finns (delete-policy utan brokerkontakt om
				// inget att planera).
				if store != nil && th.Lifecycle == string(transform.LifecycleDeleted) {
					trace := projection.CaptureTrace(ctx)
					planner := plannerFromWrites(th.Tenant, nil, defaultContext)
					if _, derr := store.DeleteEndpoint(ctx, th.Tenant, th.ID, trace, planner); derr != nil {
						return storeError(derr)
					}
				}
				return nil
			}

			log.Error("transform failed", "err", err.Error())
			return delivery.DataError(err)
		}

		if len(writes) == 0 {
			logNoWrites(log, provider, th)
			if store != nil && th.Lifecycle == string(transform.LifecycleDeleted) {
				trace := projection.CaptureTrace(ctx)
				planner := plannerFromWrites(th.Tenant, nil, defaultContext)
				if _, derr := store.DeleteEndpoint(ctx, th.Tenant, th.ID, trace, planner); derr != nil {
					return storeError(derr)
				}
			}
			return nil
		}
		if err := validateBeforeIO(writes, th.Tenant, defaultContext); err != nil {
			log.Error("entity writes invalid before broker call", "err", err.Error())
			return err
		}

		log = log.With(
			slog.String("entity_id", writes[0].EntityID),
			slog.String("type_name", writes[0].TypeName),
			slog.String("tenant", th.Tenant),
			slog.String("lifecycle", th.Lifecycle),
		)
		ctx = logging.NewContextWithLogger(ctx, log)

		// F-projektion för deleted: persistera detach + väntande arbete
		// atomärt före broker-I/O, med E:s EntityWritesToPending via
		// PlanFunc-closure och trace via CaptureTrace. DB-framgång följd
		// av brokerfel är replay-/omstartssäker via pending-kön.
		if store != nil && th.Lifecycle == string(transform.LifecycleDeleted) {
			trace := projection.CaptureTrace(ctx)
			planner := plannerFromWrites(th.Tenant, writes, defaultContext)
			if _, derr := store.DeleteEndpoint(ctx, th.Tenant, th.ID, trace, planner); derr != nil {
				return storeError(derr)
			}
			// Omedelbar leverans via DrainOnce med pendingBrokerFunc så
			// att slutfört bevaras och trace länkas (parent/link efter
			// omstart verifieras i F-tester). Partiella fel ger temporary.
			if s, ok := store.(*projection.Store); ok && s != nil {
				cbClient, cerr := cbClientFn(th.Tenant)
				if cerr != nil {
					log.Error("failed to create context broker client", "err", cerr.Error())
					return delivery.DependencyError(cerr)
				}
				completed, failed, derr := s.DrainOnce(ctx, th.Tenant, 10, time.Minute, pendingBrokerFunc(cbClient))
				log.Debug("lifecycle delete drained", "completed", completed, "failed", failed)
				if derr != nil {
					return delivery.DependencyError(derr)
				}
				if failed > 0 {
					return delivery.DependencyError(errPartialDelivery)
				}
				return nil
			}
			// Fallback när store inte är konkret Drain-bar: direktväg nedan
			// efter persistens (idempotent dubbel-leverans tolereras).
		}

		cbClient, err := cbClientFn(th.Tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return delivery.DependencyError(err)
		}

		if err := contextbroker.ApplyEntityWrites(ctx, cbClient, writes, defaultContext); err != nil {
			return brokerError(ctx, err)
		}

		log.Debug("thing lifecycle handled successfully")
		return nil
	}
}
