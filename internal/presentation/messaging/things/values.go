package things

import (
	"context"
	"errors"
	"log/slog"

	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	delivery "github.com/diwise/iot-transform-fiware/internal/presentation/messaging"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

// NewThingValuesTopicMessageHandler hanterar versionerade värdesevent
// (things.v1.values) med samma konfigurationsstyrda motor som thing.updated.
// Fel-/retry-semantiken är identisk: strukturskada är Permanent, tom tenant
// och saknad regel skippas utan broker-kontakt, brokerfel propageras för
// retry. Samma DynamicThingTransformer-instans återanvänds; händelsekällan
// (match.event) skiljer gamla och nya regler åt så de aldrig korsaktiveras.
// C1: delta/snapshot/removed/relations exponeras separat av adaptern
// (values==delta alias en release); ingen recency-filtrering sker och ingen
// modellkunskap finns i handlern. Context/trace bevaras via delivery.Begin
// till E/F och broker; loggar bär aldrig payloads/hemligheter.
// C2: E:s skrivplan förvalideras före I/O (hel batch ValidateEntityWrites +
// EntityWritesToPending utan brokerkontakt, fallback-create validerad före
// dess anrop i brokerlagret); diagnostik via MatchedRule/ExplainSkipped
// loggas utan brokerkontakt vid no-match/undertryckt.
func NewThingValuesTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc, provider TransformerProvider, defaultContext string) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) (err error) {
		ctx, l, end := delivery.Begin(ctx, l, "handle-thing-values")
		defer func() { end(err) }()
		log := l.With("content_type", itm.ContentType())

		th, err := appthings.ThingValuesFromJSON(itm.Body())
		if err != nil {
			log.Error("failed to unmarshal message body", "err", err.Error())
			return messaging.Permanent(err)
		}
		// Additiv tolerans (C1): schema/content-type avvisas aldrig här;
		// oväntade värden loggas men hanteras vidare. Struktur/identitet
		// (tom thingId, dubbletter, konflikter, NaN/Inf) är Permanent via
		// adaptern; tom tenant skippar nedan utan broker-kontakt.
		if schema, ok := th.ResolvePath("event.schema"); ok {
			if s, _ := schema.(string); s != "" && s != "v1" {
				log.Debug("unexpected event schema, tolerating additively", "schema", s)
			}
		}
		if ct := itm.ContentType(); ct != "" && ct != "application/vnd.diwise.things.v1+json" {
			log.Debug("unexpected content type, tolerating additively", "content_type", ct)
		}
		if eventID, ok := th.ResolvePath("event.eventId"); ok {
			log = log.With("message_id", eventID)
		}
		log = log.With("thing_id", th.ID, "tenant", th.Tenant)
		ctx = logging.NewContextWithLogger(ctx, log)

		if th.Tenant == "" {
			log.Debug("message contains no tenant, skipping")
			return nil
		}

		writes, err := provider.ThingsTransformer().Transform(th)
		if err != nil {
			if errors.Is(err, transform.ErrNoMatchingRule) {
				// C2-diagnostik via E:s MatchedRule/ExplainSkipped när
				// providern bär dem; annars generisk no-match utan
				// brokerkontakt. Inga payloads i loggar.
				logNoWrites(log, provider, th)
				return nil
			}

			log.Error("transform failed", "err", err.Error())
			return delivery.DataError(err)
		}

		if len(writes) == 0 {
			// Matchad men undertryckt (required saknas m.fl.): E:s
			// MatchedRule ger vald regel/revision, ExplainSkipped ger
			// precisa koder per entitet/attribut. Ingen brokerkontakt.
			logNoWrites(log, provider, th)
			return nil
		}
		// Förvalidera relevanta skrivningar före I/O: hela batchen via
		// ValidateEntityWrites (faktisk serialiserad NGSI-LD) och
		// EntityWritesToPending (stabila nycklar, samma validering som
		// projektionens PlanFunc-closure använder). Fel är Permanent
		// utan brokerkontakt; fallback-create (404-till-create)
		// valideras före dess anrop i brokerlagret.
		if err := validateBeforeIO(writes, th.Tenant, defaultContext); err != nil {
			log.Error("entity writes invalid before broker call", "err", err.Error())
			return err
		}

		log = log.With(slog.String("entity_id", writes[0].EntityID), slog.String("type_name", writes[0].TypeName), slog.String("tenant", th.Tenant))
		ctx = logging.NewContextWithLogger(ctx, log)

		cbClient, err := cbClientFn(th.Tenant)
		if err != nil {
			// Token-/fabriksfel: ingen brokerkontakt har skett.
			log.Error("failed to create context broker client", "err", err.Error())
			return delivery.DependencyError(err)
		}

		if err := contextbroker.ApplyEntityWrites(ctx, cbClient, writes, defaultContext); err != nil {
			// Brokerfel via DataError(Permanent 400/422/413)/
			// DependencyError(Temporary 401/403/429/500/503),
			// PartialUpdateError temporary; slutfört bevaras i
			// felrapporten via WriteBatchError Completed.
			return brokerError(ctx, err)
		}

		log.Debug("thing values handled successfully")
		return nil
	}
}
