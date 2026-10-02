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
				log.Debug("no matching rule for thing values, skipping", "type", th.Type, "sub_type", th.SubType)
				return nil
			}

			log.Error("transform failed", "err", err.Error())
			return delivery.DataError(err)
		}

		if len(writes) == 0 {
			return nil
		}
		if err := transform.ValidateEntityWrites(writes, defaultContext); err != nil {
			return err
		}

		log = log.With(slog.String("entity_id", writes[0].EntityID), slog.String("type_name", writes[0].TypeName), slog.String("tenant", th.Tenant))
		ctx = logging.NewContextWithLogger(ctx, log)

		cbClient, err := cbClientFn(th.Tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return delivery.DependencyError(err)
		}

		if err := contextbroker.ApplyEntityWrites(ctx, cbClient, writes, defaultContext); err != nil {
			log.Error("failed to write entity", "err", err.Error())
			return delivery.DependencyError(err)
		}

		log.Debug("thing values handled successfully")
		return nil
	}
}
