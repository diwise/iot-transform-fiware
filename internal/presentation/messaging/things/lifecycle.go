package things

import (
	"context"
	"errors"
	"log/slog"

	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
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
func NewThingLifecycleTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc, provider TransformerProvider, defaultContext string) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		log := l.With("content_type", itm.ContentType())

		th, err := appthings.ThingLifecycleFromJSON(itm.Body())
		if err != nil {
			log.Error("failed to unmarshal message body", "err", err.Error())
			return messaging.Permanent(err)
		}

		if th.Tenant == "" {
			log.Debug("message contains no tenant, skipping")
			return nil
		}

		writes, err := provider.ThingsTransformer().Transform(th)
		if err != nil {
			if errors.Is(err, transform.ErrNoMatchingRule) {
				log.Debug("no matching rule for thing lifecycle, skipping",
					"type", th.Type, "lifecycle", th.Lifecycle)
				return nil
			}

			log.Error("transform failed", "err", err.Error())
			return err
		}

		if len(writes) == 0 {
			return nil
		}

		log = log.With(
			slog.String("entity_id", writes[0].EntityID),
			slog.String("type_name", writes[0].TypeName),
			slog.String("tenant", th.Tenant),
			slog.String("lifecycle", th.Lifecycle),
		)
		ctx = logging.NewContextWithLogger(ctx, log)

		cbClient, err := cbClientFn(th.Tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return err
		}

		if err := contextbroker.ApplyEntityWrites(ctx, cbClient, writes, defaultContext); err != nil {
			log.Error("failed to write entity", "err", err.Error())
			return err
		}

		log.Debug("thing lifecycle handled successfully")
		return nil
	}
}
