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

// NewThingRelationsTopicMessageHandler hanterar versionerade
// relationshändelser (things.v1.relations) med samma konfigurationsstyrda
// motor som värdeseventen. Fel-/retry-semantiken är identisk: strukturskada
// är Permanent, tom tenant och saknad regel skippas utan broker-kontakt,
// brokerfel propageras för retry. Samma DynamicThingTransformer-instans
// återanvänds; händelsekällan (match.event) skiljer regeltyperna åt så att
// värderegler aldrig aktiveras av relationshändelser och tvärtom.
func NewThingRelationsTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc, provider TransformerProvider, defaultContext string) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		log := l.With("content_type", itm.ContentType())

		th, err := appthings.ThingRelationsFromJSON(itm.Body())
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
				log.Debug("no matching rule for thing relation, skipping",
					"type", th.Type, "relation", th.Relation, "removed", th.RelationRemoved)
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
			slog.String("relation", th.Relation),
			slog.Bool("removed", th.RelationRemoved),
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

		log.Debug("thing relation handled successfully")
		return nil
	}
}
