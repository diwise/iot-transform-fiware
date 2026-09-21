package things

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

// TransformerProvider levererar aktuell snapshot-motor per meddelande så
// att omladdade regler slår igenom utan omregistrering (TF-05).
type TransformerProvider interface {
	ThingsTransformer() *appthings.DynamicThingTransformer
}

// NewDynamicThingTopicMessageHandler hanterar samtliga saktyper med den
// konfigurationsstyrda motorn.
func NewDynamicThingTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc, provider TransformerProvider, defaultContext string) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		log := l.With("content_type", itm.ContentType())

		th, err := appthings.ThingFromJSON(itm.Body())
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
				log.Debug("no matching rule for thing, skipping", "type", th.Type, "sub_type", th.SubType)
				return nil
			}

			log.Error("transform failed", "err", err.Error())
			return err
		}

		if len(writes) == 0 {
			return nil
		}

		log = log.With(slog.String("entity_id", writes[0].EntityID), slog.String("type_name", writes[0].TypeName), slog.String("tenant", th.Tenant))
		ctx = logging.NewContextWithLogger(ctx, log)

		cbClient, err := cbClientFn(th.Tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return err
		}

		if err := applyTransformWrites(ctx, cbClient, writes, defaultContext); err != nil {
			log.Error("failed to write entity", "err", err.Error())
			return err
		}

		log.Debug("thing handled successfully")
		return nil
	}
}

func applyTransformWrites(ctx context.Context, cbClient client.ContextBrokerClient, writes []transform.EntityWrite, defaultContext string) error {
	for _, w := range writes {
		entityContext := w.Context
		if entityContext == "" {
			entityContext = defaultContext
		}

		if w.Create {
			err := contextbroker.CreateNewEntityWithContext(ctx, cbClient, w.EntityID, w.TypeName, entityContext, w.Props)
			if err != nil && !errors.Is(err, contextbroker.ErrEntityAlreadyExists) {
				return fmt.Errorf("failed to create entity %s: %w", w.EntityID, err)
			}
			continue
		}

		if err := contextbroker.MergeOrCreateWithContext(ctx, cbClient, w.EntityID, w.TypeName, entityContext, w.Props); err != nil {
			return fmt.Errorf("failed to merge or create entity %s: %w", w.EntityID, err)
		}
	}

	return nil
}
