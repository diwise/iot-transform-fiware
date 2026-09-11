package things

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

type msg[T any] struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Thing     T         `json:"thing"`
	Tenant    string    `json:"tenant"`
	Timestamp time.Time `json:"timestamp"`
}

// applyWrites executes transformed entity writes in order against the
// broker. A harmless already-exists on create continues with the
// remaining writes; any other error aborts the delivery.
func applyWrites(ctx context.Context, cbClient client.ContextBrokerClient, writes []appthings.EntityWrite) error {
	for _, w := range writes {
		if w.Create {
			err := contextbroker.CreateNewEntity(ctx, cbClient, w.EntityID, w.TypeName, w.Props)
			if err != nil && !errors.Is(err, contextbroker.ErrEntityAlreadyExists) {
				return fmt.Errorf("failed to create entity %s: %w", w.EntityID, err)
			}
			continue
		}

		if err := contextbroker.MergeOrCreate(ctx, cbClient, w.EntityID, w.TypeName, w.Props); err != nil {
			return fmt.Errorf("failed to merge or create entity %s: %w", w.EntityID, err)
		}
	}

	return nil
}

func NewBuildingTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		return nil
	}
}

func NewContainerTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		log := l.With("content_type", itm.ContentType())
		log.Debug("container received")

		m := msg[appthings.Container]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			log.Error("failed to unmarshal message body", "err", err.Error())
			return messaging.Permanent(err)
		}

		if m.Thing.Tenant == "" {
			log.Debug("message contains no tenant, skipping")
			return nil
		}

		writes := appthings.TransformContainer(m.Thing)

		log = log.With(slog.String("entity_id", writes[0].EntityID), slog.String("type_name", writes[0].TypeName), slog.String("tenant", m.Thing.Tenant))
		ctx = logging.NewContextWithLogger(ctx, log)

		cbClient, err := cbClientFn(m.Thing.Tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return err
		}

		if err := applyWrites(ctx, cbClient, writes); err != nil {
			log.Error("failed to write entity", slog.String("type_name", writes[0].TypeName), "err", err.Error())
			return err
		}

		log.Debug("container handled successfully")
		return nil
	}
}

func NewLifebuoyTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		log := l.With("content_type", itm.ContentType())
		log.Debug("lifebuoy received")

		m := msg[appthings.Lifebuoy]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return messaging.Permanent(err)
		}

		if m.Thing.Tenant == "" {
			log.Debug("message contains no tenant, skipping")
			return nil
		}

		writes := appthings.TransformLifebuoy(m.Thing)

		log = log.With(slog.String("entity_id", writes[0].EntityID), slog.String("type_name", writes[0].TypeName), slog.String("tenant", m.Thing.Tenant))
		ctx = logging.NewContextWithLogger(ctx, log)

		cbClient, err := cbClientFn(m.Thing.Tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return err
		}

		if err := applyWrites(ctx, cbClient, writes); err != nil {
			log.Error("failed to write entity", slog.String("type_name", writes[0].TypeName), "err", err.Error())
			return err
		}

		log.Debug("lifebuoy handled successfully")
		return nil
	}
}

func NewDeskTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		log := l.With("content_type", itm.ContentType())
		log.Debug("desk received")

		m := msg[appthings.Desk]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			log.Error("failed to unmarshal message body", "err", err.Error())
			return messaging.Permanent(err)
		}

		if m.Thing.Tenant == "" {
			log.Debug("message contains no tenant, skipping")
			return nil
		}

		writes := appthings.TransformDesk(m.Thing)

		log = log.With(slog.String("entity_id", writes[0].EntityID), slog.String("type_name", writes[0].TypeName), slog.String("tenant", m.Thing.Tenant))
		ctx = logging.NewContextWithLogger(ctx, log)

		cbClient, err := cbClientFn(m.Thing.Tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return err
		}

		if err := applyWrites(ctx, cbClient, writes); err != nil {
			log.Error("failed to write entity", slog.String("type_name", writes[0].TypeName), "err", err.Error())
			return err
		}

		log.Debug("desk handled successfully")
		return nil
	}
}

func NewPassageTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		return nil
	}
}

func NewPointOfInterestTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		log := l.With("content_type", itm.ContentType())

		m := msg[appthings.PointOfInterest]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			log.Error("failed to unmarshal message body", "err", err.Error())
			return messaging.Permanent(err)
		}

		if m.Thing.Tenant == "" {
			log.Debug("message contains no tenant, skipping")
			return nil
		}

		writes := appthings.TransformPointOfInterest(m.Thing)
		last := writes[len(writes)-1]

		log = log.With(slog.String("entity_id", last.EntityID), slog.String("type_name", last.TypeName), slog.String("tenant", m.Thing.Tenant))
		ctx = logging.NewContextWithLogger(ctx, log)

		cbClient, err := cbClientFn(m.Thing.Tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return err
		}

		if err := applyWrites(ctx, cbClient, writes); err != nil {
			log.Error("could not merge or create point of interest", "err", err.Error())
			return err
		}

		log.Debug("point of interest handled successfully")
		return nil
	}
}

func NewPumpingstationTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		log := l.With("content_type", itm.ContentType())
		log.Debug("pumpingstation received")

		m := msg[appthings.PumpingStation]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			log.Error("failed to unmarshal message body", "err", err.Error())
			return messaging.Permanent(err)
		}

		if m.Thing.Tenant == "" {
			log.Debug("message contains no tenant, skipping")
			return nil
		}

		writes := appthings.TransformPumpingStation(m.Thing)

		log = log.With(slog.String("entity_id", writes[0].EntityID), slog.String("type_name", writes[0].TypeName), slog.String("tenant", m.Thing.Tenant))
		ctx = logging.NewContextWithLogger(ctx, log)

		cbClient, err := cbClientFn(m.Thing.Tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return err
		}

		if err := applyWrites(ctx, cbClient, writes); err != nil {
			log.Error("failed to merge or create SewagePumpingStation", slog.String("type_name", "SewagePumpingStation"), "err", err.Error())
			return err
		}

		log.Debug("pumpingstation handled handled successfully")
		return nil
	}
}
func NewRoomTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		log := l.With("content_type", itm.ContentType())
		log.Debug("room received")

		m := msg[appthings.Room]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			log.Error("failed to unmarshal message body", "err", err.Error())
			return messaging.Permanent(err)
		}

		if m.Thing.Tenant == "" {
			log.Debug("message contains no tenant, skipping")
			return nil
		}

		writes := appthings.TransformRoom(m.Thing)

		log = log.With(slog.String("entity_id", writes[0].EntityID), slog.String("type_name", writes[0].TypeName), slog.String("tenant", m.Thing.Tenant))
		ctx = logging.NewContextWithLogger(ctx, log)

		cbClient, err := cbClientFn(m.Thing.Tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return err
		}

		if err := applyWrites(ctx, cbClient, writes); err != nil {
			log.Error("failed to write entity", "err", err.Error())
			return err
		}

		log.Debug("room handled handled successfully")
		return nil
	}
}

func NewSewerTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		log := l.With("content_type", itm.ContentType())
		log.Debug("sewer received")

		m := msg[appthings.Sewer]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			log.Error("failed to unmarshal message body", "err", err.Error())
			return messaging.Permanent(err)
		}

		if m.Thing.Tenant == "" {
			log.Debug("message contains no tenant, skipping")
			return nil
		}

		writes := appthings.TransformSewer(m.Thing)

		log = log.With(slog.String("entity_id", writes[0].EntityID), slog.String("type_name", writes[0].TypeName), slog.String("tenant", m.Thing.Tenant), slog.String("action", m.Thing.LastAction))
		ctx = logging.NewContextWithLogger(ctx, log)

		cbClient, err := cbClientFn(m.Thing.Tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return err
		}

		if err := applyWrites(ctx, cbClient, writes); err != nil {
			log.Error("failed to merge or create Sewer", "err", err.Error())
			return err
		}

		log.Debug("sewer handled handled successfully")
		return nil
	}
}

/*
func NewWaterMeterTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) error {
		m := msg[appthings.watermeter]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return nil
		}

		w := m.Thing

		toLtr := func(m3 float64) float64 {
			return math.Floor((m3 + 0.0005) * 1000)
		}

		props := make([]entities.EntityDecoratorFunc, 0, 4)

		entityID := fmt.Sprintf("%s%s", fiware.WaterConsumptionObservedIDPrefix, w.AlternativeNameOrNameOrID())

		props = append(props, decorators.Location(w.Location.Latitude, w.Location.Longitude))
		props = append(props, decorators.DateObserved(w.ObservedAt.UTC().Format(time.RFC3339)))
		props = append(props, decorators.Number("waterConsumption", toLtr(w.CumulativeVolume), UnitCode("LTR"), ObservedAt(w.ObservedAt.UTC().Format(time.RFC3339))))

		alarmValues := map[bool]float64{true: 1, false: 0}

		props = append(props, decorators.Number("alarmStopsLeaks", alarmValues[w.Leakage], ObservedAt(w.ObservedAt.UTC().Format(time.RFC3339))))
		props = append(props, decorators.Number("alarmWaterQuality", alarmValues[w.Backflow], ObservedAt(w.ObservedAt.UTC().Format(time.RFC3339))))
		props = append(props, decorators.Number("alarmTamper", alarmValues[w.Fraud], ObservedAt(w.ObservedAt.UTC().Format(time.RFC3339))))
		//props = append(props, decorators.Number("alarmBurst", alarmValues[w.Burst]))

		if w.Description != nil && *w.Description != "" {
			props = append(props, decorators.Description(*w.Description))
		}

		err = contextbroker.MergeOrCreate(ctx, cbClientFn(w.Tenant), entityID, fiware.WaterConsumptionObservedTypeName, props)
		if err != nil {
			l.Error("failed to merge or create entity", slog.String("type_name", fiware.WaterConsumptionObservedTypeName), "err", err.Error())
			return nil
		}
	}
}
*/
