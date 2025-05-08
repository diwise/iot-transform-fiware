package things

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/cip"
	helpers "github.com/diwise/iot-transform-fiware/internal/pkg/application/decorators"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/google/uuid"

	"github.com/diwise/context-broker/pkg/datamodels/fiware"
	. "github.com/diwise/context-broker/pkg/ngsild/types/properties"
)

type msg[T any] struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Thing     T         `json:"thing"`
	Tenant    string    `json:"tenant"`
	Timestamp time.Time `json:"timestamp"`
}

func NewBuildingTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
	}
}

func NewContainerTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		m := msg[container]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		c := m.Thing

		props := make([]entities.EntityDecoratorFunc, 0)

		props = append(props, helpers.FillingLevel(c.Percent, c.ObservedAt))
		props = append(props, decorators.Location(c.Location.Latitude, c.Location.Longitude))
		props = append(props, decorators.DateObserved(c.ObservedAt.UTC().Format(time.RFC3339)))

		err = cip.MergeOrCreate(ctx, cbClientFn(c.Tenant), c.EntityID(), c.TypeName(), props)
		if err != nil {
			l.Error("failed to merge or create entity", slog.String("type_name", c.TypeName()), "err", err.Error())
			return
		}
	}
}

func NewLifebuoyTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		m := msg[lifebuoy]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		lb := m.Thing

		statusValue := map[bool]string{true: "on", false: "off"}
		props := make([]entities.EntityDecoratorFunc, 0, 5)

		props = append(props, decorators.DateLastValueReported(lb.ObservedAt.UTC().Format(time.RFC3339)))
		props = append(props, decorators.Status(statusValue[lb.Presence], TxtObservedAt(lb.ObservedAt.UTC().Format(time.RFC3339))))
		props = append(props, decorators.Location(lb.Location.Latitude, lb.Location.Longitude))

		typeName := "Lifebuoy"
		entityID := fmt.Sprintf("urn:ngsi-ld:%s:%s", typeName, lb.AlternativeNameOrNameOrID())

		err = cip.MergeOrCreate(ctx, cbClientFn(lb.Tenant), entityID, typeName, props)
		if err != nil {
			l.Error("failed to merge or create entity", slog.String("type_name", typeName), "err", err.Error())
			return
		}
	}
}

func NewDeskTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		m := msg[desk]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		desk := m.Thing

		statusValue := map[bool]string{true: "on", false: "off"}
		props := make([]entities.EntityDecoratorFunc, 0, 5)

		props = append(props, decorators.DateLastValueReported(desk.ObservedAt.UTC().Format(time.RFC3339)))
		props = append(props, decorators.Status(statusValue[desk.Presence], TxtObservedAt(desk.ObservedAt.UTC().Format(time.RFC3339))))
		props = append(props, decorators.Location(desk.Location.Latitude, desk.Location.Longitude))

		entityID := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, desk.AlternativeNameOrNameOrID())

		err = cip.MergeOrCreate(ctx, cbClientFn(desk.Tenant), entityID, fiware.DeviceTypeName, props)
		if err != nil {
			l.Error("failed to merge or create entity", slog.String("type_name", fiware.DeviceTypeName), "err", err.Error())
			return
		}
	}
}

func NewPassageTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
	}
}

func NewPointOfInterestTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		log := l.With("uuid", uuid.NewString(), "topic", itm.TopicName(), "content_type", itm.ContentType(), "body", string(itm.Body()))

		m := msg[pointOfInterest]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			log.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		poi := m.Thing

		var entityID, typeNamePrefix, typeName string
		props := make([]entities.EntityDecoratorFunc, 0)

		switch strings.ToLower(poi.TypeName()) {
		case "beach":
			typeNamePrefix = fiware.WaterQualityObservedIDPrefix
			typeName = fiware.WaterQualityObservedTypeName
		default:
			typeNamePrefix = fiware.WeatherObservedIDPrefix
			typeName = fiware.WeatherObservedTypeName
		}

		entityID = fmt.Sprintf("%s%s", typeNamePrefix, poi.AlternativeNameOrNameOrID())

		log = log.With("entity_id", entityID, "type_name", typeName)

		log.Debug("processing PointOfInterest message...")

		props = append(props,
			decorators.Location(poi.Location.Latitude, poi.Location.Longitude),
			decorators.DateObserved(poi.ObservedAt.UTC().Format(time.RFC3339)),
			helpers.Temperature(poi.Temperature, poi.ObservedAt.UTC()),
		)

		//TODO: add source property

		err = cip.MergeOrCreate(ctx, cbClientFn(poi.Tenant), entityID, typeName, props)
		if err != nil {
			log.Error("failed to merge or create entity", "err", err.Error())
			return
		}

		log.Debug("done processing PointOfInterest message")
	}
}
func NewPumpingstationTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		var statusValue = map[bool]string{true: "on", false: "off"}

		m := msg[pumpingStation]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		props := make([]entities.EntityDecoratorFunc, 0, 5)

		p := m.Thing

		observedAt := time.Now().UTC().Format(time.RFC3339)
		if !p.ObservedAt.IsZero() {
			observedAt = p.ObservedAt.UTC().Format(time.RFC3339)
		}

		if p.PumpingAt == nil {
			props = append(props, decorators.DateObserved(observedAt))
		} else {
			props = append(props, decorators.DateObserved(observedAt))
			pumpingAt := p.PumpingAt.UTC().Format(time.RFC3339)
			props = append(props, decorators.Status(statusValue[p.Pumping], TxtObservedAt(pumpingAt)))
		}

		//timestamp := p.PumpingAt.UTC().Format(time.RFC3339)
		//props = append(props, decorators.DateObserved(timestamp))
		//props = append(props, decorators.Status(statusValue[p.Pumping], TxtObservedAt(timestamp)))
		props = append(props, decorators.Location(p.Location.Latitude, p.Location.Longitude))

		typeName := "SewagePumpingStation"
		entityID := fmt.Sprintf("urn:ngsi-ld:%s:%s", typeName, p.AlternativeNameOrNameOrID())

		err = cip.MergeOrCreate(ctx, cbClientFn(p.Tenant), entityID, "SewagePumpingStation", props)
		if err != nil {
			l.Error("failed to merge or create SewagePumpingStation", slog.String("type_name", "SewagePumpingStation"), "err", err.Error())
			return
		}
	}
}
func NewRoomTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		m := msg[room]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		r := m.Thing

		var entityID string
		props := make([]entities.EntityDecoratorFunc, 0)

		entityID = fmt.Sprintf("%s%s:%s", fiware.IndoorEnvironmentObservedIDPrefix, r.TypeName(), r.AlternativeNameOrNameOrID())

		ts := r.ObservedAt

		if ts.IsZero() {
			l.Debug("observedAt is zero, use Now()", slog.String("type_name", fiware.IndoorEnvironmentObservedTypeName))
			ts = time.Now()
		}

		props = append(props, decorators.Location(r.Location.Latitude, r.Location.Longitude))
		props = append(props, decorators.DateObserved(helpers.FormatTime(ts)))
		props = append(props, helpers.Temperature(r.Temperature, ts))
		props = append(props, helpers.Humidity(r.Humidity, ts))
		props = append(props, helpers.Illuminance(r.Illuminance, ts))
		props = append(props, helpers.CO2(r.CO2, ts))
		if len(r.Name) > 0 {
			props = append(props, helpers.Name(r.Name))
		}
		if len(r.AlternativeName) > 0 {
			props = append(props, helpers.AlternativeName(r.AlternativeName))
		}

		err = cip.MergeOrCreate(ctx, cbClientFn(r.Tenant), entityID, fiware.IndoorEnvironmentObservedTypeName, props)
		if err != nil {
			l.Error("failed to merge or create entity", slog.String("type_name", fiware.IndoorEnvironmentObservedTypeName), "err", err.Error())
			return
		}
	}
}

func NewSewerTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		m := msg[sewer]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		l.Debug("processing message", slog.Any("sewer", m))

		s := m.Thing

		entityID := s.EntityID()
		typeName := s.TypeName()

		log := l.With("entity_id", entityID, "type_name", typeName, "action", s.LastAction)
		ctx = logging.NewContextWithLogger(ctx, log)

		props := make([]entities.EntityDecoratorFunc, 0, 4)
		props = append(props, decorators.Location(s.Location.Latitude, s.Location.Longitude))

		var observedAt string

		if s.ObservedAt.IsZero() {
			log.Debug("observedAt is zero, use Now()")
			observedAt = time.Now().UTC().Format(time.RFC3339)
		} else {
			observedAt = s.ObservedAt.UTC().Format(time.RFC3339)
		}

		const (
			OverflowStarted string = "overflow started"
			OverflowStopped string = "overflow stopped"
			OverflowUpdated string = "overflow updated"
			OverflowUnknown string = "overflow unknown"
		)

		if s.LastAction == OverflowUnknown {
			props = append(props, decorators.DateObserved(observedAt))
		}

		if s.LastAction == OverflowStarted || s.LastAction == OverflowUpdated {
			props = append(props, decorators.DateObserved(observedAt))
			overflowAt := s.OverflowAt.UTC().Format(time.RFC3339)

			overflow := fmt.Sprintf("%t", s.Overflow)
			props = append(props, decorators.Status(overflow, TxtObservedAt(overflowAt)))

			log.Debug("overflow started", slog.String("overflow", overflow), slog.String("observedAt", observedAt), slog.String("overflowAt", overflowAt))
		}

		if s.LastAction == OverflowStopped {
			endAt := s.OverflowEndAt.UTC().Format(time.RFC3339)
			overflowAt := s.OverflowAt.UTC().Format(time.RFC3339)
			overflow := fmt.Sprintf("%t", s.Overflow)

			props = append(props, decorators.DateObserved(observedAt))
			props = append(props, decorators.Status(overflow, TxtObservedAt(endAt)))

			log.Debug("overflow ended", slog.String("overflow", overflow), slog.String("observedAt", observedAt), slog.String("overflowAt", overflowAt), slog.String("endAt", endAt))
		}

		if s.Description != nil && *s.Description != "" {
			log.Debug("adding description", slog.String("description", *s.Description))
			props = append(props, decorators.Description(*s.Description))
		}

		if s.CurrentLevel != 0 {
			log.Debug("adding current level", slog.Float64("current_level", s.CurrentLevel))
			props = append(props, decorators.Number("level", s.CurrentLevel, ObservedAt(observedAt)))
		}

		if s.Percent != 0 {
			log.Debug("adding percent", slog.Float64("percent", s.Percent))
			props = append(props, decorators.Number("percent", s.Percent, ObservedAt(observedAt)))
		}

		if len(s.RefDevices) > 0 {
			devices := []string{}
			for _, d := range s.RefDevices {
				devices = append(devices, d.DeviceID)
			}

			if len(devices) == 1 {
				urn := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, devices[0])

				//TODO: find :: and remove it in the right place...
				if strings.Contains(urn, "::") {
					log.Debug("replacing :: with : in URN (1)", slog.String("urn", urn))
					urn = strings.ReplaceAll(urn, "::", ":")
				}

				props = append(props, decorators.RefDevice(urn))
				props = append(props, decorators.Source(urn))
			} else {
				urns := []string{}
				for _, d := range devices {
					urn := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, d)

					if strings.Contains(urn, "::") {
						log.Debug("replacing :: with : in URN (2)", slog.String("urn", urn))
						urn = strings.ReplaceAll(urn, "::", ":")
					}

					urns = append(urns, urn)
				}

				props = append(props, helpers.RefDevices(urns))
				props = append(props, decorators.Source(urns[0]))
			}
		}

		err = cip.MergeOrCreate(ctx, cbClientFn(s.Tenant), entityID, typeName, props)
		if err != nil {
			log.Error("failed to merge or create Sewer", slog.String("entity_id", entityID), slog.String("type_name", typeName), "err", err.Error())
			return
		}

		l.Debug("done processing message")
	}
}

/*
func NewWaterMeterTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		m := msg[watermeter]{}
		err := json.Unmarshal(itm.Body(), &m)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
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

		err = cip.MergeOrCreate(ctx, cbClientFn(w.Tenant), entityID, fiware.WaterConsumptionObservedTypeName, props)
		if err != nil {
			l.Error("failed to merge or create entity", slog.String("type_name", fiware.WaterConsumptionObservedTypeName), "err", err.Error())
			return
		}
	}
}
*/
