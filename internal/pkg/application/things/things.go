package things

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/cip"
	helpers "github.com/diwise/iot-transform-fiware/internal/pkg/application/decorators"
	"github.com/diwise/messaging-golang/pkg/messaging"

	"github.com/diwise/context-broker/pkg/datamodels/fiware"
	. "github.com/diwise/context-broker/pkg/ngsild/types/properties"
)

func NewBuildingTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
	}
}

func NewContainerTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		c := container{}
		err := json.Unmarshal(itm.Body(), &c)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		props := make([]entities.EntityDecoratorFunc, 0)

		props = append(props, helpers.FillingLevel(c.Percent, c.ObservedAt))
		props = append(props, decorators.Location(c.Location.Latitude, c.Location.Longitude))

		err = cip.MergeOrCreate(ctx, cbClientFn(c.Tenant), c.EntityID(), c.TypeName(), props)
		if err != nil {
			l.Error("failed to merge or create entity", slog.String("type_name", c.TypeName()), "err", err.Error())
			return
		}
	}
}

func NewLifebuoyTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		lb := lifebuoy{}
		err := json.Unmarshal(itm.Body(), &lb)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		statusValue := map[bool]string{true: "on", false: "off"}
		props := make([]entities.EntityDecoratorFunc, 0, 5)

		props = append(props, decorators.DateLastValueReported(lb.ObservedAt.UTC().Format(time.RFC3339)))
		props = append(props, decorators.Status(statusValue[lb.Presence], TxtObservedAt(lb.ObservedAt.UTC().Format(time.RFC3339))))
		props = append(props, decorators.Location(lb.Location.Latitude, lb.Location.Longitude))

		typeName := "Lifebuoy"
		entityID := fmt.Sprintf("urn:ngsi-ld:%s:%s", typeName, lb.NameOrID())

		err = cip.MergeOrCreate(ctx, cbClientFn(lb.Tenant), entityID, typeName, props)
		if err != nil {
			l.Error("failed to merge or create entity", slog.String("type_name", typeName), "err", err.Error())
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
		poi := pointOfInterest{}
		err := json.Unmarshal(itm.Body(), &poi)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		var entityID, typeNamePrefix, typeName string
		props := make([]entities.EntityDecoratorFunc, 0)

		switch strings.ToLower(poi.TypeName()) {
		case "beach":
			typeNamePrefix = fiware.WaterQualityObservedIDPrefix
			typeName = fiware.WaterConsumptionObservedTypeName
		default:
			typeNamePrefix = fiware.WeatherObservedIDPrefix
			typeName = fiware.WeatherObservedTypeName
		}

		entityID = fmt.Sprintf("%s%s", typeNamePrefix, poi.NameOrID())

		props = append(props,
			decorators.Location(poi.Location.Latitude, poi.Location.Longitude),
			decorators.DateObserved(poi.ObservedAt.UTC().Format(time.RFC3339)),
			helpers.Temperature(poi.Temperature, poi.ObservedAt.UTC()),
		)

		//TODO: add source property

		err = cip.MergeOrCreate(ctx, cbClientFn(poi.Tenant), entityID, typeName, props)
		if err != nil {
			l.Error("failed to merge or create entity", slog.String("type_name", typeName), "err", err.Error())
			return
		}
	}
}
func NewPumpingstationTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		var statusValue = map[bool]string{true: "on", false: "off"}

		p := pumpingStation{}
		err := json.Unmarshal(itm.Body(), &p)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}
		props := make([]entities.EntityDecoratorFunc, 0, 5)

		if p.ObservedAt.IsZero() {
			return
		}

		timestamp := p.ObservedAt.UTC().Format(time.RFC3339)
		props = append(props, decorators.DateObserved(timestamp))
		props = append(props, decorators.Status(statusValue[p.Observed], TxtObservedAt(timestamp)))
		props = append(props, decorators.Location(p.Location.Latitude, p.Location.Longitude))

		typeName := "SewagePumpingStation"
		entityID := fmt.Sprintf("urn:ngsi-ld:%s:%s", typeName, p.NameOrID())

		err = cip.MergeOrCreate(ctx, cbClientFn(p.Tenant), entityID, "SewagePumpingStation", props)
		if err != nil {
			l.Error("failed to merge or create entity", slog.String("type_name", "SewagePumpingStation"), "err", err.Error())
			return
		}
	}
}
func NewRoomTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		r := room{}
		err := json.Unmarshal(itm.Body(), &r)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		var entityID string
		props := make([]entities.EntityDecoratorFunc, 0)

		entityID = fmt.Sprintf("%s%s:%s", fiware.IndoorEnvironmentObservedIDPrefix, r.TypeName(), r.NameOrID())

		props = append(props, decorators.Location(r.Location.Latitude, r.Location.Longitude))

		ts := r.ObservedAt

		if ts.IsZero() {
			l.Debug("observedAt is zero, use Now()", slog.String("type_name", fiware.IndoorEnvironmentObservedTypeName))
			ts = time.Now()
		}

		props = append(props, decorators.DateObserved(helpers.FormatTime(ts)))
		props = append(props, helpers.Temperature(r.Temperature, ts))

		err = cip.MergeOrCreate(ctx, cbClientFn(r.Tenant), entityID, fiware.IndoorEnvironmentObservedTypeName, props)
		if err != nil {
			l.Error("failed to merge or create entity", slog.String("type_name", fiware.IndoorEnvironmentObservedTypeName), "err", err.Error())
			return
		}
	}
}

func NewSewerTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		s := sewer{}
		err := json.Unmarshal(itm.Body(), &s)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		props := make([]entities.EntityDecoratorFunc, 0, 4)

		observedAt := s.ObservedAt.UTC().Format(time.RFC3339)

		props = append(props, decorators.DateObserved(observedAt))
		props = append(props, decorators.Status(fmt.Sprintf("%t", s.Observed), TxtObservedAt(observedAt)))
		props = append(props, decorators.Location(s.Location.Latitude, s.Location.Longitude))

		if s.Description != nil && *s.Description != "" {
			props = append(props, decorators.Description(*s.Description))
		}

		if s.CurrentLevel != 0 {
			props = append(props, decorators.Number("level", s.CurrentLevel, ObservedAt(observedAt)))
		}

		if s.Percent != 0 {
			props = append(props, decorators.Number("percent", s.Percent, ObservedAt(observedAt)))
		}

		if len(s.RefDevices) > 0 {
			devices := []string{}
			for _, d := range s.RefDevices {
				devices = append(devices, d.DeviceID)
			}

			if len(devices) == 1 {
				props = append(props, decorators.RefDevice(devices[0]))
				props = append(props, decorators.Source(devices[0]))
			} else {
				props = append(props, helpers.RefDevices(devices))
				props = append(props, decorators.Source(devices[0]))
			}
		}

		err = cip.MergeOrCreate(ctx, cbClientFn(s.Tenant), s.EntityID(), s.TypeName(), props)
		if err != nil {
			l.Error("failed to merge or create entity", slog.String("type_name", s.TypeName()), "err", err.Error())
			return
		}
	}
}

func NewWaterMeterTopicMessageHandler(messenger messaging.MsgContext, cbClientFn func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	return func(ctx context.Context, itm messaging.IncomingTopicMessage, l *slog.Logger) {
		w := watermeter{}
		err := json.Unmarshal(itm.Body(), &w)
		if err != nil {
			l.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		toLtr := func(m3 float64) float64 {
			return math.Floor((m3 + 0.0005) * 1000)
		}

		props := make([]entities.EntityDecoratorFunc, 0, 4)

		entityID := fmt.Sprintf("%s%s", fiware.WaterConsumptionObservedIDPrefix, w.NameOrID())

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
