package functions

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/diwise/context-broker/pkg/datamodels/fiware"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	prop "github.com/diwise/context-broker/pkg/ngsild/types/properties"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/cip"
	. "github.com/diwise/iot-transform-fiware/internal/pkg/application/decorators"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

type waterquality struct {
	Temperature float64   `json:"temperature"`
	Timestamp   time.Time `json:"timestamp"`
}

type counter struct {
	Count int  `json:"count"`
	State bool `json:"state"`
}

type level struct {
	Current float64  `json:"current"`
	Percent *float64 `json:"percent,omitempty"`
	Offset  *float64 `json:"offset,omitempty"`
}

type presence struct {
	State bool `json:"state"`
}

type sewagepumpingstation struct {
	ID        string    `json:"id"`
	State     bool      `json:"state"`
	Timestamp time.Time `json:"timestamp"`
	Location  *location `json:"location,omitempty"`
	Tenant    string    `json:"tenant"`
}

type location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type Func struct {
	ID       string    `json:"id"`
	Type     string    `json:"type"`
	SubType  string    `json:"subtype"`
	Location *location `json:"location,omitempty"`
	Tenant   string    `json:"tenant,omitempty"`
	Source   string    `json:"source,omitempty"`

	Counter      *counter      `json:"counter,omitempty"`
	Level        *level        `json:"level,omitempty"`
	Presence     *presence     `json:"presence,omitempty"`
	WaterQuality *waterquality `json:"waterquality,omitempty"`

	Timestamp time.Time
}

var statusValue = map[bool]string{true: "on", false: "off"}

func SewagePumpingStation(ctx context.Context, incMsg messaging.IncomingTopicMessage, cbClient client.ContextBrokerClient) error {
	sps := sewagepumpingstation{}

	err := json.Unmarshal(incMsg.Body(), &sps)
	if err != nil {
		return err
	}

	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	timestamp := ""

	if sps.Timestamp.IsZero() {
		timestamp = time.Now().UTC().Format(time.RFC3339)
		properties = append(properties, decorators.DateObserved(timestamp))
	} else {
		timestamp = sps.Timestamp.Format(time.RFC3339)
		properties = append(properties, decorators.DateObserved(timestamp))
	}

	properties = append(properties,
		decorators.Status(statusValue[sps.State], prop.TxtObservedAt(timestamp)),
	)

	if sps.Location != nil {
		properties = append(properties, decorators.Location(sps.Location.Latitude, sps.Location.Longitude))
	}

	typeName := "SewagePumpingStation"
	id := fmt.Sprintf("urn:ngsi-ld:%s:%s", typeName, sps.ID)

	return cip.MergeOrCreate(ctx, cbClient, id, typeName, properties)
}

func WaterQualityObserved(ctx context.Context, fn Func, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	id := fmt.Sprintf("%s%s:%s", fiware.WaterQualityObservedIDPrefix, fn.SubType, fn.ID)

	properties = append(properties,
		decorators.DateObserved(fn.WaterQuality.Timestamp.UTC().Format(time.RFC3339)),
		Temperature(fn.WaterQuality.Temperature, fn.WaterQuality.Timestamp.UTC()),
	)

	if fn.Source != "" {
		properties = append(properties, decorators.Source(fn.Source))
	}

	if fn.Location != nil {
		properties = append(properties, decorators.Location(fn.Location.Latitude, fn.Location.Longitude))
	}

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.WaterQualityObservedTypeName, properties)
}

func WasteContainer(ctx context.Context, msg messaging.IncomingTopicMessage, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0)

	log := logging.GetFromContext(ctx)

	wc := struct {
		ID             string    `json:"id"`
		Level          *float64  `json:"level,omitempty"`
		Percent        *float64  `json:"percent,omitempty"`
		Temperature    *float64  `json:"temperature,omitempty"`
		DateObserved   time.Time `json:"dateObserved"`
		Tenant         string    `json:"tenant"`
		WasteContainer *struct {
			Location struct {
				Latitude  float64 `json:"latitude"`
				Longitude float64 `json:"longitude"`
			} `json:"location"`
		} `json:"wastecontainer,omitempty"`
	}{}

	err := json.Unmarshal(msg.Body(), &wc)
	if err != nil {
		return err
	}

	id := fmt.Sprintf("%s:%s", "urn:ngsi-ld:WasteContainer", wc.ID)

	log = log.With(slog.String("entity_id", id))
	ctx = logging.NewContextWithLogger(ctx, log)

	if wc.Percent != nil {
		properties = append(properties, FillingLevel(*wc.Percent, wc.DateObserved))
	}

	if wc.Temperature != nil {
		properties = append(properties, Temperature(*wc.Temperature, wc.DateObserved))
	}

	if wc.WasteContainer != nil {
		properties = append(properties, decorators.Location(wc.WasteContainer.Location.Latitude, wc.WasteContainer.Location.Longitude))
	}

	return cip.MergeOrCreate(ctx, cbClient, id, "WasteContainer", properties)
}

func Sewer(ctx context.Context, incMsg messaging.IncomingTopicMessage, cbClient client.ContextBrokerClient) error {
	sewer := struct {
		ID        string    `json:"id"`
		DeviceID  *string   `json:"deviceID,omitempty"`
		Percent   *float64  `json:"percent,omitempty"`
		Level     float64   `json:"level"`
		Timestamp time.Time `json:"timestamp"`
		Location  *location `json:"location,omitempty"`
		Tenant    string    `json:"tenant"`
		Sewer     *struct {
			Location struct {
				Latitude  float64 `json:"latitude"`
				Longitude float64 `json:"longitude"`
			} `json:"location"`
			Properties map[string]any `json:"properties,omitempty"`
		} `json:"sewer,omitempty"`
	}{}

	err := json.Unmarshal(incMsg.Body(), &sewer)
	if err != nil {
		return err
	}

	typeName := "Sewer"
	id := fmt.Sprintf("urn:ngsi-ld:%s:%s", typeName, sewer.ID)

	log := logging.GetFromContext(ctx).With(slog.String("entity_id", id))

	properties := make([]entities.EntityDecoratorFunc, 0, 5)
	timestamp := sewer.Timestamp.Format(time.RFC3339)

	if sewer.Timestamp.IsZero() {
		log.Debug("timestamp was zero, set to Now()")
		timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	properties = append(properties, decorators.DateObserved(timestamp))

	if sewer.Sewer != nil {
		if len(sewer.Sewer.Properties) > 0 {
			if prop, ok := sewer.Sewer.Properties["description"]; ok {
				if desc, ok := prop.(string); ok {
					log.Debug("add sewer information (description)")
					properties = append(properties, Description(desc))
				}
			}
		}
		properties = append(properties, decorators.Location(sewer.Sewer.Location.Latitude, sewer.Sewer.Location.Longitude))
	}

	if sewer.Percent != nil {
		properties = append(properties, decorators.Number("percent", *sewer.Percent, prop.ObservedAt(timestamp)))
	}

	if sewer.DeviceID != nil {
		deviceID := fmt.Sprintf("urn:ngsi-ld:%s:%s", "Device", *sewer.DeviceID)
		properties = append(properties, decorators.RefDevice(deviceID))
		properties = append(properties, decorators.Source(*sewer.DeviceID))
	}

	properties = append(properties, decorators.Number("level", sewer.Level, prop.ObservedAt(timestamp)))

	return cip.MergeOrCreate(ctx, cbClient, id, typeName, properties)
}

func CombinedSewageOverflow(ctx context.Context, incMsg messaging.IncomingTopicMessage, cbClient client.ContextBrokerClient) error {
	cso := struct {
		ID                     string        `json:"id"`
		Type                   string        `json:"type"`
		CumulativeTime         time.Duration `json:"cumulativeTime"`
		DateObserved           time.Time     `json:"dateObserved"`
		State                  bool          `json:"state"`
		StateChanged           bool          `json:"stateChanged"`
		Tenant                 string        `json:"tenant"`
		CombinedSewageOverflow *struct {
			Location struct {
				Latitude  float64 `json:"latitude"`
				Longitude float64 `json:"longitude"`
			} `json:"location"`
		} `json:"combinedsewageoverflow,omitempty"`
	}{}

	properties := make([]entities.EntityDecoratorFunc, 0, 4)

	err := json.Unmarshal(incMsg.Body(), &cso)
	if err != nil {
		return err
	}

	log := logging.GetFromContext(ctx)

	typeName := "CombinedSewageOverflow"
	entityID := fmt.Sprintf("urn:ngsi-ld:%s:%s", typeName, cso.ID)
	observedAt := cso.DateObserved.UTC().Format(time.RFC3339)

	log.Debug(fmt.Sprintf("handle %s", entityID))

	properties = append(properties, decorators.DateObserved(observedAt))

	if cso.StateChanged {
		log.Debug("state changed for CombinedSewageOverflow")
		properties = append(properties, decorators.Status(fmt.Sprintf("%t", cso.State), prop.TxtObservedAt(observedAt)))
	} else {
		log.Debug("state has not changed")
		properties = append(properties, decorators.Status(fmt.Sprintf("%t", cso.State)))
	}

	if cso.CombinedSewageOverflow != nil {
		log.Debug("appned location to CombinedSewageOverflow")
		properties = append(properties, decorators.Location(cso.CombinedSewageOverflow.Location.Latitude, cso.CombinedSewageOverflow.Location.Longitude))
	}

	return cip.MergeOrCreate(ctx, cbClient, entityID, typeName, properties)
}
