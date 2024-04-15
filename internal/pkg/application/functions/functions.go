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

type sewer struct {
	ID        string    `json:"id"`
	Distance  float64   `json:"distance"`
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
	sewer := sewer{}

	err := json.Unmarshal(incMsg.Body(), &sewer)
	if err != nil {
		return err
	}

	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	timestamp := ""

	if sewer.Timestamp.IsZero() {
		timestamp = time.Now().UTC().Format(time.RFC3339)
		properties = append(properties, decorators.DateObserved(timestamp))
	} else {
		timestamp = sewer.Timestamp.Format(time.RFC3339)
		properties = append(properties, decorators.DateObserved(timestamp))
	}

	properties = append(properties,
		decorators.Number("distance", sewer.Distance),
	)

	if sewer.Location != nil {
		properties = append(properties, decorators.Location(sewer.Location.Latitude, sewer.Location.Longitude))
	}

	typeName := "Sewer"
	id := fmt.Sprintf("urn:ngsi-ld:%s:%s", typeName, sewer.ID)

	return cip.MergeOrCreate(ctx, cbClient, id, typeName, properties)
}
