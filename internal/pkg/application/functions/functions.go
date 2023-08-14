package functions

import (
	"context"
	"fmt"
	"time"

	"github.com/diwise/context-broker/pkg/datamodels/fiware"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/cip"
	. "github.com/diwise/iot-transform-fiware/internal/pkg/application/decorators"
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

func Lifebuoy(ctx context.Context, fn Func, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0)

	properties = append(properties, decorators.DateLastValueReported(fn.Timestamp.UTC().Format(time.RFC3339)))
	properties = append(properties, decorators.Status(statusValue[fn.Presence.State]))

	if fn.Location != nil {
		properties = append(properties, decorators.Location(fn.Location.Latitude, fn.Location.Longitude))
	}

	const (
		LifebuoyIDPrefix string = "urn:ngsi-ld:Lifebuoy:"
		LifebuoyTypeName string = "Lifebuoy"
	)

	id := fmt.Sprintf("%s%s", LifebuoyIDPrefix, fn.ID)

	return cip.MergeOrCreate(ctx, cbClient, id, LifebuoyTypeName, properties)
}

func Device(ctx context.Context, fn Func, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0)

	properties = append(properties, decorators.DateLastValueReported(fn.Timestamp.UTC().Format(time.RFC3339)))
	properties = append(properties, decorators.Status(statusValue[fn.Presence.State]))

	if fn.Location != nil {
		properties = append(properties, decorators.Location(fn.Location.Latitude, fn.Location.Longitude))
	}

	id := fiware.DeviceIDPrefix + fn.ID

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.DeviceTypeName, properties)
}