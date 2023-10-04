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

type airquality struct {
	Particulates struct {
		PM1  float64 `json:"pm1"`
		PM10 float64 `json:"pm10"`
		PM25 float64 `json:"pm25"`
		NO   float64 `json:"no"`
		NO2  float64 `json:"no2"`
		CO2  float64 `json:"co2"`
	} `json:"particulates"`
	Temperature float64   `json:"temperature"`
	Timestamp   time.Time `json:"timestamp"`
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
	AirQuality   *airquality   `json:"airquality,omitempty"`

	Timestamp time.Time
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

func AirQualityObserved(ctx context.Context, fn Func, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	id := fmt.Sprintf("%s%s:%s", fiware.AirQualityObservedIDPrefix, fn.SubType, fn.ID)

	aq := *fn.AirQuality
	p := aq.Particulates
	ts := aq.Timestamp.UTC()

	properties = append(properties,
		decorators.DateObserved(ts.Format(time.RFC3339)),
		Temperature(aq.Temperature, ts),
		CO2(p.CO2, ts),
		PM10(p.PM10, ts),
		PM1(p.PM1, ts),
		PM25(p.PM25, ts),
		NO2(p.NO2, ts),
		NO(p.NO, ts),
	)

	if fn.Source != "" {
		properties = append(properties, decorators.Source(fn.Source))
	}

	if fn.Location != nil {
		properties = append(properties, decorators.Location(fn.Location.Latitude, fn.Location.Longitude))
	}

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.AirQualityObservedTypeName, properties)
}
