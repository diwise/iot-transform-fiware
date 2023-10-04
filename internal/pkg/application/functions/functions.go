package functions

import (
	"context"
	"encoding/json"
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
		PM1  *float64 `json:"pm1,omitempty"`
		PM10 *float64 `json:"pm10,omitempty"`
		PM25 *float64 `json:"pm25,omitempty"`
		NO   *float64 `json:"no,omitempty"`
		NO2  *float64 `json:"no2,omitempty"`
		CO2  *float64 `json:"co2,omitempty"`
	} `json:"particulates"`
	Temperature *float64  `json:"temperature,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

type fnctMetadata struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Type     string    `json:"type"`
	SubType  string    `json:"subtype"`
	Location *location `json:"location,omitempty"`
	Tenant   string    `json:"tenant,omitempty"`
	Source   string    `json:"source,omitempty"`
}

type FnctUpdated struct {
	fnctMetadata
	Data      json.RawMessage `json:"data"`
	Timestamp time.Time
}

func WaterQualityObserved(ctx context.Context, fn FnctUpdated, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	id := fmt.Sprintf("%s%s:%s", fiware.WaterQualityObservedIDPrefix, fn.SubType, fn.ID)

	var wq waterquality
	err := json.Unmarshal(fn.Data, &wq)
	if err != nil {
		return err
	}

	properties = append(properties,
		decorators.DateObserved(wq.Timestamp.UTC().Format(time.RFC3339)),
		Temperature(wq.Temperature, wq.Timestamp.UTC()),
	)

	if fn.Source != "" {
		properties = append(properties, decorators.Source(fn.Source))
	}

	if fn.Location != nil {
		properties = append(properties, decorators.Location(fn.Location.Latitude, fn.Location.Longitude))
	}

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.WaterQualityObservedTypeName, properties)
}

func AirQualityObserved(ctx context.Context, fn FnctUpdated, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	id := fmt.Sprintf("%s%s:%s", fiware.AirQualityObservedIDPrefix, fn.SubType, fn.ID)

	var aq airquality
	err := json.Unmarshal(fn.Data, &aq)
	if err != nil {
		return err
	}

	particulates := aq.Particulates
	ts := aq.Timestamp.UTC()

	properties = append(properties, decorators.DateObserved(ts.Format(time.RFC3339)))

	if aq.Temperature != nil {
		properties = append(properties, Temperature(*aq.Temperature, ts))
	}

	if particulates.CO2 != nil {
		properties = append(properties, CO2(*particulates.CO2, ts))
	}

	if particulates.PM10 != nil {
		properties = append(properties, PM10(*particulates.PM10, ts))
	}

	if particulates.PM1 != nil {
		properties = append(properties, PM1(*particulates.PM1, ts))
	}

	if particulates.PM25 != nil {
		properties = append(properties, PM25(*particulates.PM25, ts))
	}

	if particulates.NO2 != nil {
		properties = append(properties, NO2(*particulates.NO2, ts))
	}

	if particulates.NO != nil {
		properties = append(properties, NO(*particulates.NO, ts))
	}

	if fn.Source != "" {
		properties = append(properties, decorators.Source(fn.Source))
	}

	if fn.Location != nil {
		properties = append(properties, decorators.Location(fn.Location.Latitude, fn.Location.Longitude))
	}

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.AirQualityObservedTypeName, properties)
}
