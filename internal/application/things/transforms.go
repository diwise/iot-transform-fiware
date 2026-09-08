package things

import (
	"fmt"
	"strings"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	helpers "github.com/diwise/iot-transform-fiware/internal/application/decorators"

	"github.com/diwise/context-broker/pkg/datamodels/fiware"
	"github.com/diwise/context-broker/pkg/ngsild/types/properties"
)

// EntityWrite describes one outbound broker write without performing
// it. The presentation adapter executes the writes in order.
type EntityWrite struct {
	EntityID string
	TypeName string
	Create   bool
	Props    []entities.EntityDecoratorFunc
}

func TransformContainer(c Container) []EntityWrite {
	props := make([]entities.EntityDecoratorFunc, 0)

	props = append(props, helpers.FillingLevel(c.Percent, c.ObservedAt))
	props = append(props, decorators.Location(c.Location.Latitude, c.Location.Longitude))
	props = append(props, decorators.DateObserved(c.ObservedAt.UTC().Format(time.RFC3339)))

	return []EntityWrite{{
		EntityID: c.EntityID(),
		TypeName: c.TypeName(),
		Props:    props,
	}}
}

func TransformLifebuoy(lb Lifebuoy) []EntityWrite {
	statusValue := map[bool]string{true: "on", false: "off"}
	props := make([]entities.EntityDecoratorFunc, 0, 5)

	props = append(props, decorators.DateLastValueReported(lb.ObservedAt.UTC().Format(time.RFC3339)))
	props = append(props, decorators.Status(statusValue[lb.Presence], properties.TxtObservedAt(lb.ObservedAt.UTC().Format(time.RFC3339))))
	props = append(props, decorators.Location(lb.Location.Latitude, lb.Location.Longitude))

	typeName := "Lifebuoy"
	entityID := fmt.Sprintf("urn:ngsi-ld:%s:%s", typeName, lb.AlternativeNameOrNameOrID())

	return []EntityWrite{{
		EntityID: entityID,
		TypeName: typeName,
		Props:    props,
	}}
}

func TransformDesk(desk Desk) []EntityWrite {
	statusValue := map[bool]string{true: "on", false: "off"}
	props := make([]entities.EntityDecoratorFunc, 0, 5)

	props = append(props, decorators.DateLastValueReported(desk.ObservedAt.UTC().Format(time.RFC3339)))
	props = append(props, decorators.Status(statusValue[desk.Presence], properties.TxtObservedAt(desk.ObservedAt.UTC().Format(time.RFC3339))))
	props = append(props, decorators.Location(desk.Location.Latitude, desk.Location.Longitude))

	entityID := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, desk.AlternativeNameOrNameOrID())

	return []EntityWrite{{
		EntityID: entityID,
		TypeName: fiware.DeviceTypeName,
		Props:    props,
	}}
}

func TransformPointOfInterest(poi PointOfInterest) []EntityWrite {
	var poiTypePrefix, observationID, observationTypePrefix, observationTypeName string
	observation := make([]entities.EntityDecoratorFunc, 0)

	writes := make([]EntityWrite, 0, 2)

	switch strings.ToLower(poi.TypeName()) {
	case "beach":
		observationTypePrefix = fiware.WaterQualityObservedIDPrefix
		observationTypeName = fiware.WaterQualityObservedTypeName
		poiTypePrefix = fiware.BeachIDPrefix

		if poi.Current.Ref != "" {
			observationID = fmt.Sprintf("%s%s", observationTypePrefix, poi.Current.Ref)
			observation = append(observation, decorators.RefDevice(fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, poi.Current.Ref)))
		} else {
			observationID = fmt.Sprintf("%s%s", observationTypePrefix, poi.AlternativeNameOrNameOrID())
		}

		poiEntityID := fmt.Sprintf("%s%s", poiTypePrefix, poi.AlternativeNameOrNameOrID())

		if poi.Description != nil && *poi.Description != "" {
			observation = append(observation, decorators.Description(*poi.Description))
		}

		writes = append(writes, EntityWrite{
			EntityID: poiEntityID,
			TypeName: poi.TypeName(),
			Create:   true,
			Props: []entities.EntityDecoratorFunc{
				decorators.Location(poi.Location.Latitude, poi.Location.Longitude),
			},
		})
	default:
		observationTypePrefix = fiware.WeatherObservedIDPrefix
		observationTypeName = fiware.WeatherObservedTypeName
		poiTypePrefix = fiware.PointOfInterestIDPrefix

		observationID = fmt.Sprintf("%s%s", observationTypePrefix, poi.AlternativeNameOrNameOrID())
	}

	poiEntityID := fmt.Sprintf("%s%s", poiTypePrefix, poi.AlternativeNameOrNameOrID())

	observation = append(observation,
		helpers.RefLocation(poiEntityID),
		decorators.Location(poi.Location.Latitude, poi.Location.Longitude),
		decorators.DateObserved(poi.ObservedAt.UTC().Format(time.RFC3339)),
	)

	if poi.Current.Value != nil {
		observation = append(observation, helpers.Temperature(*poi.Current.Value, poi.Current.Timestamp.UTC()))
	}

	if poi.Description != nil && *poi.Description != "" {
		observation = append(observation, decorators.Description(*poi.Description))
	}

	if poi.Current.Source != nil {
		observation = append(observation, decorators.Source(*poi.Current.Source))
	}

	writes = append(writes, EntityWrite{
		EntityID: observationID,
		TypeName: observationTypeName,
		Props:    observation,
	})

	return writes
}

func TransformPumpingStation(p PumpingStation) []EntityWrite {
	var statusValue = map[bool]string{true: "on", false: "off"}

	props := make([]entities.EntityDecoratorFunc, 0, 5)

	observedAt := time.Now().UTC().Format(time.RFC3339)
	if !p.ObservedAt.IsZero() {
		observedAt = p.ObservedAt.UTC().Format(time.RFC3339)
	}

	if p.PumpingAt == nil {
		props = append(props, decorators.DateObserved(observedAt))
	} else {
		props = append(props, decorators.DateObserved(observedAt))
		pumpingAt := p.PumpingAt.UTC().Format(time.RFC3339)
		props = append(props, decorators.Status(statusValue[p.Pumping], properties.TxtObservedAt(pumpingAt)))
	}

	//timestamp := p.PumpingAt.UTC().Format(time.RFC3339)
	//props = append(props, decorators.DateObserved(timestamp))
	//props = append(props, decorators.Status(statusValue[p.Pumping], properties.TxtObservedAt(timestamp)))
	props = append(props, decorators.Location(p.Location.Latitude, p.Location.Longitude))

	typeName := "SewagePumpingStation"
	entityID := fmt.Sprintf("urn:ngsi-ld:%s:%s", typeName, p.AlternativeNameOrNameOrID())

	return []EntityWrite{{
		EntityID: entityID,
		TypeName: typeName,
		Props:    props,
	}}
}

func TransformRoom(r Room) []EntityWrite {
	var entityID string
	props := make([]entities.EntityDecoratorFunc, 0)

	entityID = fmt.Sprintf("%s%s:%s", fiware.IndoorEnvironmentObservedIDPrefix, r.TypeName(), r.AlternativeNameOrNameOrID())

	ts := r.ObservedAt

	if ts.IsZero() {
		ts = time.Now()
	}

	props = append(props, decorators.Location(r.Location.Latitude, r.Location.Longitude))
	props = append(props, decorators.DateObserved(helpers.FormatTime(ts)))
	if r.Temperature.Value != nil {
		props = append(props, helpers.Temperature(*r.Temperature.Value, ts))
	}
	props = append(props, helpers.Humidity(r.Humidity, ts))
	props = append(props, helpers.Illuminance(r.Illuminance, ts))
	props = append(props, helpers.CO2(r.CO2, ts))
	if len(r.Name) > 0 {
		props = append(props, helpers.Name(r.Name))
	}
	if len(r.AlternativeName) > 0 {
		props = append(props, helpers.AlternativeName(r.AlternativeName))
	}

	return []EntityWrite{{
		EntityID: entityID,
		TypeName: fiware.IndoorEnvironmentObservedTypeName,
		Props:    props,
	}}
}

func TransformSewer(s Sewer) []EntityWrite {
	props := make([]entities.EntityDecoratorFunc, 0, 4)
	props = append(props, decorators.Location(s.Location.Latitude, s.Location.Longitude))

	if s.Name != "" {
		props = append(props, helpers.Name(s.Name))
	}

	if s.AlternativeName != "" {
		props = append(props, helpers.AlternativeName(s.AlternativeName))
	}

	var observedAt string

	if s.ObservedAt.IsZero() {
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

	if s.Measured != nil {
		ob := s.Measured.ObservedAt.UTC().Format(time.RFC3339)
		props = append(props, decorators.Number("level", s.Measured.Level, properties.ObservedAt(ob)))
		props = append(props, decorators.Number("percent", s.Measured.Percent, properties.ObservedAt(ob)))
		props = append(props, decorators.DateObserved(observedAt))
	}

	/*
		if s.CurrentLevel != 0 {
			props = append(props, decorators.Number("level", s.CurrentLevel, properties.ObservedAt(observedAt)))
		}

		if s.Percent != 0 {
			props = append(props, decorators.Number("percent", s.Percent, properties.ObservedAt(observedAt)))
		}
	*/

	if s.LastAction == OverflowUnknown {
		props = append(props, decorators.DateObserved(observedAt))
	}

	if s.LastAction == OverflowStarted || s.LastAction == OverflowUpdated {
		props = append(props, decorators.DateObserved(observedAt))
		overflowAt := s.OverflowAt.UTC().Format(time.RFC3339)

		overflow := fmt.Sprintf("%t", s.Overflow)
		props = append(props, decorators.Status(overflow, properties.TxtObservedAt(overflowAt)))
	}

	if s.LastAction == OverflowStopped {
		endAt := s.OverflowEndAt.UTC().Format(time.RFC3339)
		overflow := fmt.Sprintf("%t", s.Overflow)

		props = append(props, decorators.DateObserved(observedAt))
		props = append(props, decorators.Status(overflow, properties.TxtObservedAt(endAt)))
	}

	if s.Description != nil && *s.Description != "" {
		props = append(props, decorators.Description(*s.Description))
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
				urn = strings.ReplaceAll(urn, "::", ":")
			}

			props = append(props, decorators.RefDevice(urn))
			props = append(props, decorators.Source(urn))
		} else {
			urns := []string{}
			for _, d := range devices {
				urn := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, d)

				if strings.Contains(urn, "::") {
					urn = strings.ReplaceAll(urn, "::", ":")
				}

				urns = append(urns, urn)
			}

			props = append(props, helpers.RefDevices(urns))
			props = append(props, decorators.Source(urns[0]))
		}
	}

	return []EntityWrite{{
		EntityID: s.EntityID(),
		TypeName: s.TypeName(),
		Props:    props,
	}}
}
