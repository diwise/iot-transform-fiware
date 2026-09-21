package measurements

import (
	"context"
	"strings"

	"github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/senml"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

// SensorTypeResolver slår upp en enhets sensortyp.
type SensorTypeResolver interface {
	SensorType(ctx context.Context, deviceID string) (string, error)
}

// DynamicTransformer mappar en enskild objektobservation till
// entitetsskrivningar med den konfigurationsstyrda motorn. Oföränderlig
// efter konstruktion: omladdning sker genom att bygga en ny instans
// (se api.Snapshot, TF-05).
type DynamicTransformer struct {
	engine   *transform.Engine
	resolver SensorTypeResolver
}

func NewDynamicTransformer(rules []transform.Rule, resolver SensorTypeResolver) *DynamicTransformer {
	return &DynamicTransformer{engine: transform.NewEngine(rules), resolver: resolver}
}

// Transform slår upp sensortypen (om en resolver finns) och kör motorn.
// Om uppslaget misslyckas fortsätter matchningen utan sensortyp, så att
// regler som bara matchar på object/env fungerar även när device-management
// är otillgängligt.
func (d *DynamicTransformer) Transform(ctx context.Context, msg events.MessageAccepted) ([]transform.EntityWrite, error) {
	sensorType := ""

	if d.resolver != nil {
		resolved, err := d.resolver.SensorType(ctx, msg.DeviceID())
		if err != nil {
			logging.GetFromContext(ctx).Debug("unable to resolve sensor type", "err", err.Error(), "device_id", msg.DeviceID())
		} else {
			sensorType = resolved
		}
	}

	return d.engine.Transform(measurementFromMessage(msg, sensorType))
}

// measurementFromMessage normaliserar ett scopat meddelande till motorns
// indata. sensorType slås upp separat via device-management.
func measurementFromMessage(msg events.MessageAccepted, sensorType string) transform.Measurement {
	pack := msg.Pack()

	m := transform.Measurement{
		DeviceID:   msg.DeviceID(),
		SensorType: sensorType,
		Object:     events.GetObjectURN(pack),
		Tenant:     msg.Tenant(),
		Timestamp:  msg.Timestamp,
	}

	if env, ok := pack.GetStringValue(findShortName("env")); ok {
		m.Env = env
	}

	if t, ok := pack.GetTime(findHeader()); ok {
		m.HeaderTime = t
		m.HasHeaderTime = true
	}

	if lat, lon, ok := pack.GetLatLon(); ok {
		m.Lat, m.Lon, m.HasLatLon = lat, lon, true
	}

	m.Records = recordsFromPack(pack, m.Object)

	return m
}

func recordsFromPack(pack senml.Pack, object string) []transform.Record {
	header := findHeader()
	records := make([]transform.Record, 0, len(pack))

	for _, r := range pack {
		if header(r) {
			continue
		}

		name := shortResourceName(r.Name)
		if name == "" || name == "0" {
			continue
		}

		rec := transform.Record{Object: object, Name: name}

		if v, ok := r.GetValue(); ok {
			rec.Value = &v
		}
		if r.BoolValue != nil {
			rec.Bool = r.BoolValue
		}
		if r.StringValue != "" {
			s := r.StringValue
			rec.String = &s
		}
		if t, ok := r.GetTime(); ok {
			rec.Time = t
			rec.HasTime = true
		}

		records = append(records, rec)
	}

	return records
}

func shortResourceName(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

// findHeader matchar objektheaders på kort eller fullständigt namn. Headers
// bär objektets URN.
func findHeader() senml.RecordFinder {
	return func(r senml.Record) bool {
		if r.Name != "0" && !strings.HasSuffix(r.Name, "/0") {
			return false
		}
		return strings.HasPrefix(r.StringValue, "urn:oma:lwm2m:")
	}
}

// findShortName matchar poster på kort eller fullständigt namn.
func findShortName(n string) senml.RecordFinder {
	return func(r senml.Record) bool {
		return r.Name == n || strings.HasSuffix(r.Name, "/"+n)
	}
}
