package transform

import (
	"fmt"
	"time"
)

// Record är en normaliserad resurs-/metadatapost i en mätning.
type Record struct {
	Object  string
	Name    string
	Value   *float64
	Bool    *bool
	String  *string
	Time    time.Time
	HasTime bool
}

// Measurement är motorns normaliserade indata för en enskild
// objektobservation (ett objekt ur ett SenML-pack).
type Measurement struct {
	DeviceID      string
	SensorType    string
	Object        string
	Env           string
	Port          string
	Tenant        string
	Timestamp     time.Time
	HeaderTime    time.Time
	HasHeaderTime bool
	Lat           float64
	Lon           float64
	HasLatLon     bool
	Records       []Record
}

func (m Measurement) Kind() Kind { return KindMeasurement }

func (m Measurement) TenantID() string { return m.Tenant }

func (m Measurement) MessageTime() time.Time { return m.Timestamp }

func (m Measurement) BaseTime() (time.Time, bool) { return m.HeaderTime, m.HasHeaderTime }

func (m Measurement) LatLon() (float64, float64, bool) { return m.Lat, m.Lon, m.HasLatLon }

func (m Measurement) Variables() map[string]string {
	return map[string]string{
		"deviceID":   m.DeviceID,
		"sensorType": m.SensorType,
		"object":     m.Object,
		"env":        m.Env,
		"port":       m.Port,
		"tenant":     m.Tenant,
		"timestamp":  m.Timestamp.UTC().Format(rfc3339),
	}
}

func (m Measurement) MatchInfo() MatchInfo {
	return MatchInfo{
		Device:     m.DeviceID,
		SensorType: m.SensorType,
		Object:     m.Object,
		Env:        m.Env,
		Port:       m.Port,
		Tenant:     m.Tenant,
	}
}

// Resolve slår upp en egenskaps värdekälla (resource + valfritt object,
// eller metadata via name).
func (m Measurement) Resolve(p Property) (resolvedValue, error) {
	r, ok := m.findRecord(p)
	if !ok {
		return resolvedValue{}, nil
	}

	v, err := rawValue(p, r)
	if err != nil {
		return resolvedValue{}, err
	}

	return resolvedValue{value: v, time: r.Time, hasTime: r.HasTime, found: true}, nil
}

func (m Measurement) ResolveTime(path string) (time.Time, bool) {
	if r, ok := m.findByRecordName(path); ok && r.HasTime {
		return r.Time, true
	}
	return time.Time{}, false
}

func (m Measurement) ResolvePath(path string) (any, bool) {
	r, ok := m.findByRecordName(path)
	if !ok {
		return nil, false
	}
	v, err := rawValue(Property{Target: path}, r)
	if err != nil {
		return nil, false
	}
	return v, true
}

// findRecord slår upp en post för en property.
func (m Measurement) findRecord(p Property) (Record, bool) {
	for _, r := range m.Records {
		switch {
		case p.Resource != "":
			if r.Name != p.Resource {
				continue
			}
		case p.Name != "":
			if r.Name != p.Name {
				continue
			}
		default:
			continue
		}

		if p.Object != "" && r.Object != p.Object {
			continue
		}

		return r, true
	}

	return Record{}, false
}

func (m Measurement) findByRecordName(name string) (Record, bool) {
	for _, r := range m.Records {
		if r.Name == name {
			return r, true
		}
	}
	return Record{}, false
}

// rawValue returnerar postens nativa värde. Typomvandling till propertyns
// måltyp sker efter transformstegen, så att t.ex. boolToOnOff kan arbeta på
// en bool.
func rawValue(p Property, r Record) (any, error) {
	switch {
	case r.Value != nil:
		return *r.Value, nil
	case r.Bool != nil:
		return *r.Bool, nil
	case r.String != nil:
		return *r.String, nil
	case r.HasTime:
		return r.Time.UTC().Format(rfc3339), nil
	default:
		return nil, fmt.Errorf("property %s: value is missing", p.Target)
	}
}
