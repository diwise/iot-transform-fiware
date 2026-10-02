package transform

import (
	"fmt"
	"strconv"
	"time"
)

// Record är en normaliserad resurs-/metadatapost i en mätning.
// Channel bevarar det råa mellanprefixet mellan enhet och resurs. I ett
// fullständigt LwM2M-namn ingår även objektsegmentet där; det är inte port.
// Port för den partitionerade observationen ligger på Measurement.Port.
type Record struct {
	FullName string
	Object   string
	Name     string
	Channel  string
	Unit     string
	Value    *float64
	Bool     *bool
	String   *string
	Time     time.Time
	HasTime  bool
}

// Measurement är motorns normaliserade indata för en enskild
// objektobservation (ett objekt ur ett SenML-pack).
type Measurement struct {
	DeviceID      string
	SensorType    string
	Object        string
	Env           string
	Source        string
	Port          string // fysisk anslutning från device/port/object/resource; tomt utan port
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
		"id":         m.DeviceID,
		"deviceID":   m.DeviceID,
		"sensorType": m.SensorType,
		"object":     m.Object,
		"env":        m.Env,
		"port":       m.Port,
		"tenant":     m.Tenant,
		"timestamp":  sourceTime(m.Timestamp),
	}
}

func (m Measurement) HasResource(name string) bool {
	for _, r := range m.Records {
		if r.Name == name {
			return true
		}
	}
	return false
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
	if p.Field != "" {
		rv := resolveField(m.sourceFields(), p.Field)
		if rv.found {
			steps, _ := parsePath(p.Field)
			if !rv.hasTime && m.HasHeaderTime && !m.HeaderTime.IsZero() && len(steps) == 3 && steps[0].key == "records" && steps[1].indexed && steps[2].key == "value" && steps[1].index < len(m.Records) {
				if _, err := strconv.Atoi(m.Records[steps[1].index].Name); err == nil {
					rv.time, rv.hasTime = m.HeaderTime, true
				}
			}
			return rv, nil
		}
		if r, ok := m.findByRecordName(p.Field); ok {
			if _, err := strconv.Atoi(r.Name); err == nil {
				return m.Resolve(Property{Resource: r.Name})
			}
			value, err := rawValue(p, r)
			return resolvedValue{value: value, found: present(value)}, err
		}
		return rv, nil
	}
	r, ok := m.findRecord(p)
	if !ok {
		return resolvedValue{}, nil
	}

	v, err := rawValue(p, r)
	if err != nil {
		return resolvedValue{}, err
	}

	rv := resolvedValue{value: v, time: r.Time, hasTime: r.HasTime && !r.Time.IsZero(), found: present(v), unit: r.Unit}
	if !rv.hasTime && p.Resource != "" && m.HasHeaderTime && !m.HeaderTime.IsZero() {
		rv.time, rv.hasTime = m.HeaderTime, true
	}
	return rv, nil
}

func (m Measurement) ResolveTime(path string) (time.Time, bool) {
	if value, ok := lookupPath(m.sourceFields(), path); ok {
		if text, ok := value.(string); ok {
			if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
				return parsed, true
			}
		}
	}
	if r, ok := m.findByRecordName(path); ok && r.HasTime {
		return r.Time, true
	}
	return time.Time{}, false
}

func (m Measurement) ResolvePath(path string) (any, bool) {
	if value, ok := lookupPath(m.sourceFields(), path); ok {
		return value, true
	}
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

// records preserves wire order, duplicate resource names and normalized source
// identity. The existing short-name shortcuts remain available independently.
func (m Measurement) sourceFields() map[string]any {
	records := make([]any, len(m.Records))
	for i, r := range m.Records {
		fields := map[string]any{"fullName": r.FullName, "name": r.Name, "object": r.Object, "channel": r.Channel, "unit": r.Unit}
		switch {
		case r.Value != nil:
			fields["value"] = *r.Value
		case r.Bool != nil:
			fields["value"] = *r.Bool
		case r.String != nil:
			fields["value"] = *r.String
		}
		if r.HasTime && !r.Time.IsZero() {
			fields["observedAt"] = r.Time.UTC().Format(time.RFC3339Nano)
		}
		records[i] = fields
	}
	fields := map[string]any{"records": records}
	if m.Source != "" {
		fields["source"] = m.Source
	}
	if !m.Timestamp.IsZero() {
		fields["message"] = map[string]any{"timestamp": m.Timestamp.UTC().Format(time.RFC3339Nano)}
	}
	if m.HasHeaderTime && !m.HeaderTime.IsZero() {
		fields["header"] = map[string]any{"observedAt": m.HeaderTime.UTC().Format(time.RFC3339Nano)}
	}
	return fields
}

// findRecord slår upp en post för en property.
func (m Measurement) findRecord(p Property) (Record, bool) {
	if p.Env != "" && p.Env != m.Env {
		return Record{}, false
	}

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
		if r.Name == name || (r.FullName != "" && r.FullName == name) {
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
