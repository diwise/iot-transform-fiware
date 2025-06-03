package things

import (
	"fmt"
	"regexp"
	"time"
)

type thing struct {
	ID              string    `json:"id"`
	Type            string    `json:"type"`
	SubType         *string   `json:"subType,omitempty"`
	Name            string    `json:"name"`
	AlternativeName string    `json:"alternativeName,omitempty"`
	Description     *string   `json:"description"`
	Location        location  `json:"location"`
	RefDevices      []device  `json:"refDevices,omitempty"`
	ObservedAt      time.Time `json:"observedAt"`
	Tenant          string    `json:"tenant"`
}

var nonSafeUriRegExp = regexp.MustCompile(`[^\w\-~:/?#\[\]@!$&'()*+,;=%.]+`)

func (t thing) EntityID() string {
	return fmt.Sprintf("urn:ngsi-ld:%s:%s", t.TypeName(), t.AlternativeNameOrNameOrID())
}

func (t thing) AlternativeNameOrNameOrID() string {
	n := t.ID

	if t.Name != "" {
		n = t.Name
	}

	if t.AlternativeName != "" {
		n = t.AlternativeName
	}

	n = nonSafeUriRegExp.ReplaceAllString(n, ":")

	return n
}

func (t thing) TypeName() string {
	typeName := t.Type

	if t.SubType != nil && *t.SubType != "" {
		typeName = *t.SubType
	}

	typeName = nonSafeUriRegExp.ReplaceAllString(typeName, ":")

	return typeName
}

type location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type device struct {
	DeviceID string `json:"deviceID"`
}

type container struct {
	thing
	CurrentLevel float64 `json:"currentLevel"`
	Percent      float64 `json:"percent"`
}

type lifebuoy struct {
	thing
	Presence bool `json:"presence"`
}

type desk struct {
	thing
	Presence bool `json:"presence"`
}

type sewer struct {
	thing
	CurrentLevel   float64        `json:"currentLevel"`
	Percent        float64        `json:"percent"`
	Measured       *measured      `json:"measured,omitempty"`
	Overflow       bool           `json:"overflowObserved"`
	OverflowAt     *time.Time     `json:"overflowObservedAt"`
	OverflowEndAt  *time.Time     `json:"overflowEndedAt"`
	Duration       *time.Duration `json:"overflowDuration"`
	CumulativeTime time.Duration  `json:"overflowCumulativeTime"`
	LastAction     string         `json:"lastAction"`
}

type measured struct {
	Level      float64   `json:"level"`
	Percent    float64   `json:"percent"`
	ObservedAt time.Time `json:"observedAt"`
}

type pointOfInterest struct {
	thing
	Temperature measurement `json:"temperature"`
	Current     measurement `json:"current"`
}

type measurement struct {
	ID          string    `json:"id,omitzero"`
	Urn         string    `json:"urn,omitzero"`
	BoolValue   *bool     `json:"vb,omitempty"`
	StringValue *string   `json:"vs,omitempty"`
	Value       *float64  `json:"v,omitempty"`
	Unit        string    `json:"unit,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	Source      *string   `json:"source,omitzero"`
}

type room struct {
	thing
	Temperature measurement `json:"temperature"`
	Humidity    float64     `json:"humidity"`
	Illuminance float64     `json:"illuminance"`
	CO2         float64     `json:"co2"`
	Presence    bool        `json:"presence"`
}

type pumpingStation struct {
	thing
	Pumping        bool           `json:"pumpingObserved"`
	PumpingAt      *time.Time     `json:"pumpingObservedAt"`
	Duration       *time.Duration `json:"pumpingDuration"`
	CumulativeTime time.Duration  `json:"pumpingCumulativeTime"`
}
