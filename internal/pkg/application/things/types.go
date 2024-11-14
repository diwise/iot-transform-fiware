package things

import (
	"fmt"
	"strings"
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

func (t thing) EntityID() string {
	return fmt.Sprintf("urn:ngsi-ld:%s:%s", t.TypeName(), t.AlternativeNameOrNameOrID())
}

func (t thing) AlternativeNameOrNameOrID() string {
	n := t.ID

	if t.AlternativeName != "" {
		n = strings.ReplaceAll(t.AlternativeName, " ", "-")
		return n
	}

	if t.Name != "" {
		n = strings.ReplaceAll(t.Name, " ", "-")
		return n
	}

	return n
}

func (t thing) TypeName() string {
	if t.SubType != nil && *t.SubType != "" {
		return *t.SubType
	}
	return t.Type
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

type sewer struct {
	thing
	CurrentLevel   float64        `json:"currentLevel"`
	Percent        float64        `json:"percent"`
	Observed       bool           `json:"overflowObserved"`
	ObservedAt     *time.Time     `json:"overflowObservedAt"`
	Duration       *time.Duration `json:"overflowDuration"`
	CumulativeTime time.Duration  `json:"overflowCumulativeTime"`
}

type pointOfInterest struct {
	thing
	Temperature float64 `json:"temperature"`
}

type room struct {
	thing
	Temperature float64 `json:"temperature"`
}

type pumpingStation struct {
	thing
	Observed       bool           `json:"pumpingObserved"`
	ObservedAt     *time.Time     `json:"pumpingObservedAt"`
	Duration       *time.Duration `json:"pumpingDuration"`
	CumulativeTime time.Duration  `json:"pumpingCumulativeTime"`
}

type watermeter struct {
	thing
	CumulativeVolume float64 `json:"cumulativeVolume"`
	Leakage          bool    `json:"leakage"`
	Burst            bool    `json:"burst"`
	Backflow         bool    `json:"backflow"`
	Fraud            bool    `json:"fraud"`
}
