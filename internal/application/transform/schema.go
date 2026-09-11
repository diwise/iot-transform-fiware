// Package transform beskriver den konfigurationsstyrda mappningen från
// inkommande mätvärden och saker till NGSI-LD-entiteter. Paketet innehåller
// schemat (regler, entiteter, properties, källor, transform och derivering)
// samt validering. Själva körningen sker i motorn (senare fas).
package transform

// Kind skiljer mätvärden från saker.
type Kind string

const (
	KindMeasurement Kind = "measurement"
	KindThing       Kind = "thing"
)

// PropertyType är den NGSI-LD-typ en property skrivs som.
type PropertyType string

const (
	TypeNumber       PropertyType = "Number"
	TypeText         PropertyType = "Text"
	TypeBoolean      PropertyType = "Boolean"
	TypeDateTime     PropertyType = "DateTime"
	TypeGeo          PropertyType = "Geo"
	TypeRelationship PropertyType = "Relationship"
	TypeNumberArray  PropertyType = "NumberArray"
	TypeTextArray    PropertyType = "TextArray"
	TypeStructured   PropertyType = "Structured"
)

// ObservedAt anger varifrån en property hämtar sin observationstid.
type ObservedAt string

const (
	ObservedAtHeader  ObservedAt = "header"  // objektets bastid (dagens beteende)
	ObservedAtRecord  ObservedAt = "record"  // recordens egen t
	ObservedAtMessage ObservedAt = "message" // meddelandets timestamp
)

// When styr om en property skrivs.
type When string

const (
	WhenPresent When = "present"
)

// Model är innehållet i en konfigurationsfil.
type Model struct {
	Rules []Rule `yaml:"rules"`
}

// Rule matchar en källa och beskriver vilka entiteter som ska skrivas.
type Rule struct {
	Match    Match    `yaml:"match"`
	Entities []Entity `yaml:"entities"`
}

// Match beskriver vilka källor en regel gäller. Alla fält utom Kind är
// valfria; tomt fält matchar allt.
type Match struct {
	Kind       Kind   `yaml:"kind"`
	Device     string `yaml:"device,omitempty"`
	Port       string `yaml:"port,omitempty"`
	SensorType string `yaml:"sensorType,omitempty"`
	Object     string `yaml:"object,omitempty"`
	Resource   string `yaml:"resource,omitempty"`
	Env        string `yaml:"env,omitempty"`
	Type       string `yaml:"type,omitempty"`
	SubType    string `yaml:"subType,omitempty"`
	Tenant     string `yaml:"tenant,omitempty"`
}

// Entity beskriver en NGSI-LD-entitet som ska skrivas.
type Entity struct {
	Name       string     `yaml:"name,omitempty"`
	ID         string     `yaml:"id"`
	Type       string     `yaml:"type"`
	Context    string     `yaml:"context,omitempty"`
	Create     bool       `yaml:"create,omitempty"`
	Properties []Property `yaml:"properties"`
}

// Source beskriver var ett värde hämtas. Exakt en källa ska vara satt.
type Source struct {
	Resource string `yaml:"resource,omitempty"`
	Object   string `yaml:"object,omitempty"`
	Env      string `yaml:"env,omitempty"`
	Name     string `yaml:"name,omitempty"`
	Field    string `yaml:"field,omitempty"`
	Const    any    `yaml:"const,omitempty"`
	LatLon   bool   `yaml:"latlon,omitempty"`
	Value    string `yaml:"value,omitempty"`
}

// Transform är ett namngivet steg som appliceras på ett enskilt värde.
type Transform struct {
	Op    string   `yaml:"op"`
	Value *float64 `yaml:"value,omitempty"`
}

// Derive är en namngiven funktion över flera källor.
type Derive struct {
	Fn string   `yaml:"fn"`
	Of []Source `yaml:"of"`
}

// Property beskriver ett NGSI-LD-attribut.
type Property struct {
	Target     string       `yaml:"target"`
	Type       PropertyType `yaml:"type"`
	Unit       string       `yaml:"unit,omitempty"`
	ObservedAt ObservedAt   `yaml:"observedAt,omitempty"`
	When       When         `yaml:"when,omitempty"`
	Required   bool         `yaml:"required,omitempty"`

	Source `yaml:",inline"`

	Transform     []Transform `yaml:"transform,omitempty"`
	Derive        *Derive     `yaml:"derive,omitempty"`
	Subproperties []Property  `yaml:"subproperties,omitempty"`
}
