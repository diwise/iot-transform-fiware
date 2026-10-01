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

// EventThingsV1Values taggar versionerade värdesevent (things.v1.values)
// i match.event och motorns indata. Tom tagg = ursprunglig källa.
const EventThingsV1Values = "things.v1.values"

// EventThingsV1Relations taggar versionerade relationshändelser
// (things.v1.relations) i match.event och motorns indata.
const EventThingsV1Relations = "things.v1.relations"

// EventThingsV1Lifecycle taggar versionerade livscykelhändelser
// (things.v1.lifecycle) i match.event och motorns indata.
const EventThingsV1Lifecycle = "things.v1.lifecycle"

// LifecycleKind avgränsar livscykelregler till en händelsetyp
// (created/updated/deleted) — samma ord som producentens kontrakt.
type LifecycleKind string

const (
	LifecycleCreated LifecycleKind = "created"
	LifecycleUpdated LifecycleKind = "updated"
	LifecycleDeleted LifecycleKind = "deleted"
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
	// TypeLanguageMap skriver flerspråkiga texter som NGSI-LD
	// LanguageProperty ({type, languageMap}). Källa: field: mot strängmapp
	// eller const: med mapp; tom mapp = saknad källa.
	TypeLanguageMap PropertyType = "LanguageMap"
)

// ObservedAt anger varifrån en property hämtar sin observationstid.
type ObservedAt string

const (
	ObservedAtHeader  ObservedAt = "header"  // objektets bastid (dagens beteende)
	ObservedAtRecord  ObservedAt = "record"  // recordens egen t
	ObservedAtMessage ObservedAt = "message" // meddelandets timestamp
	ObservedAtNone    ObservedAt = "none"    // ingen observedAt sätts
)

// When styr om en property skrivs.
type When string

const (
	WhenPresent When = "present"
)

// Model är innehållet i en konfigurationsfil.
type Model struct {
	Rules []Rule `yaml:"rules" json:"rules"`
}

// Rule matchar en källa och beskriver vilka entiteter som ska skrivas.
type Rule struct {
	// Körningsmetadata kommer från lagret, aldrig från regelpayloaden.
	ID              string   `yaml:"-" json:"-"`
	Revision        int64    `yaml:"-" json:"-"`
	SnapshotVersion int64    `yaml:"-" json:"-"`
	Match           Match    `yaml:"match" json:"match"`
	Entities        []Entity `yaml:"entities" json:"entities"`
}

// Match beskriver vilka källor en regel gäller. Tomma skalära filter är
// wildcard, men Kind, Event, Lifecycle och RelationRemoved jämförs exakt.
type Match struct {
	Kind   Kind   `yaml:"kind" json:"kind"`
	Device string `yaml:"device,omitempty" json:"device,omitempty"`
	// Port matchar exakt den fysiska anslutningen i device/port/object/resource.
	// Tomt är wildcard; "0" är en explicit port, inte avsaknad av port.
	Port       string `yaml:"port,omitempty" json:"port,omitempty"`
	SensorType string `yaml:"sensorType,omitempty" json:"sensorType,omitempty"`
	Object     string `yaml:"object,omitempty" json:"object,omitempty"`
	Resource   string `yaml:"resource,omitempty" json:"resource,omitempty"`
	Env        string `yaml:"env,omitempty" json:"env,omitempty"`
	Type       string `yaml:"type,omitempty" json:"type,omitempty"`
	SubType    string `yaml:"subType,omitempty" json:"subType,omitempty"`
	Tenant     string `yaml:"tenant,omitempty" json:"tenant,omitempty"`
	// Event avgränsar vilken händelsekälla regeln gäller (t.ex.
	// "things.v1.values" för versionerade värdesevent). Tomt = den
	// ursprungliga källan (thing.updated/mätvärden); satt värde matchar
	// endast indata med samma tagg, så gamla och nya regler aldrig
	// korsaktiveras.
	Event string `yaml:"event,omitempty" json:"event,omitempty"`
	// Relation avgränsar relationsregler till en relationstyp (t.ex.
	// partOf); endast giltig med event things.v1.relations. Utan
	// match.type/subType skulle regeln träffa alla sakers länkar.
	Relation string `yaml:"relation,omitempty" json:"relation,omitempty"`
	// RelationRemoved skiljer bortkoppling (true) från koppling (osatt).
	// Jämförs med likhet: osatt matchar endast set-händelser.
	RelationRemoved bool `yaml:"relationRemoved,omitempty" json:"relationRemoved,omitempty"`
	// Lifecycle avgränsar livscykelregler till en händelsetyp (created,
	// updated, deleted); endast giltig med event things.v1.lifecycle.
	// Jämförs som hårt filter utan score-påverkan, så livscykelregler
	// aldrig korsaktiveras av värdes- eller relationshändelser.
	Lifecycle string `yaml:"lifecycle,omitempty" json:"lifecycle,omitempty"`
}

// Entity beskriver en NGSI-LD-entitet som ska skrivas.
type Entity struct {
	Name       string     `yaml:"name,omitempty" json:"name,omitempty"`
	ID         string     `yaml:"id" json:"id"`
	Type       string     `yaml:"type" json:"type"`
	Context    string     `yaml:"context,omitempty" json:"context,omitempty"`
	Create     bool       `yaml:"create,omitempty" json:"create,omitempty"`
	Properties []Property `yaml:"properties" json:"properties"`
	// RemoveAttributes rensar namngivna attribut via PATCH-null i stället
	// för att skriva värden; ömsesidigt uteslutande med create, delete och
	// med icke-tomma properties.
	RemoveAttributes []string `yaml:"removeAttributes,omitempty" json:"removeAttributes,omitempty"`
	// Delete raderar hela NGSI-LD-entiteten (idempotent, 404 = OK);
	// kräver match.lifecycle: deleted; ömsesidigt uteslutande med create,
	// removeAttributes och icke-tomma properties.
	Delete bool `yaml:"delete,omitempty" json:"delete,omitempty"`
}

// Source beskriver var ett värde hämtas. Exakt en källa ska vara satt.
type Source struct {
	Resource  string `yaml:"resource,omitempty" json:"resource,omitempty"`
	Object    string `yaml:"object,omitempty" json:"object,omitempty"`
	Env       string `yaml:"env,omitempty" json:"env,omitempty"`
	Name      string `yaml:"name,omitempty" json:"name,omitempty"`
	Field     string `yaml:"field,omitempty" json:"field,omitempty"`
	Ref       string `yaml:"ref,omitempty" json:"ref,omitempty"`
	Const     any    `yaml:"const,omitempty" json:"const,omitempty"`
	LatLon    bool   `yaml:"latlon,omitempty" json:"latlon,omitempty"`
	Timestamp bool   `yaml:"timestamp,omitempty" json:"timestamp,omitempty"`
	Value     string `yaml:"value,omitempty" json:"value,omitempty"`
	Prefix    string `yaml:"prefix,omitempty" json:"prefix,omitempty"`
}

// Transform är ett namngivet steg som appliceras på ett enskilt värde.
type Transform struct {
	Op    string   `yaml:"op" json:"op"`
	Value *float64 `yaml:"value,omitempty" json:"value,omitempty"`
}

// Derive är en namngiven funktion över flera källor.
type Derive struct {
	Fn string   `yaml:"fn" json:"fn"`
	Of []Source `yaml:"of" json:"of"`
}

// Property beskriver ett NGSI-LD-attribut.
type Property struct {
	// Array skriver en skalär Relationship som enelementslista. Listor
	// bevaras platta; flaggan är inte en flerrelationsaggregation.
	Array      bool         `yaml:"array,omitempty" json:"array,omitempty"`
	Target     string       `yaml:"target" json:"target"`
	Type       PropertyType `yaml:"type" json:"type"`
	Unit       string       `yaml:"unit,omitempty" json:"unit,omitempty"`
	ObservedAt ObservedAt   `yaml:"observedAt,omitempty" json:"observedAt,omitempty"`
	ObservedBy string       `yaml:"observedBy,omitempty" json:"observedBy,omitempty"`
	When       When         `yaml:"when,omitempty" json:"when,omitempty"`
	Required   bool         `yaml:"required,omitempty" json:"required,omitempty"`

	Source `yaml:",inline"`

	Transform     []Transform `yaml:"transform,omitempty" json:"transform,omitempty"`
	Derive        *Derive     `yaml:"derive,omitempty" json:"derive,omitempty"`
	Subproperties []Property  `yaml:"subproperties,omitempty" json:"subproperties,omitempty"`
}
