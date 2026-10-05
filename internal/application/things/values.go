package things

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// thingValuesEnvelope är den lokala trådformen för versionerade
// värdesevent (things.v1.values). Den speglar producentens låsta kontrakt
// fält för fält men är avsiktligt en egen typ: kopplingen mellan tjänsterna
// är tråd (topics/JSON), inte Go-moduler, så ingen import av producentens
// paket sker här. JSON avkodas utan DisallowUnknownFields: okända/additiva
// fält tolereras och ignoreras.
//
// Additiv v1-baslinje (PLAN1050 A):
//   - topics things.v1.values/relations/lifecycle, schema v1, contentType
//     oförändrat.
//   - values är alias för delta under en release: gamla payloads utan
//     delta/snapshot tolereras; nya payloads bär delta[]/snapshot[],
//     removed[] och relations[].
//   - Varje värde bär egen observedAt/receivedAt/source/messageId/
//     configRevision/unit/quality; snapshot-tid ersätter aldrig dessa tider.
type thingValuesEnvelope struct {
	Schema           string                  `json:"schema"`
	EventID          string                  `json:"eventId"`
	Tenant           string                  `json:"tenant"`
	ThingID          string                  `json:"thingId"`
	ThingType        string                  `json:"thingType"`
	ThingTypeVersion string                  `json:"thingTypeVersion"`
	ConfigRevision   int64                   `json:"configRevision"`
	PublishedAt      time.Time               `json:"publishedAt"`
	Thing            *thingDescriptor        `json:"thing"`
	Values           []thingValue            `json:"values"`
	Delta            []thingValue            `json:"delta"`
	Snapshot         []thingValue            `json:"snapshot"`
	Removed          removedList             `json:"removed"`
	Relations        []thingRelationEnvelope `json:"relations"`
}

type thingValue struct {
	Schema         string    `json:"schema"`
	EventID        string    `json:"eventId"`
	ThingID        string    `json:"thingId"`
	Tenant         string    `json:"tenant"`
	PropertyID     string    `json:"propertyId"`
	Value          *float64  `json:"value"`
	Quality        string    `json:"quality"`
	Unit           string    `json:"unit"`
	Source         string    `json:"source"`
	MessageID      string    `json:"messageId"`
	ObservedAt     time.Time `json:"observedAt"`
	ReceivedAt     time.Time `json:"receivedAt"`
	ComputedAt     time.Time `json:"computedAt"`
	PublishedAt    time.Time `json:"publishedAt"`
	ConfigRevision int64     `json:"configRevision"`
}

// removedList är den explicita rensningslistan (removed[]) i värdeseventet:
// egenskaps-ID:n vars aktuella värde ska rensas (explicit-rensning).
// Femläges-semantik: saknat (varken delta/snapshot/removed) vs ogiltig
// (senaste obs otillåten, ersätter aldrig current) vs tidigare-giltigt
// (snapshot bevarar giltigt current efter ny ogiltig obs) vs borttagen
// (saknas i snapshot efter definitionsborttag) vs explicit-rensning
// (listad i removed). Ogiltig ersätter aldrig current; snapshot blandas
// aldrig med otillåtna värden från senaste ogiltiga obs.
// JSON-formen är additivt tolerant: ["prop"] eller
// [{"propertyId":"prop"}]; tom/missing tolereras för gamla payloads.
type removedList []string

// UnmarshalJSON accepterar strängar eller objekt med propertyId/property/id.
// Okända former är strukturfel (Permanent), aldrig tyst skip.
func (r *removedList) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*r = nil
		return nil
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	// Explicit [] (tom lista) ska skiljas från missing (nil): behåll
	// icke-nil även för tom lista så att adaptern kan exponera removed
	// som tom vy i stället för att utelämna nyckeln.
	out := make([]string, 0, len(raw))
	for i, m := range raw {
		var s string
		if err := json.Unmarshal(m, &s); err == nil {
			if s == "" {
				return fmt.Errorf("things.v1.values: removed[%d] is empty", i)
			}
			out = append(out, s)
			continue
		}
		var obj struct {
			PropertyID string `json:"propertyId"`
			Property   string `json:"property"`
			ID         string `json:"id"`
		}
		if err := json.Unmarshal(m, &obj); err != nil {
			return fmt.Errorf("things.v1.values: removed[%d] must be string or {propertyId}: %s", i, string(m))
		}
		id := obj.PropertyID
		if id == "" {
			id = obj.Property
		}
		if id == "" {
			id = obj.ID
		}
		if id == "" {
			return fmt.Errorf("things.v1.values: removed[%d] missing propertyId: %s", i, string(m))
		}
		out = append(out, id)
	}
	*r = out
	return nil
}

// ThingValuesFromJSON tolkar ett versionerat värdesevent och bygger motorns
// indata. Delad normalisering med preview (paritet produktion/preview):
// preview anropar samma funktion, så D/E/G/I ser identiska källvyer.
//
// Källvyer (exakta Fields-nycklar D/E/G/I bygger mot):
//   - values.<prop>.{value,quality,unit,source,observedAt,receivedAt,
//     computedAt,publishedAt,messageId,configRevision}: alias för delta
//     under en release (bakåtkompatibilitet).
//   - delta.<prop>.{samma}: det som uppdaterades nu.
//   - snapshot.<prop>.{samma}: producentens current-state (samtliga aktuella
//     värden), aldrig sammanfogad av konsumenten och aldrig berikad med
//     otillåtna värden från senaste ogiltiga obs.
//   - descriptor|thing.{thingId,type,typeName,name,alternativeName,subType,
//     category,variantId,variantVersion,metadata,location,geometry}.
//   - event.{schema,eventId,tenant,thingId,thingType,thingTypeVersion,
//     configRevision,publishedAt}.
//   - observations[] (+valuePresent): samtliga delta-observationers
//     kvalitet/proveniens utan numeriska värden (ingen bypass).
//   - relations[]: inbäddade relationsändringar.
//   - removed[]: explicita rensningar.
//
// Kontraktsregler: saknad/ogiltig kvalitet ersätter aldrig aktuellt —
// därför tas endast ok/uncertain-värden med i values/delta/snapshot, så att
// required:true hoppar entiteten när en krävd källa saknas eller är ogiltig.
// Noll/false bevaras (nil-skillnad, inte noll-skillnad); fryst unit bevaras
// verbatim utan normalisering; bool 1.0/0.0 bevaras numeriskt; NaN/Inf är
// strukturfel (Permanent).
//
// Konsumenttid är max(delta[].observedAt), fallback max(snapshot[].observedAt)
// (mättid, aldrig transporttid). Relationstid blir aldrig mättid.
// Schema/content-type tolereras additivt (loggas av presentationen) och
// valideras aldrig till fel här; tom tenant är inget strukturfel
// (presentationen skippar utan broker-kontakt).
func ThingValuesFromJSON(body []byte) (transform.Thing, error) {
	var env thingValuesEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return transform.Thing{}, err
	}
	if env.ThingID == "" {
		return transform.Thing{}, fmt.Errorf("things.v1.values: missing thingId")
	}
	// Tom tenant är inget strukturfel: presentationen skippar utan
	// broker-kontakt (samma grind som thing.updated).

	deltaPresent := env.Delta != nil
	valuesPresent := env.Values != nil
	snapshotPresent := env.Snapshot != nil

	effectiveDelta := env.Delta
	if !deltaPresent {
		effectiveDelta = env.Values
	} else if valuesPresent && len(env.Values) > 0 && len(env.Delta) > 0 {
		// values==delta alias en release: producenten sänder identiskt
		// under övergången. Motstridiga alias utan tyst last-write-wins:
		// olika egenskapsmängder är strukturfel.
		if err := checkValuesDeltaAlias(env.Values, env.Delta); err != nil {
			return transform.Thing{}, err
		}
	}

	deltaLatest, deltaMap, deltaObs, err := buildValueView(effectiveDelta, env.ThingID, env.Tenant, env.ConfigRevision, "delta")
	if err != nil {
		return transform.Thing{}, err
	}
	var snapshotMap map[string]any
	var snapshotLatest time.Time
	if snapshotPresent {
		var snapshotObs []any
		snapshotLatest, snapshotMap, snapshotObs, err = buildValueView(env.Snapshot, env.ThingID, env.Tenant, env.ConfigRevision, "snapshot")
		if err != nil {
			return transform.Thing{}, err
		}
		_ = snapshotObs
	}

	removed, err := buildRemovedView(env.Removed)
	if err != nil {
		return transform.Thing{}, err
	}

	input := descriptorInput(env.ThingID, env.ThingType, env.Tenant, env.Thing)
	input.Event = transform.EventThingsV1Values
	input.ObservedAt = deltaLatest
	if input.ObservedAt.IsZero() {
		input.ObservedAt = snapshotLatest
	}
	// values är alias för delta en release: båda vyerna exponeras med
	// identiskt innehåll så att gamla values.*-regler fortsätter fungera
	// mot delta utan omskrivning.
	valuesAlias := make(map[string]any, len(deltaMap))
	maps.Copy(valuesAlias, deltaMap)
	input.Fields["values"] = valuesAlias
	input.Fields["delta"] = deltaMap
	if snapshotPresent {
		if snapshotMap == nil {
			snapshotMap = map[string]any{}
		}
		input.Fields["snapshot"] = snapshotMap
	}
	input.Fields["observations"] = deltaObs
	if env.Removed != nil {
		input.Fields["removed"] = removed
	}
	event := map[string]any{
		"schema": env.Schema, "eventId": env.EventID, "tenant": env.Tenant, "thingId": env.ThingID,
		"thingType": env.ThingType, "thingTypeVersion": env.ThingTypeVersion, "configRevision": env.ConfigRevision,
	}
	addSourceTime(event, "publishedAt", env.PublishedAt)
	input.Fields["event"] = event
	if len(env.Relations) > 0 {
		relations := make([]any, len(env.Relations))
		for i, relation := range env.Relations {
			fields, err := validatedRelationFields(relation)
			if err != nil {
				return transform.Thing{}, fmt.Errorf("things.v1.values: relations[%d]: %w", i, err)
			}
			relations[i] = fields
		}
		input.Fields["relations"] = relations
	}
	return input, nil
}

// checkValuesDeltaAlias kräver samma egenskapsmängd när både values och
// delta bär data under alias-releasen. Olika mängder är strukturfel i
// stället för tyst vinst för ena fältet.
func checkValuesDeltaAlias(values, delta []thingValue) error {
	valueIDs := make(map[string]struct{}, len(values))
	for _, v := range values {
		if v.PropertyID == "" {
			continue
		}
		valueIDs[v.PropertyID] = struct{}{}
	}
	deltaIDs := make(map[string]struct{}, len(delta))
	for _, v := range delta {
		if v.PropertyID == "" {
			continue
		}
		deltaIDs[v.PropertyID] = struct{}{}
	}
	if len(valueIDs) != len(deltaIDs) {
		return fmt.Errorf("things.v1.values: values/delta alias mismatch (different property sets %d vs %d)", len(valueIDs), len(deltaIDs))
	}
	for id := range valueIDs {
		if _, ok := deltaIDs[id]; !ok {
			return fmt.Errorf("things.v1.values: values/delta alias mismatch (property %q only in values)", id)
		}
	}
	return nil
}

// buildRemovedView validerar removed[] utan tysta last-write-wins:
// tomt ID och dubbletter är strukturfel (Permanent).
func buildRemovedView(removed removedList) ([]any, error) {
	if removed == nil {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(removed))
	out := make([]any, 0, len(removed))
	for i, id := range removed {
		if id == "" {
			return nil, fmt.Errorf("things.v1.values: removed[%d] is empty", i)
		}
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("things.v1.values: duplicate removed %q", id)
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
}

// buildValueView mappar en värdelista till
// <view>.<propertyId>.{value,quality,unit,source,observedAt,...} och
// returnerar senaste mättid, skrivbar vy och observationsvy. Endast
// ok/uncertain med icke-nil finit värde tas med i skrivvyn;
// invalid/okänt/tomt utelämnas så att konsumentregeln gäller (hoppa
// property, required hoppar entitet). Observationsvyn bär samtliga
// observationers kvalitet/proveniens + valuePresent men aldrig numeriska
// värden, så att den inte kan användas för att kringgå grinden.
//
// Strukturfel (Permanent, aldrig tyst last-write-wins):
//   - tom propertyId,
//   - dubblett-propertyId inom samma lista,
//   - motstridig kuvert-/värdeidentitet (thingId/tenant/configRevision),
//   - NaN/Inf-värde.
func buildValueView(list []thingValue, envelopeThingID, envelopeTenant string, envelopeRevision int64, view string) (time.Time, map[string]any, []any, error) {
	var latest time.Time
	byProperty := make(map[string]any, len(list))
	observations := make([]any, 0, len(list))
	seen := make(map[string]struct{}, len(list))
	for i, v := range list {
		if v.PropertyID == "" {
			return time.Time{}, nil, nil, fmt.Errorf("things.v1.values: %s[%d] missing propertyId", view, i)
		}
		if _, dup := seen[v.PropertyID]; dup {
			return time.Time{}, nil, nil, fmt.Errorf("things.v1.values: duplicate %s %q", view, v.PropertyID)
		}
		seen[v.PropertyID] = struct{}{}
		if v.ThingID != "" && v.ThingID != envelopeThingID {
			return time.Time{}, nil, nil, fmt.Errorf("things.v1.values: %s[%d] thingId %q conflicts with envelope %q", view, i, v.ThingID, envelopeThingID)
		}
		if v.Tenant != "" && envelopeTenant != "" && v.Tenant != envelopeTenant {
			return time.Time{}, nil, nil, fmt.Errorf("things.v1.values: %s[%d] tenant %q conflicts with envelope %q", view, i, v.Tenant, envelopeTenant)
		}
		if v.ConfigRevision != 0 && envelopeRevision != 0 && v.ConfigRevision != envelopeRevision {
			return time.Time{}, nil, nil, fmt.Errorf("things.v1.values: %s[%d] configRevision %d conflicts with envelope %d", view, i, v.ConfigRevision, envelopeRevision)
		}
		if v.Value != nil && (math.IsNaN(*v.Value) || math.IsInf(*v.Value, 0)) {
			return time.Time{}, nil, nil, fmt.Errorf("things.v1.values: %s %q has non-finite value (NaN/Inf not allowed)", view, v.PropertyID)
		}
		if v.ObservedAt.After(latest) {
			latest = v.ObservedAt
		}
		provenance := valueProvenance(v)
		provenance["valuePresent"] = v.Value != nil
		observations = append(observations, provenance)
		if v.Quality != "ok" && v.Quality != "uncertain" {
			continue
		}
		if v.Value == nil {
			// Värdelös observation (nullable i kontraktet): behandla som
			// saknad källa så att required hoppar entiteten i stället för
			// att typfel ger omsändningsloop.
			continue
		}
		entry := valueProvenance(v)
		entry["value"] = *v.Value
		byProperty[v.PropertyID] = entry
	}
	return latest, byProperty, observations, nil
}

func valueProvenance(v thingValue) map[string]any {
	fields := map[string]any{
		"schema": v.Schema, "eventId": v.EventID, "thingId": v.ThingID, "tenant": v.Tenant,
		"propertyId": v.PropertyID, "quality": v.Quality, "source": v.Source,
		"messageId": v.MessageID, "configRevision": v.ConfigRevision,
	}
	// Fryst unit: bevaras verbatim, normaliseras aldrig här.
	if v.Unit != "" {
		fields["unit"] = v.Unit
	}
	addSourceTime(fields, "observedAt", v.ObservedAt)
	addSourceTime(fields, "receivedAt", v.ReceivedAt)
	addSourceTime(fields, "computedAt", v.ComputedAt)
	addSourceTime(fields, "publishedAt", v.PublishedAt)
	return fields
}
