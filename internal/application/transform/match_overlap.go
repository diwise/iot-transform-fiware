package transform

import "strings"

// overlaps is conservative: false means the matches cannot select the same
// input. It follows matches' filters, not specificity or rule ordering.
//
// PLAN1050 D: thingTypeVersion/variantId/variantVersion är exakta filter med
// tom wildcard, precis som övriga skalära filter (utom resource). Olika
// explicit angivna värden utesluter överlapp; tomt på ena sidan bevarar
// överlapp. Ingen recency-/revisionsfiltrering ingår.
func (m Match) overlaps(other Match) bool {
	if m.Kind != other.Kind || m.Event != other.Event || m.Lifecycle != other.Lifecycle || m.RelationRemoved != other.RelationRemoved {
		return false
	}
	for _, values := range [][2]string{
		{m.Tenant, other.Tenant},
		{m.Device, other.Device},
		{m.Object, other.Object},
		{m.Env, other.Env},
		{m.Port, other.Port},
		{m.Type, other.Type},
		{m.SubType, other.SubType},
		{m.Relation, other.Relation},
		{m.ToType, other.ToType},
		{m.ThingTypeVersion, other.ThingTypeVersion},
		{m.VariantID, other.VariantID},
		{m.VariantVersion, other.VariantVersion},
	} {
		if values[0] != "" && values[1] != "" && values[0] != values[1] {
			return false
		}
	}
	// Different resources are not disjoint: HasResource is a presence check
	// and a single observation can contain both resources.
	return m.SensorType == "" || other.SensorType == "" || strings.EqualFold(m.SensorType, other.SensorType)
}

// specificityD är D:s kompatibilitets-specificitet: identisk med motorns
// score() (enkelräkning, ingen dubbelräkning). Motorns match() ägs av E och
// använder score(); denna hjälpare används av D:s kompatibilitetskontroll
// för tvetydighetsdetektion med samma rangordning. Högre värde = mer
// specifik regel. Ingen recency-/revisionspåverkan.
func (m Match) specificityD() int {
	return m.score()
}
