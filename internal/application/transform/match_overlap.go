package transform

import "strings"

// overlaps is conservative: false means the matches cannot select the same
// input. It follows matches' filters, not specificity or rule ordering.
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
	} {
		if values[0] != "" && values[1] != "" && values[0] != values[1] {
			return false
		}
	}
	// Different resources are not disjoint: HasResource is a presence check
	// and a single observation can contain both resources.
	return m.SensorType == "" || other.SensorType == "" || strings.EqualFold(m.SensorType, other.SensorType)
}
