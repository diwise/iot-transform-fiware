package things

type Thing struct {
	ID              string  `json:"id"`
	Type            string  `json:"type"`
	SubType         *string `json:"subType,omitempty"`
	Name            string  `json:"name"`
	AlternativeName string  `json:"alternativeName,omitempty"`
	Tenant          string  `json:"tenant"`
}

func (t Thing) TypeName() string {
	typeName := t.Type

	if t.SubType != nil && *t.SubType != "" {
		typeName = *t.SubType
	}

	return typeName
}
