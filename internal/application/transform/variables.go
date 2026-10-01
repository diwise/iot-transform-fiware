package transform

import (
	"fmt"
	"regexp"
	"strings"
)

// Standardvariabler för ID-mallar och relationsvärden. Dynamiska källfält
// är också tillåtna; listan är inte en whitelist för malluttryck.
//
// Mätvärden (message.accepted):
//
//	id, deviceID, sensorType, object, port, timestamp,
//	env, tenant
//
// Saker (things v1):
//
//	id, uuid, type, typeName, subType, name, alternativeName, tenant, timestamp
//
// Dessutom får alltid {{entities.<namn>.id}} referera till en entitet i samma
// regel.
var knownTemplateVariables = map[string]bool{
	"deviceID":        true,
	"sensorType":      true,
	"object":          true,
	"port":            true,
	"name":            true,
	"timestamp":       true,
	"env":             true,
	"tenant":          true,
	"id":              true,
	"uuid":            true,
	"type":            true,
	"typeName":        true,
	"subType":         true,
	"alternativeName": true,
}

// KnownTemplateVariables returnerar en sorterad lista över standardvariabler.
func KnownTemplateVariables() []string {
	return keys(knownTemplateVariables)
}

var entityRefExpr = regexp.MustCompile(`^entities\.([A-Za-z0-9_-]+)\.id$`)

// validateTemplate kontrollerar att alla {{...}}-uttryck använder kända
// variabler, field-vägar, entities-referenser eller hjälpfunktioner.
func validateTemplate(s string) error {
	_, err := parseTemplate(s)
	return err
}

func validateExpr(expr string) error {
	if entityRefExpr.MatchString(expr) {
		return nil
	}

	if strings.HasPrefix(expr, "field:") {
		_, err := parsePath(strings.TrimSpace(strings.TrimPrefix(expr, "field:")))
		return err
	}

	if _, args, call, err := templateCall(expr); call {
		if err != nil {
			return err
		}
		for _, arg := range args {
			if err := validateExpr(arg); err != nil {
				return err
			}
		}
		return nil
	}

	if strings.HasPrefix(expr, "entities.") {
		return fmt.Errorf("invalid entity reference %q", expr)
	}
	_, err := parsePath(expr)
	return err
}

func entityReferences(template string) ([]string, error) {
	parts, err := parseTemplate(template)
	if err != nil {
		return nil, err
	}
	var names []string
	var visit func(string) error
	visit = func(expr string) error {
		if ref := entityRefExpr.FindStringSubmatch(expr); ref != nil {
			names = append(names, ref[1])
			return nil
		}
		_, args, call, err := templateCall(expr)
		if err != nil {
			return err
		}
		if call {
			for _, arg := range args {
				if err := visit(arg); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, part := range parts {
		if part.expression != "" {
			if err := visit(part.expression); err != nil {
				return nil, err
			}
		}
	}
	return names, nil
}
