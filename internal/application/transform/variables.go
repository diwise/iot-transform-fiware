package transform

import (
	"fmt"
	"regexp"
	"strings"
)

// Templatevariabler som får användas i id-mallar och relationsvärden.
//
// Mätvärden (context-broker/iot-agent):
//
//	deviceID, sensorType, port, object, resource, name, value, timestamp,
//	env, tenant
//
// Saker (iot-things):
//
//	id, nameOrID, uuid, type, subType, name, alternativeName, tenant
//
// Dessutom får alltid {{entities.<namn>.id}} referera till en entitet i samma
// regel.
var knownTemplateVariables = map[string]bool{
	"deviceID":        true,
	"sensorType":      true,
	"port":            true,
	"object":          true,
	"resource":        true,
	"name":            true,
	"value":           true,
	"timestamp":       true,
	"env":             true,
	"ref":             true,
	"tenant":          true,
	"id":              true,
	"nameOrID":        true,
	"uuid":            true,
	"type":            true,
	"subType":         true,
	"alternativeName": true,
}

// KnownTemplateVariables returnerar en sorterad lista över tillåtna variabler.
func KnownTemplateVariables() []string {
	return keys(knownTemplateVariables)
}

var (
	templateExprRe = regexp.MustCompile(`\{\{\s*([^{}]+?)\s*\}\}`)
	entityRefExpr  = regexp.MustCompile(`^entities\.([A-Za-z0-9_-]+)\.id$`)
	safeURIExpr    = regexp.MustCompile(`^safeURI\(\s*([A-Za-z0-9_]+)\s*\)$`)
)

// validateTemplate kontrollerar att alla {{...}}-uttryck använder kända
// variabler eller entities-referenser.
func validateTemplate(s string) error {
	for _, m := range templateExprRe.FindAllStringSubmatch(s, -1) {
		expr := strings.TrimSpace(m[1])

		if entityRefExpr.MatchString(expr) {
			continue
		}

		if mm := safeURIExpr.FindStringSubmatch(expr); mm != nil {
			if !knownTemplateVariables[mm[1]] {
				return fmt.Errorf("unknown template variable %q in safeURI", mm[1])
			}
			continue
		}

		if !knownTemplateVariables[expr] {
			return fmt.Errorf("unknown template variable %q", expr)
		}
	}

	return nil
}
