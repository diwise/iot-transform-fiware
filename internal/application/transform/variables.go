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
)

// validateTemplate kontrollerar att alla {{...}}-uttryck använder kända
// variabler, field-vägar, entities-referenser eller hjälpfunktioner.
func validateTemplate(s string) error {
	for _, m := range templateExprRe.FindAllStringSubmatch(s, -1) {
		if err := validateExpr(strings.TrimSpace(m[1])); err != nil {
			return err
		}
	}
	return nil
}

func validateExpr(expr string) error {
	if entityRefExpr.MatchString(expr) {
		return nil
	}

	if strings.HasPrefix(expr, "field:") {
		if strings.TrimSpace(strings.TrimPrefix(expr, "field:")) == "" {
			return fmt.Errorf("empty field path in template")
		}
		return nil
	}

	if inner, ok := funcArg(expr, "safeURI"); ok {
		return validateExpr(inner)
	}

	if inner, ok := funcArg(expr, "firstPresent"); ok {
		for _, arg := range splitArgs(inner) {
			if err := validateExpr(arg); err != nil {
				return err
			}
		}
		return nil
	}

	if !knownTemplateVariables[expr] {
		return fmt.Errorf("unknown template variable %q", expr)
	}
	return nil
}
