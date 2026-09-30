package transform

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

type expressionResolver func(string) (string, error)
type templateFunction struct {
	minArgs, maxArgs int // maxArgs=0: obegränsat
	result           PropertyType
	evaluate         func([]string, expressionResolver) (string, error)
}

// Generella textfunktioner. Frånvaro propageras av safeURI, medan
// firstPresent provar nästa argument; syntax-/typfel propageras alltid.
var templateFunctions = map[string]templateFunction{
	"safeURI": {minArgs: 1, maxArgs: 1, result: TypeText, evaluate: func(args []string, resolve expressionResolver) (string, error) {
		value, err := resolve(args[0])
		if err != nil {
			return "", err
		}
		return url.PathEscape(strings.TrimSpace(value)), nil
	}},
	"firstPresent": {minArgs: 1, result: TypeText, evaluate: func(args []string, resolve expressionResolver) (string, error) {
		for _, arg := range args {
			value, err := resolve(arg)
			if errors.Is(err, errExprNotFound) {
				continue
			}
			if err != nil {
				return "", err
			}
			if value != "" {
				return value, nil
			}
		}
		return "", nil
	}},
}

func templateCall(expr string) (templateFunction, []string, bool, error) {
	open := strings.IndexByte(expr, '(')
	if open < 0 {
		return templateFunction{}, nil, false, nil
	}
	// Parentestecken i en citerad fältnyckel är inte ett funktionsanrop.
	if strings.HasPrefix(expr, "field:") {
		return templateFunction{}, nil, false, nil
	}
	fn, ok := templateFunctions[expr[:open]]
	if !ok || !strings.HasSuffix(expr, ")") {
		return fn, nil, true, fmt.Errorf("unknown or malformed function %q", expr)
	}
	args, err := splitArgs(expr[open+1 : len(expr)-1])
	if err != nil {
		return fn, nil, true, err
	}
	if len(args) < fn.minArgs || (fn.maxArgs != 0 && len(args) > fn.maxArgs) {
		return fn, nil, true, fmt.Errorf("invalid argument count for %q", expr[:open])
	}
	return fn, args, true, nil
}

var variablesByKind = map[Kind]map[string]bool{
	KindMeasurement: {"deviceID": true, "sensorType": true, "object": true, "env": true, "tenant": true, "timestamp": true},
	KindThing:       {"id": true, "nameOrID": true, "uuid": true, "type": true, "typeName": true, "subType": true, "name": true, "alternativeName": true, "tenant": true, "timestamp": true},
}

func validateTemplateForKind(template string, kind Kind) error {
	parts, err := parseTemplate(template)
	if err != nil {
		return err
	}
	var check func(string) error
	check = func(expr string) error {
		_, args, call, err := templateCall(expr)
		if err != nil {
			return err
		}
		if call {
			for _, arg := range args {
				if err := check(arg); err != nil {
					return err
				}
			}
			return nil
		}
		if entityRefExpr.MatchString(expr) || strings.HasPrefix(expr, "field:") {
			return nil
		}
		if !variablesByKind[kind][expr] {
			return fmt.Errorf("variable %q is not available for %s", expr, kind)
		}
		return nil
	}
	for _, part := range parts {
		if part.expression != "" {
			if err := check(part.expression); err != nil {
				return err
			}
		}
	}
	return nil
}
