package transform

import (
	"errors"
	"fmt"
	"sort"
)

// TransformOp är de namngivna steg som kan appliceras på ett enskilt värde.
const (
	OpMultiply    = "multiply"
	OpDivide      = "divide"
	OpAdd         = "add"
	OpFloor       = "floor"
	OpRound       = "round"
	OpBoolToOnOff = "boolToOnOff"
	// OpNumToOnOff mappar numerisk närvaro (v2-presence anländer som 1/0,
	// inte bool) till "on"/"off". Befintlig boolToOnOff ändras inte.
	OpNumToOnOff = "numToOnOff"
	OpSafeURI    = "safeURI"
)

// DeriveFn är de namngivna funktioner som kombinerar flera källor.
const (
	FnAnyTrue      = "anyTrue"
	FnAllTrue      = "allTrue"
	FnSum          = "sum"
	FnMin          = "min"
	FnMax          = "max"
	FnCoalesce     = "coalesce"
	FnFirstPresent = "firstPresent"
)

var validTransformOps = map[string]bool{
	OpMultiply:    true,
	OpDivide:      true,
	OpAdd:         true,
	OpFloor:       true,
	OpRound:       true,
	OpBoolToOnOff: true,
	OpNumToOnOff:  true,
	OpSafeURI:     true,
}

// transformNeedsValue markerar de steg som kräver ett numeriskt argument.
var transformNeedsValue = map[string]bool{
	OpMultiply: true,
	OpDivide:   true,
	OpAdd:      true,
}

var validDeriveFns = map[string]bool{
	FnAnyTrue:      true,
	FnAllTrue:      true,
	FnSum:          true,
	FnMin:          true,
	FnMax:          true,
	FnCoalesce:     true,
	FnFirstPresent: true,
}

// KnownTransformOps returnerar namnen på samtliga tillåtna transformsteg.
func KnownTransformOps() []string {
	return keys(validTransformOps)
}

// KnownDeriveFns returnerar namnen på samtliga tillåtna deriveringsfunktioner.
func KnownDeriveFns() []string {
	return keys(validDeriveFns)
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (p Property) validateFunctions() error {
	if len(p.Transform) > 0 && p.Derive != nil {
		return errors.New("transform and derive cannot be combined")
	}

	for _, t := range p.Transform {
		if !validTransformOps[t.Op] {
			return fmt.Errorf("unknown transform.op %q", t.Op)
		}
		if transformNeedsValue[t.Op] && t.Value == nil {
			return fmt.Errorf("transform.op %q requires a value", t.Op)
		}
		if !transformNeedsValue[t.Op] && t.Value != nil {
			return fmt.Errorf("transform.op %q must not declare a value", t.Op)
		}
	}

	if p.Derive != nil {
		if !validDeriveFns[p.Derive.Fn] {
			return fmt.Errorf("unknown derive.fn %q", p.Derive.Fn)
		}
		if len(p.Derive.Of) == 0 {
			return errors.New("derive requires at least one source")
		}
		for i, s := range p.Derive.Of {
			if err := (Property{Source: s}).validateSource(); err != nil {
				return fmt.Errorf("derive.of %d: %w", i, err)
			}
		}
	}

	return nil
}
