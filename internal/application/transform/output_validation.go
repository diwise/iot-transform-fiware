package transform

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
)

// Validation implements the service's bounded NGSI-LD profile, not context
// expansion, full ETSI conformance or versioned Smart Data Models validation.
func validateTerm(value string) error {
	if value == "" || !utf8.ValidString(value) {
		return fmt.Errorf("empty or invalid term")
	}
	if strings.Contains(value, ":") {
		return validateURI(value)
	}
	for i, r := range value {
		if unicode.IsLetter(r) || r == '_' || (i > 0 && (unicode.IsDigit(r) || r == '-' || r == '.')) {
			continue
		}
		return fmt.Errorf("invalid compact term %q", value)
	}
	return nil
}

func validateContextURL(value string) error {
	u, err := url.Parse(value)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return fmt.Errorf("context must be an absolute HTTP(S) URL without credentials")
	}
	if err := validateURI(value); err != nil {
		return fmt.Errorf("invalid context URI")
	}
	return nil
}

func validateUnitCode(value string) error {
	if value == "" || !utf8.ValidString(value) || strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return fmt.Errorf("invalid unitCode")
	}
	return nil
}

var timestampPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]{1,3})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$`)
var languagePattern = regexp.MustCompile(`^[A-Za-z]{1,8}(-[A-Za-z0-9]{1,8})*$`)

func validateTimestamp(value string) error {
	if !timestampPattern.MatchString(value) {
		return fmt.Errorf("expected RFC3339 timestamp with at most millisecond precision")
	}
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return fmt.Errorf("invalid date time: %w", err)
	}
	return nil
}

func writeError(w EntityWrite, err error) error {
	return fmt.Errorf("rule %q revision %d snapshot %d, entity %q: %w", w.RuleID, w.RuleRevision, w.SnapshotVersion, w.EntityID, err)
}

func validateWriteHeader(w EntityWrite) error {
	if err := validateURI(w.EntityID); err != nil {
		return fmt.Errorf("entity.id: %w", err)
	}
	if err := validateTerm(w.TypeName); err != nil {
		return fmt.Errorf("entity.type: %w", err)
	}
	if w.Context != "" {
		if err := validateContextURL(w.Context); err != nil {
			return err
		}
	}
	if (w.Delete && (w.Create || len(w.Props) > 0 || len(w.RemoveAttributes) > 0)) || (len(w.RemoveAttributes) > 0 && (w.Create || len(w.Props) > 0)) {
		return fmt.Errorf("conflicting write operations")
	}
	seen := map[string]bool{}
	for _, name := range w.RemoveAttributes {
		if err := validateAttributeName(name); err != nil {
			return err
		}
		if seen[name] {
			return fmt.Errorf("duplicate remove attribute %q", name)
		}
		seen[name] = true
	}
	return nil
}

// BuildEntity copies the decorators and uses the same context shape as the
// broker library. It validates actual serialized JSON before returning it.
func BuildEntity(w EntityWrite, defaultContext string) (types.Entity, error) {
	if err := validateWriteHeader(w); err != nil {
		return nil, writeError(w, err)
	}
	contextURL := w.Context
	if contextURL == "" {
		contextURL = defaultContext
	}
	if contextURL != "" {
		if err := validateContextURL(contextURL); err != nil {
			return nil, writeError(w, err)
		}
	}
	props := append([]entities.EntityDecoratorFunc(nil), w.Props...)
	if contextURL != "" {
		props = append(props, entities.Context([]string{contextURL}))
	} else {
		props = append(props, entities.DefaultContext())
	}
	entity, err := entities.New(w.EntityID, w.TypeName, props...)
	if err != nil {
		return nil, writeError(w, err)
	}
	seen := map[string]bool{}
	var attributeErr error
	if err := entity.ForEachAttribute(func(_, name string, _ any) {
		if attributeErr != nil {
			return
		}
		if err := validateAttributeName(name); err != nil {
			attributeErr = err
			return
		}
		if seen[name] {
			attributeErr = fmt.Errorf("duplicate Property/Relationship attribute %q", name)
		}
		seen[name] = true
	}); err != nil {
		return nil, writeError(w, err)
	}
	if attributeErr != nil {
		return nil, writeError(w, attributeErr)
	}
	raw, err := json.Marshal(entity)
	if err != nil {
		return nil, writeError(w, fmt.Errorf("serialize entity: %w", err))
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, writeError(w, err)
	}
	if body["id"] != w.EntityID || body["type"] != w.TypeName {
		return nil, writeError(w, fmt.Errorf("serialized header differs from write identity/type"))
	}
	contexts, ok := body["@context"].([]any)
	if !ok || len(contexts) != 1 {
		return nil, writeError(w, fmt.Errorf("context profile requires one URL"))
	}
	if value, ok := contexts[0].(string); !ok {
		return nil, writeError(w, fmt.Errorf("context URL required"))
	} else if err := validateContextURL(value); err != nil {
		return nil, writeError(w, err)
	}
	for name, value := range body {
		if name == "id" || name == "type" || name == "@context" {
			continue
		}
		if err := validateAttribute(name, value); err != nil {
			return nil, writeError(w, fmt.Errorf("attribute %q: %w", name, err))
		}
	}
	return entity, nil
}

// ValidateEntityWrites checks every write before executing any part of a batch.
func ValidateEntityWrites(writes []EntityWrite, defaultContext string) error {
	for _, w := range writes {
		if err := validateWriteHeader(w); err != nil {
			return writeError(w, err)
		}
		if w.Delete {
			continue
		}
		if len(w.RemoveAttributes) > 0 {
			contextURL := w.Context
			if contextURL == "" {
				contextURL = defaultContext
			}
			if contextURL != "" {
				if err := validateContextURL(contextURL); err != nil {
					return writeError(w, err)
				}
			}
			continue
		}
		if _, err := BuildEntity(w, defaultContext); err != nil {
			return err
		}
	}
	return nil
}

func validateAttribute(name string, raw any) error {
	if err := validateAttributeName(name); err != nil {
		return err
	}
	attribute, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("normalized attribute object required")
	}
	kind, _ := attribute["type"].(string)
	allowed := map[string]bool{"type": true, "observedAt": true}
	switch kind {
	case "Property":
		allowed["value"], allowed["unitCode"], allowed["observedBy"] = true, true, true
	case "GeoProperty":
		allowed["value"] = true
	case "Relationship":
		allowed["object"] = true
	case "LanguageProperty":
		allowed["languageMap"] = true
	default:
		return fmt.Errorf("unsupported attribute type %q", kind)
	}
	for key := range attribute {
		if !allowed[key] {
			return fmt.Errorf("unsupported attribute metadata %q", key)
		}
	}
	if observed, exists := attribute["observedAt"]; exists {
		text, ok := observed.(string)
		if !ok {
			return fmt.Errorf("observedAt must be a timestamp string")
		}
		if err := validateTimestamp(text); err != nil {
			return fmt.Errorf("observedAt: %w", err)
		}
	}
	switch kind {
	case "Property":
		value, exists := attribute["value"]
		if !exists || value == nil {
			return fmt.Errorf("Property requires non-null value")
		}
		if err := validatePropertyValue(value); err != nil {
			return err
		}
		if code, exists := attribute["unitCode"]; exists {
			text, ok := code.(string)
			if !ok {
				return fmt.Errorf("invalid unitCode")
			}
			if err := validateUnitCode(text); err != nil {
				return err
			}
			if !numericPropertyValue(value) {
				return fmt.Errorf("unitCode requires numeric value or numeric list")
			}
		}
		if observer, exists := attribute["observedBy"]; exists {
			if _, ok := coordinateNumber(value); !ok {
				return fmt.Errorf("observedBy is supported only on numeric scalar Properties")
			}
			object, ok := observer.(map[string]any)
			if !ok || len(object) != 2 || object["type"] != "Relationship" {
				return fmt.Errorf("observedBy requires scalar Relationship")
			}
			uri, ok := object["object"].(string)
			if !ok {
				return fmt.Errorf("observedBy requires URI string")
			}
			if err := validateURI(uri); err != nil {
				return fmt.Errorf("observedBy: %w", err)
			}
		}
	case "GeoProperty":
		if _, err := validateGeoJSON(attribute["value"]); err != nil {
			return err
		}
	case "Relationship":
		if _, err := relationshipObjects(attribute["object"], false); err != nil {
			return err
		}
	case "LanguageProperty":
		values, ok := attribute["languageMap"].(map[string]any)
		if !ok || len(values) == 0 {
			return fmt.Errorf("languageMap requires nonempty language-to-text object")
		}
		for language, value := range values {
			if language != "@none" && !languagePattern.MatchString(language) {
				return fmt.Errorf("invalid language tag %q", language)
			}
			if _, ok := value.(string); !ok {
				return fmt.Errorf("languageMap values must be text")
			}
		}
	}
	return nil
}

func numericPropertyValue(value any) bool {
	if _, ok := coordinateNumber(value); ok {
		return true
	}
	if items, ok := value.([]any); ok && len(items) > 0 {
		for _, item := range items {
			if _, ok := coordinateNumber(item); !ok {
				return false
			}
		}
		return true
	}
	return false
}

func validatePropertyValue(value any) error {
	switch x := value.(type) {
	case string, bool:
		return nil
	case float64:
		if finite(x) {
			return nil
		}
		return fmt.Errorf("finite number required")
	case []any:
		if len(x) == 0 {
			return fmt.Errorf("empty attribute list unsupported")
		}
		_, textList := x[0].(string)
		for _, item := range x {
			if textList {
				if _, ok := item.(string); ok {
					continue
				}
			} else {
				if _, ok := coordinateNumber(item); ok {
					continue
				}
			}
			return fmt.Errorf("only homogeneous text or numeric lists are supported")
		}
		return nil
	case map[string]any:
		if tag, typed := x["@type"]; typed {
			text, ok := x["@value"].(string)
			if tag != "DateTime" || len(x) != 2 || !ok {
				return fmt.Errorf("only typed DateTime values are supported")
			}
			return validateTimestamp(text)
		}
		return validateStructuredJSON(x)
	default:
		return fmt.Errorf("unsupported Property value")
	}
}

func validateStructuredJSON(value any) error {
	switch x := value.(type) {
	case nil, string, bool:
		return nil
	case float64:
		if finite(x) {
			return nil
		}
		return fmt.Errorf("finite structured number required")
	case []any:
		for _, item := range x {
			if err := validateStructuredJSON(item); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		for key, item := range x {
			if key == "@context" {
				return fmt.Errorf("embedded context unsupported")
			}
			if err := validateStructuredJSON(item); err != nil {
				return fmt.Errorf("structured field %q: %w", key, err)
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported structured value")
	}
}
