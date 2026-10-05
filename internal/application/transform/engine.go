package transform

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/iot-transform-fiware/internal/application/relations"
)

// Input är den normaliserade indata motorn arbetar på. Implementeras av
// Measurement (mätvärden) och Thing (saker).
type Input interface {
	Kind() Kind
	TenantID() string
	MatchInfo() MatchInfo
	Variables() map[string]string
	LatLon() (float64, float64, bool)
	MessageTime() time.Time
	BaseTime() (time.Time, bool)

	// Resolve slår upp en egenskaps värdekälla.
	Resolve(p Property) (resolvedValue, error)
	// ResolveTime slår upp en tid via en field-väg (observedAt: field:<väg>).
	ResolveTime(path string) (time.Time, bool)
	// ResolvePath slår upp ett värde via en JSON-väg/namn (arrayer, Structured).
	ResolvePath(path string) (any, bool)
	// HasResource anger om indatat innehåller en resurs med angivet namn.
	HasResource(name string) bool
}

// errExprNotFound markerar att ett template-uttryck saknas (t.ex. ett
// field-värde). firstPresent behandlar det som tomt och går vidare.
var errExprNotFound = errors.New("template expression not found")

// MatchInfo är de fält en regel kan matcha mot, oavsett indatatyp.
//
// PLAN1050 E: ThingTypeVersion/VariantID/VariantVersion är exakta
// strängfilter för things (tom wildcard), lästa från Thing via explicita
// fält eller C1-källvyer. Mätvärden bär alltid tomma versioner, så
// versionerade things-regler träffar dem aldrig. Ingen recency- eller
// revisionspåverkan.
type MatchInfo struct {
	Device     string
	SensorType string
	Object     string
	Env        string
	Port       string
	Tenant     string
	Type       string
	SubType    string
	// Event taggar händelsekällan (tomt för ursprungliga källor).
	Event string
	// Relation avgränsar relationsregler till en relationstyp; RelationRemoved
	// skiljer bortkoppling från koppling (likhetsmatchning).
	Relation        string
	RelationRemoved bool
	// Lifecycle avgränsar livscykelregler till en händelsetyp
	// (likhetsmatchning, ingen score).
	Lifecycle string
	ToType    string
	// ThingTypeVersion avgränsar things-regler till exakt mallversion.
	ThingTypeVersion string
	// VariantID avgränsar things-regler till exakt variant.
	VariantID string
	// VariantVersion avgränsar things-regler till exakt variantversion.
	VariantVersion string
}

// resolvedValue är resultatet av en värdekälleuppslagning.
type resolvedValue struct {
	value        any
	time         time.Time
	hasTime      bool
	found        bool
	unit         string
	unitConflict bool
}

// EntityWrite beskriver en entitet som ska skrivas, utan att utföra
// skrivningen. Presentationen exekverar skrivningarna i listordning.
// RemoveAttributes rensar namngivna attribut (PATCH-null) i stället för
// att skriva värden; Delete raderar hela entiteten (idempotent).
// Props är då tomma.
type EntityWrite struct {
	RuleID           string
	RuleRevision     int64
	SnapshotVersion  int64
	EntityID         string
	TypeName         string
	Context          string
	Create           bool
	Props            []entities.EntityDecoratorFunc
	RemoveAttributes []string
	Delete           bool
}

// Engine kör den konfigurationsstyrda mappningen.
type Engine struct {
	rules []Rule
}

func NewEngine(rules []Rule) *Engine {
	return &Engine{rules: cloneConfiguration(reflect.ValueOf(rules)).Interface().([]Rule)}
}

// Regler innehåller JSON/YAML-värden, slices, maps och numeriska pekare.
// Kopiera alla muterbara delar så caller inte kan ändra den aktiva motorn.
func cloneConfiguration(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.New(v.Type()).Elem()
		if v.Kind() == reflect.Pointer {
			out.Set(reflect.New(v.Type().Elem()))
			out.Elem().Set(cloneConfiguration(v.Elem()))
		} else {
			out.Set(cloneConfiguration(v.Elem()))
		}
		return out
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := range v.Len() {
			out.Index(i).Set(cloneConfiguration(v.Index(i)))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			out.SetMapIndex(iter.Key(), cloneConfiguration(iter.Value()))
		}
		return out
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		out.Set(v)
		for i := range v.NumField() {
			if out.Field(i).CanSet() && v.Field(i).CanInterface() {
				out.Field(i).Set(cloneConfiguration(v.Field(i)))
			}
		}
		return out
	default:
		return v
	}
}

// Matched rapporterar om någon regel matchar indatat, oavsett om den
// sedan producerar skrivningar. Skiljer "ingen regel" (false) från
// "matchad men undertryckt" (true med tomma writes, t.ex. required som
// saknas) — preview behöver detta för ärlig matched-rapportering (TF-06).
func (e *Engine) Matched(in Input) bool {
	_, ok := e.match(in)
	return ok
}

// MatchedRule returnerar den valda regeln (högst prioritet/specificitet)
// för preview/diagnostik, även när den sedan undertrycks (tomma writes).
// Tenant-avgränsad via matches(); offline (ingen I/O).
func (e *Engine) MatchedRule(in Input) (Rule, bool) {
	return e.match(in)
}

// ExplainSkipped förklarar varför en matchad regel inte producerade
// entiteter: per entitet/attribut med D:s diagnoskoder (required-missing
// m.fl.), tenant-avgränsat via den valda regeln. Tom vid ingen matchning
// eller vid träffar. Pure funktion, ingen I/O, inga modellnamn.
func (e *Engine) ExplainSkipped(in Input) []Diagnostic {
	rule, ok := e.match(in)
	if !ok {
		return nil
	}
	// Snabbväg: om Transform ger skrivningar eller fel är inget undertryckt.
	writes, err := e.Transform(in)
	if err != nil || len(writes) > 0 {
		return nil
	}
	var out []Diagnostic
	tenant := in.TenantID()
	thingID := ""
	if th, ok := in.(Thing); ok {
		thingID = th.ID
	} else if m, ok := in.(Measurement); ok {
		thingID = m.DeviceID
	}
	ids := map[string]string{}
	for index, ent := range rule.Entities {
		id, err := renderTemplate(ent.ID, in, ids)
		if err != nil {
			out = append(out, Diagnostic{
				Code: CodeTargetNotFound, RuleID: rule.ID, RuleRevision: rule.Revision,
				SnapshotVersion: rule.SnapshotVersion, Entity: entityLabel(ent, index),
				Expression: ent.ID, Tenant: tenant, ThingID: thingID,
				Detail: "entity id cannot be rendered from example event",
			})
			continue
		}
		if ent.Name != "" {
			ids[ent.Name] = id
		}
		if ent.Delete || len(ent.RemoveAttributes) > 0 {
			continue
		}
		for _, p := range ent.Properties {
			if _, err := buildProperty(p, in, ids); err != nil {
				out = append(out, skipDiagnostic(rule, ent, index, p, tenant, thingID, err))
			}
		}
		// Entitet utan någon skrivbar property efter skip: rapportera
		// entitetsnivå när ingen attributdiagnos fångade orsaken (t.ex.
		// tom properties-lista efter undertryckning).
	}
	if len(out) == 0 {
		out = append(out, Diagnostic{
			Code: CodeRequiredMissing, RuleID: rule.ID, RuleRevision: rule.Revision,
			SnapshotVersion: rule.SnapshotVersion, Tenant: tenant, ThingID: thingID,
			Detail: "matched rule produced no entities (required source missing or suppressed)",
		})
	}
	return out
}

func entityLabel(ent Entity, index int) string {
	if ent.Name != "" {
		return ent.Name
	}
	return fmt.Sprintf("entity %d", index)
}

// skipDiagnostic mappar byggfel till D:s diagnoskoder (best-effort;
// feltext bevaras i Detail). Okända roller → target-notfound,
// enhetsfel → unit-conflict, geometri → geometry-invalid,
// ID/typ/attribut → ngsild-invalid, övriga källfel → required-missing
// när required, annars invalid-source.
func skipDiagnostic(rule Rule, ent Entity, index int, p Property, tenant, thingID string, err error) Diagnostic {
	msg := err.Error()
	code := CodeInvalidSource
	switch {
	case containsAny(msg, []string{"unknown entity", "references unknown"}):
		code = CodeTargetNotFound
	case containsAny(msg, []string{"source unit", "convert", "incompatible template source units"}):
		code = CodeUnitConflict
	case containsAny(msg, []string{"location", "Geo", "coordinate", "polygon", "geometry"}):
		code = CodeGeometryInvalid
	case containsAny(msg, []string{"entity.id", "entity.type", "attribute", "conflicting types", "URI", "context", "DateTime", "relationship"}):
		code = CodeNGSILDInvalid
	default:
		if errors.Is(err, ErrRequiredSourceMissing) || strings.Contains(msg, ErrRequiredSourceMissing.Error()) {
			code = CodeRequiredMissing
		}
	}
	expr := p.Field
	if expr == "" {
		expr = p.Ref
	}
	if expr == "" && p.Value != "" {
		expr = p.Value
	}
	if expr == "" && p.Derive != nil {
		expr = "derive " + p.Derive.Fn
	}
	return Diagnostic{
		Code: code, RuleID: rule.ID, RuleRevision: rule.Revision,
		SnapshotVersion: rule.SnapshotVersion, Entity: entityLabel(ent, index),
		Attribute: p.Target, Expression: expr, Tenant: tenant, ThingID: thingID,
		Detail: msg,
	}
}

// Transform matchar indatat mot den mest specifika regeln och producerar
// entitetsskrivningar. Ingen matchande regel ger ErrNoMatchingRule.
func (e *Engine) Transform(in Input) ([]EntityWrite, error) {
	rule, ok := e.match(in)
	if !ok {
		return nil, ErrNoMatchingRule
	}

	ids := map[string]string{}
	writes := make([]EntityWrite, 0, len(rule.Entities))

	for index, ent := range rule.Entities {
		id, err := renderTemplate(ent.ID, in, ids)
		if err != nil {
			return nil, fmt.Errorf("rule %q, entity %d (%q), id expression %q: %w", rule.ID, index, ent.Name, ent.ID, err)
		}

		if ent.Name != "" {
			ids[ent.Name] = id
		}

		typeName, err := renderTemplate(ent.Type, in, ids)
		if err != nil {
			return nil, fmt.Errorf("rule %q, entity %d (%q), type expression %q: %w", rule.ID, index, ent.Name, ent.Type, err)
		}
		if err := validateWriteHeader(EntityWrite{EntityID: id, TypeName: typeName, Context: ent.Context}); err != nil {
			return nil, fmt.Errorf("rule %q, entity %d (%q): %w", rule.ID, index, ent.Name, err)
		}

		// delete-entiteter raderar hela entiteten; properties byggs aldrig.
		if ent.Delete {
			writes = append(writes, EntityWrite{
				EntityID: id,
				TypeName: typeName,
				Context:  ent.Context,
				Delete:   true,
			})
			continue
		}

		// removeAttributes-entiteter rensar namngivna attribut i stället
		// för att skriva värden; properties byggs aldrig för dem.
		if len(ent.RemoveAttributes) > 0 {
			writes = append(writes, EntityWrite{
				EntityID:         id,
				TypeName:         typeName,
				Context:          ent.Context,
				RemoveAttributes: append([]string(nil), ent.RemoveAttributes...),
			})
			continue
		}

		props, err := entityProps(ent, in, ids)
		if err != nil {
			if errors.Is(err, ErrRequiredSourceMissing) {
				continue
			}
			return nil, fmt.Errorf("rule %q, entity %d (%q): %w", rule.ID, index, ent.Name, err)
		}
		if len(props) == 0 {
			continue
		}

		writes = append(writes, EntityWrite{
			EntityID: id,
			TypeName: typeName,
			Context:  ent.Context,
			Create:   ent.Create,
			Props:    props,
		})
	}

	for i := range writes {
		writes[i].RuleID = rule.ID
		writes[i].RuleRevision = rule.Revision
		writes[i].SnapshotVersion = rule.SnapshotVersion
	}
	if err := ValidateEntityWrites(writes, ""); err != nil {
		return nil, err
	}
	return writes, nil
}

func entityProps(ent Entity, in Input, ids map[string]string) ([]entities.EntityDecoratorFunc, error) {
	props := make([]entities.EntityDecoratorFunc, 0, len(ent.Properties))

	for _, p := range ent.Properties {
		dec, err := buildProperty(p, in, ids)
		if err != nil {
			return nil, propertyError(p, err)
		}
		if dec == nil {
			continue
		}
		props = append(props, dec)
	}

	return props, nil
}

// match väljer den regel med högst specificitet och därefter lägst
// konfigordning.
func (e *Engine) match(in Input) (Rule, bool) {
	info := in.MatchInfo()
	kind := in.Kind()

	best := -1
	bestScore := -1

	for i, r := range e.rules {
		if r.Match.Kind != kind || !r.Match.matches(in, info) {
			continue
		}

		if s := r.Match.score(); best < 0 || r.Priority > e.rules[best].Priority || (r.Priority == e.rules[best].Priority && s > bestScore) {
			bestScore, best = s, i
		}
	}

	if best < 0 {
		return Rule{}, false
	}

	return e.rules[best], true
}

func (mm Match) matches(in Input, info MatchInfo) bool {
	if mm.Resource != "" && !in.HasResource(mm.Resource) {
		return false
	}
	if mm.Device != "" && mm.Device != info.Device {
		return false
	}
	if mm.SensorType != "" && !strings.EqualFold(mm.SensorType, info.SensorType) {
		return false
	}
	if mm.Object != "" && mm.Object != info.Object {
		return false
	}
	if mm.Env != "" && mm.Env != info.Env {
		return false
	}
	if mm.Port != "" && mm.Port != info.Port {
		return false
	}
	if mm.Type != "" && mm.Type != info.Type {
		return false
	}
	if mm.SubType != "" && mm.SubType != info.SubType {
		return false
	}
	if mm.ToType != "" && mm.ToType != info.ToType {
		return false
	}
	if mm.Tenant != "" && mm.Tenant != info.Tenant {
		return false
	}
	// PLAN1050 D/E: exakt versions-/variantmatchning för things (tom
	// wildcard). Mätvärden bär alltid tomma versioner och träffar därför
	// aldrig versionerade things-regler.
	if mm.ThingTypeVersion != "" && mm.ThingTypeVersion != info.ThingTypeVersion {
		return false
	}
	if mm.VariantID != "" && mm.VariantID != info.VariantID {
		return false
	}
	if mm.VariantVersion != "" && mm.VariantVersion != info.VariantVersion {
		return false
	}
	// Händelsekälla måste stämma exakt; tomt matchar endast omärkt indata
	// (ursprungliga källor), så gamla regler aldrig aktiveras av nya event.
	if mm.Event != info.Event {
		return false
	}
	if mm.Relation != "" && mm.Relation != info.Relation {
		return false
	}
	if mm.RelationRemoved != info.RelationRemoved {
		return false
	}
	if mm.Lifecycle != info.Lifecycle {
		return false
	}
	return true
}

// score rangordnar regler vid samma prioritet. PLAN1050 D-specificitet är
// inkopplad här (E äger match): thingTypeVersion/variantId/variantVersion
// ger +1 vardera (enkelräkning). specificityD() är identisk med score();
// ingen dubbelräkning, ordning bevarad. Ingen recency-/revisionspåverkan.
func (mm Match) score() int {
	s := 0
	if mm.Device != "" {
		s += 4
	}
	if mm.SensorType != "" {
		s += 2
	}
	if mm.Object != "" {
		s++
	}
	if mm.Resource != "" {
		s++
	}
	if mm.Port != "" {
		s++
	}
	if mm.Env != "" {
		s++
	}
	if mm.ToType != "" {
		s++
	}
	if mm.SubType != "" {
		s += 4
	}
	if mm.Type != "" {
		s += 2
	}
	if mm.Tenant != "" {
		s++
	}
	if mm.ThingTypeVersion != "" {
		s++
	}
	if mm.VariantID != "" {
		s++
	}
	if mm.VariantVersion != "" {
		s++
	}
	return s
}

// renderTemplate ersätter uttryck i en mall: {{variabel}},
// {{entities.<namn>.id}}, {{field:<väg>}}, {{safeURI(<uttryck>)}} och
// {{firstPresent(<uttryck>, …)}}.
func renderTemplate(tpl string, in Input, entities map[string]string) (string, error) {
	vars := in.Variables()
	parts, err := parseTemplate(tpl)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	for _, part := range parts {
		out.WriteString(part.text)
		if part.expression == "" {
			continue
		}
		value, err := resolveExpr(part.expression, vars, in, entities)
		if err != nil {
			return "", err
		}
		out.WriteString(value)
	}
	return out.String(), nil
}

func resolveExpr(expr string, vars map[string]string, in Input, entities map[string]string) (string, error) {
	expr = strings.TrimSpace(expr)

	if mm := entityRefExpr.FindStringSubmatch(expr); mm != nil {
		id, ok := entities[mm[1]]
		if !ok {
			return "", fmt.Errorf("unknown entity %q", mm[1])
		}
		return id, nil
	}

	if fn, args, call, err := templateCall(expr); call {
		if err != nil {
			return "", err
		}
		return fn.evaluate(args, func(arg string) (string, error) { return resolveExpr(arg, vars, in, entities) })
	}

	path := strings.TrimSpace(strings.TrimPrefix(expr, "field:"))
	if v, ok := vars[path]; ok {
		if !present(v) {
			return "", fmt.Errorf("%w: variable %q", errExprNotFound, path)
		}
		return v, nil
	}
	v, ok := in.ResolvePath(path)
	if !ok || !present(v) {
		return "", fmt.Errorf("%w: field %q", errExprNotFound, path)
	}
	return templateValue(v)
}

func templateValue(v any) (string, error) {
	if v == nil {
		return "", errExprNotFound
	}
	if t, ok := v.(time.Time); ok {
		return t.UTC().Format(time.RFC3339Nano), nil
	}
	if s, ok := v.(string); ok {
		return s, nil
	}
	// JSON bevarar skalärtyper och gör objekt/listor användbara i exempelvis
	// Geo-mallar, i stället för Go:s icke-JSON-form map[...].
	raw, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("template field is not JSON encodable: %w", err)
	}
	return string(raw), nil
}

func stringify(v any) string {
	if s, ok := toString(v); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// Specificity exponerar motorns rangordningstal för preview/diagnostik
// (samma score som match() använder, inkl. D-versions/variant-specificitet).
// Högre värde = mer specifik regel. Ingen recency-/revisionspåverkan.
func (mm Match) Specificity() int { return mm.score() }

// PendingOperation mappar en EntityWrite till F:s neutrala brokeroperation.
// Protokollverb, inga modellnamn.
func PendingOperation(w EntityWrite) string {
	switch {
	case w.Delete:
		return relations.OpDelete
	case len(w.RemoveAttributes) > 0:
		return relations.OpRemoveAttributes
	case w.Create:
		return relations.OpCreate
	default:
		return relations.OpMerge
	}
}

// EntityWritesToPending serialiserar validerade skrivningar till F:s
// PendingWrite med stabila idempotensnycklar för C2/projektion. Validerar
// faktisk serialiserad NGSI-LD (hela batchen) före någon nyckel/payload
// byggs; ogiltig output ger fel före I/O. Pure funktion (ingen brokerkontakt,
// ingen mutation), deterministisk: samma tenant+skrivning ger samma nyckel
// och payload, så replay är ofarlig (ON CONFLICT DO NOTHING i F-lagret).
// Nyckeln är unik per tenant och bär regel/revision/snapshotversion,
// operation, entitets-ID och payload-hash. Inga modellnamn; payload är
// ogenomskinlig NGSI-LD-JSON som F aldrig tolkar.
func EntityWritesToPending(tenant string, writes []EntityWrite, defaultContext string) ([]relations.PendingWrite, error) {
	if err := ValidateEntityWrites(writes, defaultContext); err != nil {
		return nil, err
	}
	out := make([]relations.PendingWrite, 0, len(writes))
	for _, w := range writes {
		op := PendingOperation(w)
		payload, err := pendingPayload(w, defaultContext)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(payload)
		hash := hex.EncodeToString(sum[:])
		rule := w.RuleID
		if rule == "" {
			rule = "inline"
		}
		key := fmt.Sprintf("%s|%s|%s|%s|rev%d|snap%d|%s",
			tenant, op, w.EntityID, rule, w.RuleRevision, w.SnapshotVersion, hash)
		out = append(out, relations.PendingWrite{
			IdempotencyKey: key,
			Operation:      op,
			EntityID:       w.EntityID,
			Payload:        payload,
			RuleID:         w.RuleID,
			RuleRevision:   w.RuleRevision,
		})
	}
	return out, nil
}

// pendingPayload serialiserar en skrivning till stabil NGSI-LD-JSON.
// Create/merge valideras via BuildEntity (faktisk serialiserad output);
// removeAttributes/delete bär minimal deterministisk JSON med effektiv
// context så att nyckeln förblir stabil utan brokerkontakt.
func pendingPayload(w EntityWrite, defaultContext string) ([]byte, error) {
	if w.Delete {
		contextURL := w.Context
		if contextURL == "" {
			contextURL = defaultContext
		}
		doc := map[string]any{"id": w.EntityID, "type": w.TypeName, "operation": relations.OpDelete}
		if contextURL != "" {
			doc["@context"] = contextURL
		}
		return json.Marshal(doc)
	}
	if len(w.RemoveAttributes) > 0 {
		contextURL := w.Context
		if contextURL == "" {
			contextURL = defaultContext
		}
		attrs := append([]string(nil), w.RemoveAttributes...)
		doc := map[string]any{"id": w.EntityID, "type": w.TypeName, "removeAttributes": attrs, "operation": relations.OpRemoveAttributes}
		if contextURL != "" {
			doc["@context"] = contextURL
		}
		return json.Marshal(doc)
	}
	ent, err := BuildEntity(w, defaultContext)
	if err != nil {
		return nil, err
	}
	return json.Marshal(ent)
}
