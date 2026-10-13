package transform

import (
	"fmt"
	"slices"
	"strings"
)

// MatchDiagnostic describes ranking for one input; it never combines rules.
type MatchDiagnostic struct {
	Index           int    `json:"index"`
	RuleID          string `json:"ruleId"`
	Revision        int64  `json:"revision"`
	SnapshotVersion int64  `json:"snapshotVersion"`
	Priority        int    `json:"priority"`
	Specificity     int    `json:"specificity"`
	Winner          bool   `json:"winner"`
	Reason          string `json:"reason"`
}

func (e *Engine) ExplainMatch(in Input) []MatchDiagnostic {
	selected, found := e.match(in)
	if !found {
		return []MatchDiagnostic{}
	}
	result := make([]MatchDiagnostic, 0)
	chosen := false
	for i, r := range e.rules {
		if r.Match.Kind != in.Kind() || !r.Match.matches(in, in.MatchInfo()) {
			continue
		}
		reason := "lower priority"
		if r.Priority == selected.Priority {
			reason = "lower specificity"
			if r.Match.score() == selected.Match.score() {
				reason = "later snapshot order"
				if !chosen {
					reason = "winner"
					chosen = true
				}
			}
		}
		result = append(result, MatchDiagnostic{Index: i, RuleID: r.ID, Revision: r.Revision, SnapshotVersion: r.SnapshotVersion, Priority: r.Priority, Specificity: r.Match.score(), Winner: reason == "winner", Reason: reason})
	}
	return result
}

type RuleOverlap struct {
	First        int    `json:"first"`
	Second       int    `json:"second"`
	FirstRuleID  string `json:"firstRuleId"`
	SecondRuleID string `json:"secondRuleId"`
}

// PotentialOverlaps is conservative. Actual per-input shadowing is explained
// by ExplainMatch; an overlap alone does not mean a rule is globally unreachable.
func (e *Engine) PotentialOverlaps() []RuleOverlap {
	result := make([]RuleOverlap, 0)
	for i, r := range e.rules {
		for j := range i {
			if r.Match.overlaps(e.rules[j].Match) {
				result = append(result, RuleOverlap{First: j, Second: i, FirstRuleID: e.rules[j].ID, SecondRuleID: r.ID})
			}
		}
	}
	return result
}

// PLAN1050 D: strukturerad diagnostik och mall–regel-kompatibilitet.
//
// Diagnostik bär {code,ruleId,ruleRevision,snapshotVersion,entity,attribute,
// expression,tenant,thingId,configRevision} med koderna
// required-missing|invalid-source|no-match|ambiguous-match|unit-conflict|
// geometry-invalid|ngsild-invalid|revision-conflict|tenant-denied|
// target-notfound|cardinality|deleted-target.
//
// Full JSON-LD-context-expansion, versionerad SDM-schemavalidering och
// brokerns faktiska entitetsexistens verifieras INTE (se NGSI-LD-profilen):
// valideringen kontrollerar faktisk serialiserad output via BuildEntity/
// ValidateEntityWrites, inget mer.
//
// Extra mallunderlag (TemplateVersion/SourceContract) är ett explicit,
// valfritt indata till kompatibilitetskontrollen — aldrig ett dolt,
// obligatoriskt kataloguppslag per mätning. Körningen använder eventets
// egna thingTypeVersion/variantId/variantVersion/descriptor, inte en
// katalog. Utan underlag kontrolleras endast statiskt omöjliga
// konstruktioner; dynamiska/valfria källor ger skip, inte fel.

// DiagnosticCode är de avtalade diagnoskoderna.
type DiagnosticCode string

const (
	CodeRequiredMissing  DiagnosticCode = "required-missing"
	CodeInvalidSource    DiagnosticCode = "invalid-source"
	CodeNoMatch          DiagnosticCode = "no-match"
	CodeAmbiguousMatch   DiagnosticCode = "ambiguous-match"
	CodeUnitConflict     DiagnosticCode = "unit-conflict"
	CodeGeometryInvalid  DiagnosticCode = "geometry-invalid"
	CodeNGSILDInvalid    DiagnosticCode = "ngsild-invalid"
	CodeRevisionConflict DiagnosticCode = "revision-conflict"
	CodeTenantDenied     DiagnosticCode = "tenant-denied"
	CodeTargetNotFound   DiagnosticCode = "target-notfound"
	CodeCardinality      DiagnosticCode = "cardinality"
	CodeDeletedTarget    DiagnosticCode = "deleted-target"
)

// Diagnostic är en strukturerad regel-/kompatibilitetsdiagnos. Entity är
// målrollen (Entity.Name) eller "entity <n>" vid anonym entitet; Attribute
// är propertyns target; Expression är den granskade källvägen/mallen.
type Diagnostic struct {
	Code            DiagnosticCode `json:"code"`
	RuleID          string         `json:"ruleId,omitempty"`
	RuleRevision    int64          `json:"ruleRevision,omitempty"`
	SnapshotVersion int64          `json:"snapshotVersion,omitempty"`
	Entity          string         `json:"entity,omitempty"`
	Attribute       string         `json:"attribute,omitempty"`
	Expression      string         `json:"expression,omitempty"`
	Tenant          string         `json:"tenant,omitempty"`
	ThingID         string         `json:"thingId,omitempty"`
	ConfigRevision  int64          `json:"configRevision,omitempty"`
	Detail          string         `json:"detail,omitempty"`
}

func (d Diagnostic) Error() string {
	parts := []string{string(d.Code)}
	if d.RuleID != "" {
		parts = append(parts, "rule "+d.RuleID)
	}
	if d.Entity != "" {
		parts = append(parts, "entity "+d.Entity)
	}
	if d.Attribute != "" {
		parts = append(parts, "attribute "+d.Attribute)
	}
	if d.Expression != "" {
		parts = append(parts, "expression "+d.Expression)
	}
	if d.Detail != "" {
		parts = append(parts, d.Detail)
	}
	return strings.Join(parts, ": ")
}

// TemplateVersion är det explicita mallunderlaget för kompatibilitet.
// Generiska strängar, inga modellnamn i motorn: värden jämförs exakt.
// Tom ThingTypeVersion/VariantID/VariantVersion är wildcard i matchningen
// (tom version matchar alla men regeln måste deklarera thingType).
type TemplateVersion struct {
	ThingType        string `json:"thingType"`
	SubType          string `json:"subType,omitempty"`
	ThingTypeVersion string `json:"thingTypeVersion,omitempty"`
	VariantID        string `json:"variantId,omitempty"`
	VariantVersion   string `json:"variantVersion,omitempty"`
}

// PropertyContract beskriver en känd källegenskap i values/snapshot/delta.
// Unit är källans enhet om känd (tom = okänd/dynamisk, ingen konfliktprövning).
type PropertyContract struct {
	ID   string `json:"id"`
	Unit string `json:"unit,omitempty"`
}

// RelationSlot beskriver en namngiven relationsplats. AllowedTargets är
// tillåtna måltyper (tom = okänt/dynamiskt). Max 0 betyder obegränsat.
type RelationSlot struct {
	Name           string   `json:"name"`
	AllowedTargets []string `json:"allowedTargets,omitempty"`
	Min            int      `json:"min,omitempty"`
	Max            int      `json:"max,omitempty"`
}

// SourceContract är det explicita källunderlaget. Tom/nil kontrakt betyder
// okänt underlag: endast statiskt omöjliga konstruktioner rapporteras,
// dynamiska/valfria källor ger aldrig falsk fullständig garanti.
type SourceContract struct {
	Properties []PropertyContract `json:"properties,omitempty"`
	Relations  []RelationSlot     `json:"relations,omitempty"`
	Geometry   bool               `json:"geometry,omitempty"`
	Location   bool               `json:"location,omitempty"`
	Metadata   []string           `json:"metadata,omitempty"`
}

// CompatRequest är offline/API-indata för kompatibilitet: explicit
// mallversion + källkontrakt mot en kandidatregeluppsättning.
type CompatRequest struct {
	Template       TemplateVersion `json:"template"`
	Sources        SourceContract  `json:"sources"`
	Rules          []Rule          `json:"rules"`
	Tenant         string          `json:"tenant,omitempty"`
	ThingID        string          `json:"thingId,omitempty"`
	ConfigRevision int64           `json:"configRevision,omitempty"`
}

// CompatResult är utfallet: Compatible gäller endast utan fel-diagnostik.
// Ambiguous-match rapporteras som diagnostik (ordning avgör i motorn).
type CompatResult struct {
	Compatible  bool         `json:"compatible"`
	Diagnostics []Diagnostic `json:"diagnostics"`
}

// CheckCompatibility kontrollerar en kandidatregeluppsättning mot explicit
// mallversion och källkontrakt. Pure offline-funktion: ingen I/O, inget
// kataloguppslag, inga modellnamn. Användbar offline och via API
// (POST /models/compat). Seed/regel-API/snapshot/preview använder samma
// Validate för regelgiltighet; detta lager lägger mallunderlag ovanpå.
func CheckCompatibility(req CompatRequest) CompatResult {
	result := CompatResult{Compatible: true}
	add := func(d Diagnostic) {
		if d.Tenant == "" {
			d.Tenant = req.Tenant
		}
		if d.ThingID == "" {
			d.ThingID = req.ThingID
		}
		if d.ConfigRevision == 0 {
			d.ConfigRevision = req.ConfigRevision
		}
		result.Diagnostics = append(result.Diagnostics, d)
		result.Compatible = false
	}

	// 1. Gemensam giltighet för hela kandidatuppsättningen (samma som
	// YAML/seed/API/snapshot/preview). Fångar okända roller, cykler,
	// framåt-/självref, dubblett-target och oförenliga ID/typdefinitioner.
	if err := Validate(req.Rules); err != nil {
		add(mapValidateError(req, err))
		return result
	}

	// 2. Per-regel match mot mallen (typ/version/variant/tenant).
	matched := make([]int, 0, len(req.Rules))
	for i, r := range req.Rules {
		d, ok := matchTemplateDiagnostic(r, i, req)
		if ok {
			matched = append(matched, i)
			continue
		}
		if d.Code != "" {
			add(d)
			continue
		}
		matched = append(matched, i)
	}
	if len(matched) == 0 && len(req.Rules) > 0 {
		add(Diagnostic{Code: CodeNoMatch, Tenant: req.Tenant, ThingID: req.ThingID, ConfigRevision: req.ConfigRevision,
			Detail: fmt.Sprintf("no candidate rule matches template %q version %q variant %q/%q", req.Template.ThingType, req.Template.ThingTypeVersion, req.Template.VariantID, req.Template.VariantVersion)})
		return result
	}

	// 3. Tvetydiga matchningar: samma prioritet och D-specificitet med
	// överlappande matchningar (ordning avgör i motorn).
	for a := 0; a < len(matched); a++ {
		for b := a + 1; b < len(matched); b++ {
			i, j := matched[a], matched[b]
			ri, rj := req.Rules[i], req.Rules[j]
			if ri.Match.Kind != rj.Match.Kind || ri.Match.Event != rj.Match.Event {
				continue
			}
			if !ri.Match.overlaps(rj.Match) {
				continue
			}
			if ri.Priority == rj.Priority && ri.Match.specificityD() == rj.Match.specificityD() {
				add(Diagnostic{Code: CodeAmbiguousMatch, RuleID: rj.ID, RuleRevision: rj.Revision, SnapshotVersion: rj.SnapshotVersion,
					Tenant: req.Tenant, ThingID: req.ThingID, ConfigRevision: req.ConfigRevision,
					Detail: fmt.Sprintf("rules %q and %q overlap with equal priority/specificity; order decides", ri.ID, rj.ID)})
			}
		}
	}

	// 4. Källkontrakt mot matchande regler (endast med explicit underlag).
	if len(req.Sources.Properties) > 0 || len(req.Sources.Relations) > 0 || req.Sources.Geometry || req.Sources.Location || len(req.Sources.Metadata) > 0 {
		for _, i := range matched {
			for _, d := range checkRuleSources(req.Rules[i], req) {
				add(d)
			}
		}
	}

	// 5. Målidentitetskonsistens mellan objekt-/relations-/lifecycle-regler
	// täcks av Validate (konfliktkontroll vid överlapp) samt per-regel
	// dubblett-target. En sak→flera mål är avsett (deklarerad ordning).
	return result
}

// matchTemplateDiagnostic rapporterar om en regel inte gäller för mallen.
// (ok=true) betyder match; (Diagnostic{Code:""}) betyder varken match eller
// fel kan avgöras utan underlag (dynamiskt). Tenant-fel ger tenant-denied,
// övriga missar ger no-match (ingen falsk garanti utan explicit version).
func matchTemplateDiagnostic(r Rule, index int, req CompatRequest) (Diagnostic, bool) {
	base := Diagnostic{RuleID: r.ID, RuleRevision: r.Revision, SnapshotVersion: r.SnapshotVersion,
		Tenant: req.Tenant, ThingID: req.ThingID, ConfigRevision: req.ConfigRevision}
	if r.Match.Kind != KindThing {
		// Mätvärdesregler berörs inte av mallversion/variant (additivt,
		// bakåtkompatibelt): de är dynamiska för denna kontroll.
		return Diagnostic{}, true
	}
	t := req.Template
	if r.Match.Type != "" && t.ThingType != "" && r.Match.Type != t.ThingType {
		d := base
		d.Code = CodeNoMatch
		d.Detail = fmt.Sprintf("rule %d match.type %q does not match template %q", index, r.Match.Type, t.ThingType)
		return d, false
	}
	if r.Match.SubType != "" && t.SubType != "" && r.Match.SubType != t.SubType {
		d := base
		d.Code = CodeNoMatch
		d.Detail = fmt.Sprintf("rule %d match.subType %q does not match template subType %q", index, r.Match.SubType, t.SubType)
		return d, false
	}
	if r.Match.ThingTypeVersion != "" && t.ThingTypeVersion != "" && r.Match.ThingTypeVersion != t.ThingTypeVersion {
		d := base
		d.Code = CodeNoMatch
		d.Detail = fmt.Sprintf("rule %d match.thingTypeVersion %q does not match template version %q", index, r.Match.ThingTypeVersion, t.ThingTypeVersion)
		return d, false
	}
	if r.Match.VariantID != "" && t.VariantID != "" && r.Match.VariantID != t.VariantID {
		d := base
		d.Code = CodeNoMatch
		d.Detail = fmt.Sprintf("rule %d match.variantId %q does not match template variant %q", index, r.Match.VariantID, t.VariantID)
		return d, false
	}
	if r.Match.VariantVersion != "" && t.VariantVersion != "" && r.Match.VariantVersion != t.VariantVersion {
		d := base
		d.Code = CodeNoMatch
		d.Detail = fmt.Sprintf("rule %d match.variantVersion %q does not match template variant version %q", index, r.Match.VariantVersion, t.VariantVersion)
		return d, false
	}
	if r.Match.Tenant != "" && req.Tenant != "" && r.Match.Tenant != req.Tenant {
		d := base
		d.Code = CodeTenantDenied
		d.Tenant = req.Tenant
		d.Detail = fmt.Sprintf("rule %d tenant %q is outside requesting tenant %q", index, r.Match.Tenant, req.Tenant)
		return d, false
	}
	return Diagnostic{}, true
}

// checkRuleSources kontrollerar egenskaps-ID:n, relationsnamn/tillåtna mål,
// typer, enheter/omvandlingar, geometrier och kardinalitet där underlaget
// räcker. Skiljer omöjligt (fel) från dynamiskt/valfritt (tyst tillåtet).
func checkRuleSources(r Rule, req CompatRequest) []Diagnostic {
	var out []Diagnostic
	knownProps := map[string]PropertyContract{}
	for _, p := range req.Sources.Properties {
		knownProps[p.ID] = p
	}
	knownRels := map[string]RelationSlot{}
	for _, rel := range req.Sources.Relations {
		knownRels[rel.Name] = rel
	}
	hasContract := len(req.Sources.Properties) > 0 || len(req.Sources.Relations) > 0

	for ei, e := range r.Entities {
		role := e.Name
		if role == "" {
			role = fmt.Sprintf("entity %d", ei)
		}
		// Relationsregel: relationsnamn och tillåtna mål mot kontraktet.
		if r.Match.Event == EventThingsV1Relations && r.Match.Relation != "" && hasContract && len(req.Sources.Relations) > 0 {
			slot, ok := knownRels[r.Match.Relation]
			if !ok {
				out = append(out, Diagnostic{Code: CodeTargetNotFound, RuleID: r.ID, RuleRevision: r.Revision, SnapshotVersion: r.SnapshotVersion,
					Entity: role, Expression: r.Match.Relation, Tenant: req.Tenant, ThingID: req.ThingID, ConfigRevision: req.ConfigRevision,
					Detail: fmt.Sprintf("relation %q not in source contract", r.Match.Relation)})
			} else if len(slot.AllowedTargets) > 0 && r.Match.ToType != "" {
				allowed := slices.Contains(slot.AllowedTargets, r.Match.ToType)
				if !allowed {
					out = append(out, Diagnostic{Code: CodeTargetNotFound, RuleID: r.ID, RuleRevision: r.Revision, SnapshotVersion: r.SnapshotVersion,
						Entity: role, Expression: r.Match.ToType, Tenant: req.Tenant, ThingID: req.ThingID, ConfigRevision: req.ConfigRevision,
						Detail: fmt.Sprintf("relation %q target type %q not allowed", r.Match.Relation, r.Match.ToType)})
				}
			}
		}
		// Delete-regler mot borttaget mål: lifecycle deleted utan descriptor
		// är avsett (idempotent radering), ingen kardinalitetsprövning här.
		if e.Delete {
			continue
		}
		if len(e.RemoveAttributes) > 0 {
			continue
		}
		for _, p := range e.Properties {
			out = append(out, checkPropertySources(r, role, p, req, knownProps)...)
		}
		// Create kräver full snapshot: alla required-källor måste finnas i
		// kontraktet. Giltig PATCH (delmängd) ≠ giltig create.
		if e.Create && len(req.Sources.Properties) > 0 {
			for _, p := range e.Properties {
				if !p.Required {
					continue
				}
				for _, id := range referencedPropertyIDs(p) {
					if _, ok := knownProps[id]; !ok {
						out = append(out, Diagnostic{Code: CodeRequiredMissing, RuleID: r.ID, RuleRevision: r.Revision, SnapshotVersion: r.SnapshotVersion,
							Entity: role, Attribute: p.Target, Expression: id, Tenant: req.Tenant, ThingID: req.ThingID, ConfigRevision: req.ConfigRevision,
							Detail: "create requires validated snapshot with all required sources"})
					}
				}
			}
		}
	}
	return out
}

// checkPropertySources granskar en property mot källkontraktet.
func checkPropertySources(r Rule, role string, p Property, req CompatRequest, knownProps map[string]PropertyContract) []Diagnostic {
	var out []Diagnostic
	base := Diagnostic{RuleID: r.ID, RuleRevision: r.Revision, SnapshotVersion: r.SnapshotVersion,
		Entity: role, Attribute: p.Target, Tenant: req.Tenant, ThingID: req.ThingID, ConfigRevision: req.ConfigRevision}

	// Geometrikrav där underlag räcker: required Geo mot location/geometry.
	if p.Type == TypeGeo && p.Required && (p.Field == "location" || p.Field == "geometry") {
		if p.Field == "location" && !req.Sources.Location && !req.Sources.Geometry && len(req.Sources.Properties) > 0 {
			d := base
			d.Code = CodeGeometryInvalid
			d.Expression = p.Field
			d.Detail = "required Point shortcut has no location/geometry in source contract"
			out = append(out, d)
		}
		if p.Field == "geometry" && !req.Sources.Geometry && len(req.Sources.Properties) > 0 {
			d := base
			d.Code = CodeGeometryInvalid
			d.Expression = p.Field
			d.Detail = "required geometry has no geometry in source contract"
			out = append(out, d)
		}
	}

	ids := referencedPropertyIDs(p)
	if len(req.Sources.Properties) == 0 {
		// Utan explicit egenskapsunderlag: dynamiskt, ingen garanti.
		// Enhetskonflikter kan ändå inte uteslutas här (källenheter är
		// datadrivna); de prövas vid körning/preview.
		return out
	}
	for _, id := range ids {
		contract, ok := knownProps[id]
		if !ok {
			if p.Required {
				d := base
				d.Code = CodeRequiredMissing
				d.Expression = id
				d.Detail = fmt.Sprintf("required source property %q not in contract", id)
				out = append(out, d)
			}
			// Valfri saknad källa är dynamisk skip, inte fel.
			continue
		}
		// Enhetskonflikt där underlag räcker: känd källenhet mot
		// deklarerad unit/convert.to utan ekvivalens eller convert.
		if contract.Unit != "" && (p.Type == TypeNumber || p.Type == TypeNumberArray) {
			want := p.Unit
			if p.Convert != nil {
				want = p.Convert.To
				if canonicalUnit(contract.Unit) != canonicalUnit(p.Convert.From) {
					d := base
					d.Code = CodeUnitConflict
					d.Expression = id
					d.Detail = fmt.Sprintf("source unit %q does not match convert.from %q", contract.Unit, p.Convert.From)
					out = append(out, d)
					continue
				}
			}
			if want != "" && canonicalUnit(contract.Unit) != canonicalUnit(want) && p.Convert == nil {
				d := base
				d.Code = CodeUnitConflict
				d.Expression = id
				d.Detail = fmt.Sprintf("source unit %q cannot be relabeled %q without convert", contract.Unit, want)
				out = append(out, d)
			}
		}
	}
	return out
}

// referencedPropertyIDs samlar egenskaps-ID:n under values/snapshot/delta
// från field/ref, {{field:...}}-mallar och derive.of. Citerade nycklar
// (values."a.b".value) normaliseras till rå ID-sträng. evidence
// observations[]/event.*/descriptor.* ger inga ID:n (proveniens/descriptor,
// aldrig skrivbara värden).
func referencedPropertyIDs(p Property) []string {
	seen := map[string]bool{}
	var out []string
	add := func(id string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	for _, path := range collectFieldPaths(p) {
		if id, ok := valuesPropertyID(path); ok {
			add(id)
		}
	}
	return out
}

func collectFieldPaths(p Property) []string {
	var out []string
	if p.Field != "" {
		out = append(out, p.Field)
	}
	if p.Ref != "" {
		out = append(out, p.Ref)
	}
	if after, ok := strings.CutPrefix(string(p.ObservedAt), "field:"); ok && after != "" {
		out = append(out, after)
	}
	out = append(out, templateFieldPaths(p.Value)...)
	out = append(out, templateFieldPaths(p.ObservedBy)...)
	if p.Derive != nil {
		for _, s := range p.Derive.Of {
			if s.Field != "" {
				out = append(out, s.Field)
			}
			if s.Ref != "" {
				out = append(out, s.Ref)
			}
			out = append(out, templateFieldPaths(s.Value)...)
		}
	}
	for _, sp := range p.Subproperties {
		out = append(out, collectFieldPaths(sp)...)
	}
	return out
}

func templateFieldPaths(tpl string) []string {
	parts, err := parseTemplate(tpl)
	if err != nil {
		return nil
	}
	var out []string
	var visit func(string)
	visit = func(expr string) {
		expr = strings.TrimSpace(expr)
		if after, ok := strings.CutPrefix(expr, "field:"); ok {
			out = append(out, strings.TrimSpace(after))
			return
		}
		if _, args, call, err := templateCall(expr); call && err == nil {
			for _, arg := range args {
				visit(arg)
			}
			return
		}
		if entityRefExpr.MatchString(expr) {
			return
		}
		// Bart {{väg}}-uttryck (inkl. citerade/indexerade): samla om det är
		// en values/snapshot/delta-väg, annars ignorera (descriptor/event).
		out = append(out, expr)
	}
	for _, part := range parts {
		if part.expression != "" {
			visit(part.expression)
		}
	}
	return out
}

// valuesPropertyID extraherar egenskaps-ID:t ur values/snapshot/delta-vägar.
// "values.temperature.value" → ("temperature", true).
// Citerade ID:n avciteras; [n]-index efter ID:t ignoreras för ID-syfte.
func valuesPropertyID(path string) (string, bool) {
	steps, err := parsePath(path)
	if err != nil || len(steps) < 2 {
		return "", false
	}
	switch steps[0].key {
	case "values", "snapshot", "delta":
		if steps[1].indexed {
			return "", false
		}
		return steps[1].key, true
	}
	return "", false
}

// mapValidateError översätter Validate-fel till strukturerad diagnostik
// (best-effort mappning; feltexten bevaras i Detail). Okända roller och
// referensfel → target-notfound; ID/typ/attributfel → ngsild-invalid;
// enhetsfel → unit-conflict; geometrifel → geometry-invalid; övriga
// källfel → invalid-source.
func mapValidateError(req CompatRequest, err error) Diagnostic {
	msg := err.Error()
	d := Diagnostic{Tenant: req.Tenant, ThingID: req.ThingID, ConfigRevision: req.ConfigRevision, Detail: msg}
	switch {
	case containsAny(msg, []string{"unknown entity", "unknown template", "references unknown", "target-notfound"}):
		d.Code = CodeTargetNotFound
	case containsAny(msg, []string{"unit", "convert", "UnitCode"}):
		d.Code = CodeUnitConflict
	case containsAny(msg, []string{"location", "Geo", "coordinate", "polygon", "geometry"}):
		d.Code = CodeGeometryInvalid
	case containsAny(msg, []string{"entity.id", "entity.type", "attribute", "conflicting types", "duplicate entity", "URI", "context", "DateTime", "relationship"}):
		d.Code = CodeNGSILDInvalid
	case containsAny(msg, []string{"tenant"}):
		d.Code = CodeTenantDenied
	case containsAny(msg, []string{"cardinality", "min", "max"}):
		d.Code = CodeCardinality
	case containsAny(msg, []string{"deleted"}):
		d.Code = CodeDeletedTarget
	case containsAny(msg, []string{"revision", "conflict"}):
		d.Code = CodeRevisionConflict
	default:
		d.Code = CodeInvalidSource
	}
	// Bär regelkontext om den finns i feltexten ("rule <n> (<id>)").
	d.RuleID = req.ThingID
	return d
}

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
