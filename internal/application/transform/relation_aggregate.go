package transform

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
)

// RelationPlanner combines normal event writes with complete inverse sets.
// Capture the immutable engine once per event/rebuild, never reload rules
// from inside the source-link transaction.
func (e *Engine) RelationPlanner(direct []EntityWrite, defaultContext string) relations.PlanFunc {
	return func(snap relations.Snapshot) ([]relations.PendingWrite, error) {
		writes := append([]EntityWrite(nil), direct...)
		if snap.EndpointDeleted != "" || snap.Reconcile {
			groups := snap.Groups
			for _, group := range groups {
				links := append([]relations.Link(nil), group.Removed...)
				if snap.Reconcile {
					links = append(links, group.Outgoing...)
				}
				for index, l := range links {
					removed := index < len(group.Removed)
					more, err := e.Transform(RelationInput(l, removed))
					if errors.Is(err, ErrNoMatchingRule) {
						continue
					}
					if err != nil {
						return nil, err
					}
					writes = append(writes, more...)
				}
			}
		}
		aggregated, err := e.AggregateRelations(snap)
		if err != nil {
			return nil, err
		}
		writes = append(writes, aggregated...)
		// Never recreate an entity that this same lifecycle batch deletes.
		deleted := map[string]bool{}
		for _, w := range writes {
			if w.Delete {
				deleted[w.EntityID] = true
			}
		}
		filtered := writes[:0]
		for _, w := range writes {
			if !w.Delete && deleted[w.EntityID] {
				continue
			}
			filtered = append(filtered, w)
		}
		pending, err := EntityWritesToPending(snap.Tenant, filtered, defaultContext)
		if err != nil {
			return nil, err
		}
		for i := range pending {
			// Payload-only dedup would suppress a legitimate A->B->A while
			// the first A is still pending. Seen source IDs handle replay.
			pending[i].IdempotencyKey += "|trigger:" + snap.Trigger
		}
		return pending, nil
	}
}

// HasRelationAggregates reports whether this immutable engine needs a
// durable projection. Stateless/inline preview cannot invent a complete set.
func (e *Engine) HasRelationAggregates() bool {
	for _, r := range e.rules {
		if !r.Disabled && RuleHasRelationAggregates(r) {
			return true
		}
	}
	return false
}

func RuleHasRelationAggregates(r Rule) bool {
	for _, ent := range r.Entities {
		for _, p := range ent.Properties {
			if p.Aggregate != nil {
				return true
			}
		}
	}
	return false
}

// RelationInput reconstructs the stable subset retained by source_links.
// Names, geometry, metadata and variant/subtype are deliberately not invented.
func RelationInput(l relations.Link, removed bool) Thing {
	from := map[string]any{"thingId": l.FromID, "type": l.FromKind}
	to := map[string]any{"thingId": l.ToID, "type": l.ToKind}
	rel := map[string]any{
		"type": l.Relation, "fromId": l.FromID, "toId": l.ToID,
		"from": from, "to": to, "removed": removed,
		"fromThingType": l.FromKind, "fromThingTypeVersion": l.FromKindVersion,
		"toThingType": l.ToKind, "toThingTypeVersion": l.ToKindVersion,
		"tenant": l.Tenant, "revision": l.Revision,
	}
	return Thing{ID: l.FromID, Type: l.FromKind, Tenant: l.Tenant,
		ThingTypeVersion: l.FromKindVersion, ObservedAt: l.ChangedAt,
		Event: EventThingsV1Relations, Relation: l.Relation, RelationRemoved: removed,
		Fields: map[string]any{"relation": rel, "relations": []any{rel}, "event": rel}}
}

// AggregateRelations plans complete inverse sets inside the projection's
// transaction. The rule owns its attribute independently of the event's
// set/remove rule selection; membership uses the declaration's match filters.
// No I/O, no broker reads, no retained process-local state.
func (e *Engine) AggregateRelations(snap relations.Snapshot) ([]EntityWrite, error) {
	groups := snap.Groups
	if len(groups) == 0 {
		groups = []relations.Snapshot{snap}
	}
	var out []EntityWrite
	type declaration struct{ rule, entity int }
	seen := map[string]declaration{}
	for _, group := range groups {
		if group.Tenant != snap.Tenant {
			return nil, fmt.Errorf("aggregate: snapshot tenant mismatch")
		}
		anchors := append(append([]relations.Link(nil), group.Outgoing...), group.Removed...)
		if group.Change != nil {
			anchors = append(anchors, relations.LinkFromChange(*group.Change))
		}
		for ruleIndex, r := range e.rules {
			if r.Disabled || !RuleHasRelationAggregates(r) {
				continue
			}
			for entityIndex, ent := range r.Entities {
				for _, p := range ent.Properties {
					if p.Aggregate == nil {
						continue
					}
					for _, anchor := range anchors {
						if anchor.Tenant != snap.Tenant || anchor.ToID == snap.EndpointDeleted || !aggregateOwnerMatches(r, anchor) {
							continue
						}
						members, ok := group.ReverseNew[anchor.ToID]
						if !ok {
							members, ok = group.ReversePrevious[anchor.ToID]
						}
						if !ok {
							return nil, fmt.Errorf("aggregate: missing complete reverse set for %q", anchor.ToID)
						}
						in := RelationInput(anchor, false)
						id, err := renderTemplate(ent.ID, in, nil)
						if err != nil {
							return nil, err
						}
						key := id + "\x00" + p.Target
						owner := declaration{ruleIndex, entityIndex}
						if previous, ok := seen[key]; ok {
							if previous != owner {
								return nil, fmt.Errorf("aggregate: multiple declarations own %q/%q", id, p.Target)
							}
							continue
						}
						seen[key] = owner
						ids := []string{}
						for _, member := range members {
							if member.Tenant != snap.Tenant || member.Relation != group.Relation || member.ToID != anchor.ToID {
								return nil, fmt.Errorf("aggregate: foreign link in reverse set")
							}
							if !aggregateMatches(r, member) {
								continue
							}
							value, err := renderTemplate(p.Value, RelationInput(member, false), nil)
							if err != nil {
								return nil, err
							}
							ids = append(ids, value)
						}
						slices.Sort(ids)
						ids = slices.Compact(ids)
						w := EntityWrite{EntityID: id, TypeName: ent.Type, Context: ent.Context,
							RuleID: r.ID, RuleRevision: r.Revision, SnapshotVersion: r.SnapshotVersion}
						if len(ids) == 0 {
							w.RemoveAttributes = []string{p.Target}
						} else {
							dec, err := buildProperty(Property{Target: p.Target, Type: TypeRelationship,
								Array: true, Const: ids, ObservedAt: ObservedAtNone}, in, nil)
							if err != nil {
								return nil, err
							}
							w.Props = append(w.Props, dec)
						}
						out = append(out, w)
					}
				}
			}
		}
	}
	return out, ValidateEntityWrites(out, "")
}

func aggregateMatches(r Rule, link relations.Link) bool {
	in := RelationInput(link, false)
	return r.Match.Kind == KindThing && r.Match.matches(in, in.MatchInfo())
}

// The owner is affected even when a membership filter no longer matches
// (e.g. a source template version changed). Membership itself still uses
// the full declaration filter; zero eligible members clears the attribute.
func aggregateOwnerMatches(r Rule, link relations.Link) bool {
	return link.Relation == r.Match.Relation && link.ToKind == r.Match.ToType && (r.Match.Tenant == "" || r.Match.Tenant == link.Tenant)
}

func validateRelationAggregate(r Rule, ent Entity) error {
	for _, p := range ent.Properties {
		if p.Aggregate == nil {
			continue
		}
		if p.Aggregate.Direction != "reverse" {
			return fmt.Errorf("aggregate.direction must be reverse")
		}
		m := r.Match
		if m.Kind != KindThing || m.Event != EventThingsV1Relations || m.Relation == "" || m.Type == "" || m.ToType == "" || m.RelationRemoved {
			return fmt.Errorf("aggregate requires a set relation rule with match.relation, type and toType")
		}
		if m.SubType != "" || m.VariantID != "" || m.VariantVersion != "" {
			return fmt.Errorf("aggregate match cannot use subtype/variant: not retained in source_links")
		}
		if len(ent.Properties) != 1 || ent.Create || ent.Delete || len(ent.RemoveAttributes) != 0 || strings.Contains(ent.Type, "{{") {
			return fmt.Errorf("aggregate entity requires one aggregate property, a static type and merge semantics")
		}
		if p.Type != TypeRelationship || p.Array || p.Const != nil || p.Source != (Source{Value: p.Value}) || p.Value == "" ||
			p.When != "" || p.Required || (p.ObservedAt != "" && p.ObservedAt != ObservedAtNone) || p.ObservedBy != "" ||
			p.Unit != "" || p.Convert != nil || p.Derive != nil || len(p.Transform) != 0 || len(p.Subproperties) != 0 {
			return fmt.Errorf("aggregate requires Relationship + value, without array/source/modifiers/observation metadata")
		}
		if err := validateAggregateTemplate(ent.ID, "to"); err != nil {
			return fmt.Errorf("aggregate entity.id: %w", err)
		}
		if err := validateAggregateTemplate(p.Value, "from"); err != nil {
			return fmt.Errorf("aggregate value: %w", err)
		}
	}
	return nil
}

// Stable identity templates make import/rebuild truthful with the minimal
// link metadata. Reject descriptor/name paths and cross-entity role refs.
func validateAggregateTemplate(tpl, side string) error {
	parts, err := parseTemplate(tpl)
	if err != nil {
		return err
	}
	hasID := false
	var check func(string) error
	check = func(expr string) error {
		expr = strings.TrimSpace(expr)
		if _, args, call, err := templateCall(expr); call {
			if err != nil {
				return err
			}
			for _, arg := range args {
				if err := check(arg); err != nil {
					return err
				}
			}
			return nil
		}
		path := strings.TrimSpace(strings.TrimPrefix(expr, "field:"))
		if path == "tenant" {
			return nil
		}
		if path == "relation."+side+"Id" || path == "relation."+side+".thingId" {
			hasID = true
			return nil
		}
		return fmt.Errorf("only tenant and stable relation.%sId/relation.%s.thingId paths are available", side, side)
	}
	for _, part := range parts {
		if part.expression != "" {
			if err := check(part.expression); err != nil {
				return err
			}
		}
	}
	if !hasID {
		return fmt.Errorf("template must include relation.%sId", side)
	}
	return nil
}

func identityTemplate(s string) string {
	for _, path := range []string{"relation.from.thingId", "relation.to.thingId", "relation.fromId", "relation.toId"} {
		s = strings.ReplaceAll(s, path, "id")
	}
	return strings.ReplaceAll(s, "field:", "")
}

func (e *Engine) guardAggregateAttribute(id string, ent Entity, in Input) error {
	if ent.Delete {
		return nil
	}
	th, ok := in.(Thing)
	if !ok {
		return nil
	}
	for _, r := range e.rules {
		if r.Disabled || (r.Match.Tenant != "" && r.Match.Tenant != th.Tenant) {
			continue
		}
		ownerID := ""
		if th.Event == EventThingsV1Relations {
			v, _ := th.ResolvePath("relation.toId")
			ownerID, _ = v.(string)
		} else if th.Type == r.Match.ToType {
			ownerID = th.ID
		}
		if ownerID == "" {
			continue
		}
		for _, owner := range r.Entities {
			for _, p := range owner.Properties {
				if p.Aggregate == nil {
					continue
				}
				ownerInput := RelationInput(relations.Link{Tenant: th.Tenant, FromID: th.ID, ToID: ownerID}, false)
				targetID, err := renderTemplate(owner.ID, ownerInput, nil)
				if err != nil {
					return err
				}
				if targetID != id {
					continue
				}
				for _, prop := range ent.Properties {
					if prop.Target == p.Target && prop.Aggregate == nil {
						return fmt.Errorf("attribute %q is owned by relation aggregation", p.Target)
					}
				}
				if slices.Contains(ent.RemoveAttributes, p.Target) {
					return fmt.Errorf("attribute %q is owned by relation aggregation", p.Target)
				}
			}
		}
	}
	return nil
}

// One declaration owns each destination attribute. In particular a value or
// lifecycle rule must not replace a complete set with a scalar/constant.
func validateAggregateOwnership(rules []Rule) error {
	for i, r := range rules {
		if r.Disabled {
			continue
		}
		for _, ent := range r.Entities {
			for _, p := range ent.Properties {
				if p.Aggregate == nil {
					continue
				}
				for j, other := range rules {
					if other.Disabled || (r.Match.Tenant != "" && other.Match.Tenant != "" && r.Match.Tenant != other.Match.Tenant) {
						continue
					}
					for k, target := range other.Entities {
						if identityTemplate(target.ID) != identityTemplate(ent.ID) {
							continue
						}
						for _, prop := range target.Properties {
							if i == j && target.ID == ent.ID && prop.Target == p.Target {
								continue
							}
							if prop.Target == p.Target {
								return fmt.Errorf("rule %d entity %d: attribute %q is owned by relation aggregation in rule %d", j, k, p.Target, i)
							}
						}
						if slices.Contains(target.RemoveAttributes, p.Target) {
							return fmt.Errorf("rule %d: removeAttributes %q is owned by relation aggregation", j, p.Target)
						}
					}
				}
			}
		}
	}
	return nil
}
