package relations

import (
	"encoding/json"
	"fmt"
)

// LinksFromThingsExport converts a COMPLETE, quiesced GET /api/v1/things/export
// ObjectSpec[] snapshot (iot-things-v2's PLAN1050 contract). Target types come
// from actual same-tenant objects, never from a caller's targetTemplateId.
// ObjectSpec does not carry a config revision or change time: these remain
// unknown (0/zero), not invented ordering watermarks. No model knowledge.
func LinksFromThingsExport(raw []byte, tenant string) ([]Link, error) {
	var objects []struct {
		ThingID         string `json:"thingId"`
		Tenant          string `json:"tenant"`
		TemplateID      string `json:"templateId"`
		TemplateVersion string `json:"templateVersion"`
		Relations       []struct {
			Name             string `json:"name"`
			TargetThingID    string `json:"targetThingId"`
			TargetTemplateID string `json:"targetTemplateId"`
		} `json:"relations"`
	}
	if tenant == "" {
		return nil, fmt.Errorf("%w: tenant required", ErrInvalidChange)
	}
	if err := json.Unmarshal(raw, &objects); err != nil {
		return nil, fmt.Errorf("%w: things export: %v", ErrInvalidChange, err)
	}
	index := map[string]int{}
	for i, o := range objects {
		if o.Tenant != tenant {
			continue
		}
		if o.ThingID == "" || o.TemplateID == "" {
			return nil, fmt.Errorf("%w: object lacks identity or template", ErrInvalidChange)
		}
		if _, ok := index[o.ThingID]; ok {
			return nil, fmt.Errorf("%w: duplicate object in export", ErrInvalidChange)
		}
		index[o.ThingID] = i
	}
	links := []Link{}
	seen := map[LinkKey]bool{}
	for _, o := range objects {
		if o.Tenant != tenant {
			continue
		}
		for _, rel := range o.Relations {
			i, ok := index[rel.TargetThingID]
			if !ok || rel.Name == "" {
				return nil, fmt.Errorf("%w: incomplete export or broken target for %q/%q", ErrInvalidChange, o.ThingID, rel.Name)
			}
			to := objects[i]
			if rel.TargetTemplateID != "" && rel.TargetTemplateID != to.TemplateID {
				return nil, fmt.Errorf("%w: declared target template conflicts with actual target", ErrInvalidChange)
			}
			l := Link{Tenant: tenant, Relation: rel.Name, FromID: o.ThingID, ToID: to.ThingID, FromKind: o.TemplateID, FromKindVersion: o.TemplateVersion, ToKind: to.TemplateID, ToKindVersion: to.TemplateVersion}
			if !seen[l.Key()] {
				links = append(links, l)
				seen[l.Key()] = true
			}
		}
	}
	// Use the existing deterministic codec to return canonical ordering.
	ordered, err := LinksToJSON(links)
	if err != nil {
		return nil, err
	}
	return LinksFromJSON(ordered)
}
