package relations

import "sort"

// SnapshotsForReplacement includes both current and disappeared groups. Unlike
// SnapshotsForRebuild it can clear the last old inverse member after a complete
// authoritative import. Only same-tenant links participate. No watermark or
// source revision comparison: the import caller must quiesce source changes.
func SnapshotsForReplacement(before, after []Link, tenant string) []Snapshot {
	type group struct{ relation, from string }
	groups := map[group]bool{}
	keep := map[LinkKey]bool{}
	for _, l := range after {
		if l.Tenant == tenant {
			keep[l.Key()] = true
			groups[group{l.Relation, l.FromID}] = true
		}
	}
	for _, l := range before {
		if l.Tenant == tenant {
			groups[group{l.Relation, l.FromID}] = true
		}
	}
	keys := make([]group, 0, len(groups))
	for g := range groups {
		keys = append(keys, g)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].relation != keys[j].relation {
			return keys[i].relation < keys[j].relation
		}
		return keys[i].from < keys[j].from
	})
	out := make([]Snapshot, 0, len(keys))
	for _, g := range keys {
		current := Outgoing(after, tenant, g.relation, g.from)
		snap := Snapshot{Tenant: tenant, Relation: g.relation, FromID: g.from, Outgoing: current, IsLastLink: len(current) == 0,
			ReverseNew: map[string][]Link{}, ReversePrevious: map[string][]Link{}}
		for _, l := range current {
			snap.ReverseNew[l.ToID] = Reverse(after, tenant, g.relation, l.ToID)
		}
		for _, l := range Outgoing(before, tenant, g.relation, g.from) {
			if !keep[l.Key()] {
				snap.Removed = append(snap.Removed, l)
				snap.ReversePrevious[l.ToID] = Reverse(after, tenant, g.relation, l.ToID)
			}
		}
		out = append(out, snap)
	}
	return out
}
