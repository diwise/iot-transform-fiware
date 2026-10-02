package transform

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
