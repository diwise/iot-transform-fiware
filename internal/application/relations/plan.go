// Package relations — planeringsavtal mot E (Våg 2).
//
// Denna fil definierar det exakta PlanFunc-avtalet som E:s motor och C2:s
// inkoppling planerar efter: stabila idempotensnycklar, deterministisk
// Snapshot→PendingWrite[]-mappning samt regeländringsåterbyggnad från
// ExportTenant-underlag utan I/O. Allt här är rent (ingen I/O, ingen
// context, ingen brokerkontakt) så att E kan återanvända planeringen och
// tester kan köras utan DB.
//
// Nyckelns form är "<regel>:<op>:<relation>:<from>[:<to>]":
//   - länkskrivning:  "<ruleID>:<ruleRev>:<op>:<relation>:<from>:<to>"
//   - sista-länk:     "<ruleID>:<ruleRev>:clear:<relation>:<from>"
//   - endpoint-delete:"<ruleID>:<ruleRev>:delete:<endpoint>"
//
// Utan regel (tom RuleRef) utelämnas regelprefixet men nyckeln förblir
// stabil: samma snapshot ger samma nycklar. Ny regelrevision ger nya
// nycklar och därmed nytt arbete vid Rebuild; samma regel ger samma nycklar
// och idempotent replay (ON CONFLICT DO NOTHING per tenant+nyckel).
//
// Tenant ingår aldrig i själva nyckelsträngen: den är implicit via DB:ns
// UNIQUE (tenant, idempotency_key). Samma nyckel i två tenants ger två
// isolerade rader — aldrig läckage.
package relations

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// RuleRef identifierar den regelversion som planerade en skrivning.
// Tom ID = oversionerad planering (fortfarande deterministisk, men
// regeländringar ger inga nya nycklar förrän regeln namnges).
type RuleRef struct {
	ID       string
	Revision int64
}

// escapeKeyPart gör nyckeldelar injektiva: "%" → "%25", ":" → "%3A".
// Utan escaping skulle from="a:b"/to="c" kollidera med from="a"/to="b:c".
func escapeKeyPart(s string) string {
	s = strings.ReplaceAll(s, "%", "%25")
	return strings.ReplaceAll(s, ":", "%3A")
}

// prefix bär regelidentiteten eller är tom vid oversionerad planering.
func (r RuleRef) prefix() string {
	if r.ID == "" {
		return ""
	}
	return escapeKeyPart(r.ID) + ":" + strconv.FormatInt(r.Revision, 10) + ":"
}

// IdempotencyKey bygger den stabila nyckeln för en enlänkskrivning
// (merge per Outgoing-länk, removeAttributes per Removed/Detached-länk).
// operation är ett Op*-verb, relation/from/to är källidentiteter — aldrig
// modellnamn. Tenant är implicit (unik per tenant i DB).
func IdempotencyKey(rule RuleRef, operation, relation, from, to string) string {
	return rule.prefix() + operation + ":" +
		escapeKeyPart(relation) + ":" + escapeKeyPart(from) + ":" + escapeKeyPart(to)
}

// IdempotencyKeyForClear bygger nyckeln för sista-länk-rensningen
// (IsLastLink: from har ingen länk kvar för relationen).
func IdempotencyKeyForClear(rule RuleRef, relation, from string) string {
	return rule.prefix() + "clear:" + escapeKeyPart(relation) + ":" + escapeKeyPart(from)
}

// IdempotencyKeyForEndpointDelete bygger nyckeln för lifecycle-delete av
// målet självt (EndpointDeleted, utöver per-länk-rensningarna).
func IdempotencyKeyForEndpointDelete(rule RuleRef, endpoint string) string {
	return rule.prefix() + "delete:" + escapeKeyPart(endpoint)
}

// ReferencePlanner är referensimplementationen av PlanFunc-avtalet som E:s
// motor ska vara paritetslik med. Generisk och modellfri: relationen bärs
// som källnamn i payload, aldrig som måltyp; EntityID är "entity:<from>" /
// "entity:<endpoint>" (E substituerar regelns verkliga URN-mall, F tolkar
// aldrig innehållet). Deterministisk: samma Snapshot + samma regel ger
// byte-identiska nycklar i sorterad ordning.
//
// Mappningen:
//   - en OpMerge-skrivning per Outgoing-länk,
//   - en OpRemoveAttributes-skrivning per Removed-länk (inkl. Detached, som
//     redan syns i Removed — dubbletter dedupliceras),
//   - en OpRemoveAttributes-clear vid IsLastLink (utan EndpointDeleted),
//   - en OpDelete vid EndpointDeleted (målet självt).
func ReferencePlanner(rule RuleRef) PlanFunc {
	return func(snap Snapshot) ([]PendingWrite, error) {
		seen := map[string]bool{}
		var out []PendingWrite
		emit := func(w PendingWrite) {
			if seen[w.IdempotencyKey] {
				return
			}
			seen[w.IdempotencyKey] = true
			out = append(out, w)
		}
		for _, l := range snap.Outgoing {
			payload, _ := json.Marshal(map[string]string{
				"relation": l.Relation, "from": l.FromID, "to": l.ToID,
			})
			emit(PendingWrite{
				IdempotencyKey: IdempotencyKey(rule, OpMerge, snap.Relation, l.FromID, l.ToID),
				Operation:      OpMerge,
				EntityID:       "entity:" + l.FromID,
				Payload:        payload,
				RuleID:         rule.ID,
				RuleRevision:   rule.Revision,
			})
		}
		for _, l := range snap.Removed {
			payload, _ := json.Marshal(map[string]string{
				"relation": l.Relation, "from": l.FromID, "removedTo": l.ToID,
			})
			emit(PendingWrite{
				IdempotencyKey: IdempotencyKey(rule, OpRemoveAttributes, snap.Relation, l.FromID, l.ToID),
				Operation:      OpRemoveAttributes,
				EntityID:       "entity:" + l.FromID,
				Payload:        payload,
				RuleID:         rule.ID,
				RuleRevision:   rule.Revision,
			})
		}
		// Detached som inte redan syns i Removed (defensivt: DeleteEndpoint
		// lägger samma länkar i båda, men rena snapshot-byggare kan skilja).
		for _, l := range snap.Detached {
			payload, _ := json.Marshal(map[string]string{
				"relation": l.Relation, "from": l.FromID, "removedTo": l.ToID,
			})
			emit(PendingWrite{
				IdempotencyKey: IdempotencyKey(rule, OpRemoveAttributes, snap.Relation, l.FromID, l.ToID),
				Operation:      OpRemoveAttributes,
				EntityID:       "entity:" + l.FromID,
				Payload:        payload,
				RuleID:         rule.ID,
				RuleRevision:   rule.Revision,
			})
		}
		if snap.IsLastLink && snap.EndpointDeleted == "" {
			payload, _ := json.Marshal(map[string]any{
				"relation": snap.Relation, "from": snap.FromID, "cleared": true,
			})
			emit(PendingWrite{
				IdempotencyKey: IdempotencyKeyForClear(rule, snap.Relation, snap.FromID),
				Operation:      OpRemoveAttributes,
				EntityID:       "entity:" + snap.FromID,
				Payload:        payload,
				RuleID:         rule.ID,
				RuleRevision:   rule.Revision,
			})
		}
		if snap.EndpointDeleted != "" {
			payload, _ := json.Marshal(map[string]string{
				"endpoint": snap.EndpointDeleted, "lifecycle": "deleted",
			})
			emit(PendingWrite{
				IdempotencyKey: IdempotencyKeyForEndpointDelete(rule, snap.EndpointDeleted),
				Operation:      OpDelete,
				EntityID:       "entity:" + snap.EndpointDeleted,
				Payload:        payload,
				RuleID:         rule.ID,
				RuleRevision:   rule.Revision,
			})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].IdempotencyKey < out[j].IdempotencyKey })
		return out, nil
	}
}

// SnapshotsForRebuild grupperar ett ExportTenant-underlag till
// per-from-snapshots med kompletta mängder — utan I/O. Indata är
// Store.ExportTenant-utdata (samtliga länkar för en tenant); relation ""
// tar alla relationsnamn. Grupper sorteras på (relation, from) och varje
// grupps Outgoing på ToID; ReverseNew byggs komplett per mål över hela
// underlaget (sorterat på FromID). Removed/Detached är tomma och IsLastLink
// är falsk (export innehåller endast icke-tomma from; sista-länk uppstår
// först vid live-remove). Tenantfrämmande länkar filtreras strikt.
func SnapshotsForRebuild(links []Link, tenant, relation string) []Snapshot {
	byFrom := map[LinkKey][]Link{}
	for _, l := range links {
		if l.Tenant != tenant {
			continue
		}
		if relation != "" && l.Relation != relation {
			continue
		}
		if l.FromID == "" || l.ToID == "" || l.Relation == "" {
			continue
		}
		k := LinkKey{Relation: l.Relation, FromID: l.FromID}
		byFrom[k] = append(byFrom[k], l)
	}
	// Kompletta omvända mängder över hela underlaget.
	byReverse := map[LinkKey][]Link{}
	for _, l := range links {
		if l.Tenant != tenant {
			continue
		}
		if relation != "" && l.Relation != relation {
			continue
		}
		if l.Relation == "" || l.ToID == "" {
			continue
		}
		k := LinkKey{Relation: l.Relation, ToID: l.ToID}
		byReverse[k] = append(byReverse[k], l)
	}
	keys := make([]LinkKey, 0, len(byFrom))
	for k := range byFrom {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Relation != keys[j].Relation {
			return keys[i].Relation < keys[j].Relation
		}
		return keys[i].FromID < keys[j].FromID
	})
	out := make([]Snapshot, 0, len(keys))
	for _, k := range keys {
		members := append([]Link(nil), byFrom[k]...)
		sort.Slice(members, func(i, j int) bool { return members[i].ToID < members[j].ToID })
		snap := Snapshot{
			Tenant:          tenant,
			Relation:        k.Relation,
			FromID:          k.FromID,
			Outgoing:        members,
			ReverseNew:      map[string][]Link{},
			ReversePrevious: map[string][]Link{},
			IsLastLink:      len(members) == 0,
		}
		seen := map[string]bool{}
		for _, l := range members {
			if seen[l.ToID] {
				continue
			}
			seen[l.ToID] = true
			rev := append([]Link(nil), byReverse[LinkKey{Relation: k.Relation, ToID: l.ToID}]...)
			sort.Slice(rev, func(i, j int) bool { return rev[i].FromID < rev[j].FromID })
			if rev == nil {
				rev = []Link{}
			}
			snap.ReverseNew[l.ToID] = rev
		}
		out = append(out, snap)
	}
	if out == nil {
		out = []Snapshot{}
	}
	return out
}

// PlanRebuildFromExport planerar om brokerarbete från ett ExportTenant-
// underlag utan nya mätningar och utan broker-I/O: regeluppsättningen bärs
// av planner-closuren (E bygger den från aktuell regelversion), projektionen
// av links. Varje per-from-snapshot planeras i deterministisk gruppordning;
// fel i en grupp avbryter utan delsvar (lagret persisterar atomärt).
// Returnerar de planerade skrivningarna i gruppordning (varje grupps
// skrivningar i plannerordning); E-paritet kräver att planeraren själv är
// deterministisk (se ReferencePlanner).
func PlanRebuildFromExport(links []Link, tenant, relation string, planner PlanFunc) ([]PendingWrite, error) {
	if tenant == "" {
		return nil, fmt.Errorf("%w: tenant is required", ErrInvalidChange)
	}
	if planner == nil {
		return nil, fmt.Errorf("%w: planner is required", ErrInvalidChange)
	}
	var out []PendingWrite
	for _, snap := range SnapshotsForRebuild(links, tenant, relation) {
		writes, err := planner(snap)
		if err != nil {
			return nil, err
		}
		out = append(out, writes...)
	}
	if out == nil {
		out = []PendingWrite{}
	}
	return out, nil
}
