// Package relations är paket F:s neutrala domän för källrelationer.
//
// Begrepp är tenant och källrelationer (namn, ändpunkter, revision) —
// aldrig standardmodellnamn. Allt här är rent (ingen I/O, ingen context)
// så att C2/E kan återanvända planeringen och tester kan köras utan DB.
//
// Revisionsemantik (PLAN1050): revision är enbart metadata för konflikt-
// och diagnostikändamål. Ingen recency- eller ordningsfiltrering införs:
// sena event appliceras och kan återställa äldre mängdstate (dokumenterad
// avgränsning). Samtidighetsskyddet bygger på länkgranulära rader +
// transaktionella DB-lås, inte på revisionsjämförelser.
package relations

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// HierarchicalRelation är det enda relationsnamn med hierarkisemantik
// (max-en-förälder + cykelskydd). Låst av PLAN1050; övriga namn styrs
// enbart av generella Spec-gränser.
const HierarchicalRelation = "partOf"

// Brokeroperationer för väntande arbete. Protokollverb, inte modellnamn;
// C2 mappar motorns skrivningar till dessa via PlanFunc.
const (
	OpCreate           = "create"
	OpMerge            = "merge"
	OpRemoveAttributes = "removeAttributes"
	OpDelete           = "delete"
)

// Domänfel. Lagret mappar dessa till sina ErrConflict/ErrInvalid:
// strukturfel är irreparabla (C2: Permanent), gränsfel är deterministiska
// konflikter mot spec/hierarki/tenant.
var (
	ErrInvalidChange  = errors.New("relations: invalid change")
	ErrTenantMismatch = errors.New("relations: tenant mismatch")
	ErrCardinality    = errors.New("relations: cardinality violated")
	ErrAllowedTarget  = errors.New("relations: target not allowed")
	ErrCycle          = errors.New("relations: hierarchy cycle denied")
)

// Change är en neutral relationsändring (set/remove/flytt).
// Move bär PreviousToID och appliceras atomärt (remove+set i samma
// revision). Revision lagras som metadata och filtrerar aldrig.
type Change struct {
	RelationID string
	Tenant     string
	Relation   string
	FromID     string
	ToID       string
	// PreviousToID är satt endast för flytt (atomärt remove+set).
	PreviousToID string
	Removed      bool
	Revision     int64
	ChangedAt    time.Time
	// Frysta ändpunktsbeskrivningar med källtypversioner.
	FromKind        string
	FromKindVersion string
	ToKind          string
	ToKindVersion   string
	// ToTenant är tom eller lika med Tenant; avvikelse är tenantbrott.
	ToTenant string
}

// IsMove rapporterar om ändringen är en flytt (atomärt remove+set).
func (c Change) IsMove() bool { return c.PreviousToID != "" }

// Link är en lagrad mängdmedlem: en länk i (tenant, relation, from) -> to.
//
// JSON-formen är det neutrala bootstrap-/återsynkformatet mot B
// (GET /things/export + GET /things/{id}/relations-underlag aggregerat
// till []Link): tenant/relation/fromId/toId/revision/changedAt samt frysta
// källtyper. B bär aldrig FIWARE-ID:n i källdata; F lagrar aldrig
// modellnamn — kind-fälten är källtypsträngar, inte måltyper.
type Link struct {
	Tenant          string    `json:"tenant"`
	Relation        string    `json:"relation"`
	FromID          string    `json:"fromId"`
	ToID            string    `json:"toId"`
	Revision        int64     `json:"revision"`
	ChangedAt       time.Time `json:"changedAt"`
	FromKind        string    `json:"fromKind,omitempty"`
	FromKindVersion string    `json:"fromKindVersion,omitempty"`
	ToKind          string    `json:"toKind,omitempty"`
	ToKindVersion   string    `json:"toKindVersion,omitempty"`
}

// Key identifierar länken utan metadata.
func (l Link) Key() LinkKey { return LinkKey{Relation: l.Relation, FromID: l.FromID, ToID: l.ToID} }

// LinkKey är länkens identitet inom en tenant.
type LinkKey struct {
	Relation string `json:"relation"`
	FromID   string `json:"fromId"`
	ToID     string `json:"toId"`
}

// Spec är kardinalitetsspecen för ett generellt relationsnamn.
// MaxLinks 0 = obegränsat. Tom AllowedTargets = alla mål tillåtna
// (okänd målkind är fail-open: kan inte bevisa brott).
type Spec struct {
	Name           string
	AllowedTargets []string
	MinLinks       int
	MaxLinks       int
	Hierarchical   bool
}

// PendingWrite är ett ogenomskinligt brokerarbete planerat från en komplett
// mängd. Payload tolkas aldrig av F (kan bära modellmappat innehåll från
// E/C); F persisterar den atomärt med projektionen och bevarar trace
// separat. IdempotencyKey är unik per tenant.
//
// PlanFunc-avtalet mot E:s motor (Våg 2):
//
//   - Planeraren är en ren funktion (ingen I/O, ingen context): Snapshot in,
//     PendingWrite[] ut. Lagret anropar den inom transaktionsgränsen och
//     rullar tillbaka både projektion och arbete vid fel. Broker-I/O sker
//     aldrig i planeraren — endast via pending-kön/DrainOnce.
//   - Stabila idempotensnycklar: samma Change + samma regel (RuleID +
//     RuleRevision) ger samma writes och samma nycklar; replay av samma
//     planering är ofarlig (ON CONFLICT DO NOTHING per tenant+nyckel).
//     Ny regelrevision ger nya nycklar och därmed nytt arbete vid Rebuild.
//     Använd IdempotencyKey/IdempotencyKeyForClear/IdempotencyKeyForEndpointDelete
//     (relation+entitet+attribut/operation) så att E och C2 planerar lika.
//   - removed:true-skrivningar: varje länk i Snapshot.Removed (remove/flytt)
//     och Snapshot.Detached (delete-policy) planeras som en
//     removeAttributes-rensning (OpRemoveAttributes), en per länk.
//   - Sista-länk-rensning: när Snapshot.IsLastLink är sann (from har ingen
//     länk kvar för relationen) planeras en clear-skrivning
//     (OpRemoveAttributes mot from-entiteten, se IdempotencyKeyForClear).
//   - Lifecycle-delete: när Snapshot.EndpointDeleted är satt (DeleteEndpoint)
//     planeras en delete-skrivning (OpDelete mot målet självt) utöver
//     removed:true-skrivningarna per lossad länk
//     (se IdempotencyKeyForEndpointDelete).
//   - Tenant-avgränsat: Snapshot.Tenant är den enda tenanten; planeraren får
//     aldrig blanda in länkar från annan tenant. Inga modellnamn i nycklar,
//     operationer eller F-ägd payload — modellkunskap bor i regeln/E:s
//     payload, aldrig i F:s planeringsbeslut.
//
// Exempel (generiskt, inga modellnamn — E:s verkliga planner serialiserar
// NGSI-LD i Payload men följer samma nyckel-/mängdregler):
//
//	planner := ReferencePlanner(RuleRef{ID: "rule-1", Revision: 9})
//	writes, err := planner(snap)
//	// writes innehåller en merge per Outgoing-länk, en removeAttributes per
//	// Removed/Detached-länk, ev. clear vid IsLastLink och ev. delete vid
//	// EndpointDeleted — alla med stabila nycklar per tenant.
type PendingWrite struct {
	IdempotencyKey string
	Operation      string
	EntityID       string
	Payload        []byte
	RuleID         string
	RuleRevision   int64
}

// Snapshot är det kompletta underlag PlanFunc får: utgående mängd för det
// berörda from plus omvända mängder för både nytt och tidigare mål.
// Planeraren ska bygga fullständiga skrivningar från dessa mängder —
// array: true på en enstaka länk ersätter aldrig mängdprojektion.
//
// Fältens roll i PlanFunc-avtalet:
//   - Tenant/Relation/FromID avgränsar gruppen (en from per anrop, utom
//     DeleteEndpoint där FromID är det borttagna målet).
//   - Outgoing är den fullständiga utgående mängden efter mutationen; en
//     merge-skrivning (OpMerge) planeras per länk.
//   - ReverseNew/ReversePrevious är kompletta omvända mängder per berört mål
//     (nya respektive tidigare). E kan använda dem för omvända relationer
//     utan extra I/O.
//   - Removed bär borttagna länkar (remove/flytt); Detached bär lossade länkar
//     vid delete-policy (samma länkar syns i Removed). En removeAttributes-
//     skrivning planeras per sådan länk.
//   - IsLastLink signalerar sista-länk-rensning (clear-skrivning).
//   - Detached + EndpointDeleted signalerar lifecycle-delete (delete av målet
//     självt utöver per-länk-rensningarna).
type Snapshot struct {
	Tenant   string
	Relation string
	FromID   string
	// Outgoing är den fullständiga utgående mängden efter mutationen.
	Outgoing []Link
	// ReverseNew är kompletta omvända mängder per nytt berört mål.
	ReverseNew map[string][]Link
	// ReversePrevious är kompletta omvända mängder per tidigare mål
	// (remove/flytt). Tom vid rent set.
	ReversePrevious map[string][]Link
	// Removed är länkar som togs bort i denna applicering.
	Removed []Link
	// IsLastLink är sann när from inte längre har någon länk kvar för
	// relationen (sista-länk-rensning ska planeras av C2/E).
	IsLastLink bool
	// Detached är länkar som lossades av delete-policy; EndpointDeleted
	// bär det borttagna målets ID (lifecycle deleted planeras av C2/E
	// som removed:true per länk + delete av målet självt).
	Detached        []Link
	EndpointDeleted string
}

// PlanFunc planerar brokerarbete från en komplett snapshot. Måste vara ren
// (ingen I/O): lagret anropar den inom transaktionsgränsen och rullar
// tillbaka både projektion och arbete vid fel.
//
// Kontraktet i korthet (fullt avtal under PendingWrite): deterministisk
// Snapshot→PendingWrite[] från kompletta mängder, stabila idempotensnycklar
// per (regel, operation, relation, entitet), removed:true per borttagen länk,
// clear vid IsLastLink, delete vid EndpointDeleted, strikt tenant-avgränsat,
// inga modellnamn, trace bärs separat av lagret (aldrig i Payload).
type PlanFunc func(Snapshot) ([]PendingWrite, error)

// ValidateChange kontrollerar struktur och tenantgräns utan I/O.
// Tom tenant/relation/from är strukturfel; remove/flytt kräver mål;
// ToTenant avvikande från Tenant är tenantbrott.
func ValidateChange(c Change) error {
	if c.Tenant == "" || c.Relation == "" || c.FromID == "" {
		return fmt.Errorf("%w: tenant, relation and from are required", ErrInvalidChange)
	}
	if c.ToTenant != "" && c.ToTenant != c.Tenant {
		return fmt.Errorf("%w: link tenant %q target tenant %q", ErrTenantMismatch, c.Tenant, c.ToTenant)
	}
	if c.IsMove() {
		if c.Removed {
			return fmt.Errorf("%w: move must not be marked removed", ErrInvalidChange)
		}
		if c.ToID == "" || c.PreviousToID == "" {
			return fmt.Errorf("%w: move requires both to and previousTo", ErrInvalidChange)
		}
		if c.ToID == c.PreviousToID {
			return fmt.Errorf("%w: move target equals previous target", ErrInvalidChange)
		}
		return nil
	}
	if c.ToID == "" {
		return fmt.Errorf("%w: to is required for set/remove", ErrInvalidChange)
	}
	return nil
}

// BuildRelationID bygger den stabila identiteten
// v1/<tenant>/<from>/relations/<type>/<to>/<revision>/<set|removed>.
// To ingår för unicitet vid flera mål samma revision (paritet med B:s
// buildRelationEvent): duplikat (tenant,relation,from,to) = idempotent
// upsert medan olika To ger distinkta ID:n.
func BuildRelationID(tenant, from, relType, to string, revision int64, removed bool) string {
	action := "set"
	if removed {
		action = "removed"
	}
	return "v1/" + tenant + "/" + from + "/relations/" + relType + "/" + to + "/" + strconv.FormatInt(revision, 10) + "/" + action
}

// ParsedRelationID är en tolkad stabil identitet.
type ParsedRelationID struct {
	Tenant   string
	FromID   string
	Relation string
	ToID     string
	Revision int64
	Removed  bool
}

// ParseRelationID tolkar strikt v1 med To
// (v1/<tenant>/<from>/relations/<type>/<to>/<revision>/<set|removed>, åtta
// segment) samt legacy v1 utan To (sju segment, ToID tom) som opakt läsbar
// bakåtkompatibilitet. Icke-v1-former (t.ex. "partof-<barn>") är inte
// v1 och nekas här; de förblir läsbara opakt hos B/C1 (ingen
// formatvalidering där) medan F:s skrivväg är strikt v1.
func ParseRelationID(id string) (ParsedRelationID, error) {
	var out ParsedRelationID
	parts := strings.Split(id, "/")
	if len(parts) == 7 && parts[0] == "v1" && parts[3] == "relations" {
		rev, err := strconv.ParseInt(parts[5], 10, 64)
		if err != nil {
			return out, fmt.Errorf("%w: relation id %q has invalid revision: %v", ErrInvalidChange, id, err)
		}
		var removed bool
		switch parts[6] {
		case "set":
		case "removed":
			removed = true
		default:
			return out, fmt.Errorf("%w: relation id %q action must be set or removed", ErrInvalidChange, id)
		}
		if parts[1] == "" || parts[2] == "" || parts[4] == "" {
			return out, fmt.Errorf("%w: relation id %q has empty tenant/from/type", ErrInvalidChange, id)
		}
		return ParsedRelationID{Tenant: parts[1], FromID: parts[2], Relation: parts[4], Revision: rev, Removed: removed}, nil
	}
	if len(parts) != 8 || parts[0] != "v1" || parts[3] != "relations" {
		return out, fmt.Errorf("%w: relation id %q must match v1/<tenant>/<from>/relations/<type>/<to>/<revision>/<set|removed>", ErrInvalidChange, id)
	}
	rev, err := strconv.ParseInt(parts[6], 10, 64)
	if err != nil {
		return out, fmt.Errorf("%w: relation id %q has invalid revision: %v", ErrInvalidChange, id, err)
	}
	var removed bool
	switch parts[7] {
	case "set":
	case "removed":
		removed = true
	default:
		return out, fmt.Errorf("%w: relation id %q action must be set or removed", ErrInvalidChange, id)
	}
	if parts[1] == "" || parts[2] == "" || parts[4] == "" || parts[5] == "" {
		return out, fmt.Errorf("%w: relation id %q has empty tenant/from/type/to", ErrInvalidChange, id)
	}
	return ParsedRelationID{Tenant: parts[1], FromID: parts[2], Relation: parts[4], ToID: parts[5], Revision: rev, Removed: removed}, nil
}

// CheckRelationID jämför stabil identitet mot kuvertets fält (diagnostik).
// Mismatch är strukturfel: identitet och tillstånd måste höra ihop. Tom
// identitet tolereras (äldre producent utan relationId); legacy v1 utan To
// validerar tenant/from/relation/revision/removed men hoppar To-kontrollen.
// Icke-v1-former nekas strikt här (F:s skrivväg); B/C1 förblir toleranta
// läsare (ingen formatvalidering, opakt sträng).
func CheckRelationID(c Change) error {
	if c.RelationID == "" {
		return nil
	}
	parsed, err := ParseRelationID(c.RelationID)
	if err != nil {
		return err
	}
	if parsed.Tenant != c.Tenant || parsed.FromID != c.FromID || parsed.Relation != c.Relation ||
		parsed.Revision != c.Revision || parsed.Removed != c.Removed {
		return fmt.Errorf("%w: relation id %q does not match envelope fields", ErrInvalidChange, c.RelationID)
	}
	if parsed.ToID != "" && parsed.ToID != c.ToID {
		return fmt.Errorf("%w: relation id %q does not match envelope to %q", ErrInvalidChange, c.RelationID, c.ToID)
	}
	return nil
}

// Outgoing returnerar den kompletta utgående mängden för
// (tenant, relation, from) sorterad på ToID.
func Outgoing(all []Link, tenant, relation, from string) []Link {
	var out []Link
	for _, l := range all {
		if l.Tenant == tenant && l.Relation == relation && l.FromID == from {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ToID < out[j].ToID })
	return out
}

// Reverse returnerar den kompletta omvända mängden för
// (tenant, relation, to) sorterad på FromID.
func Reverse(all []Link, tenant, relation, to string) []Link {
	var out []Link
	for _, l := range all {
		if l.Tenant == tenant && l.Relation == relation && l.ToID == to {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FromID < out[j].FromID })
	return out
}

// DetachForEndpoint samlar delete-policy-underlaget: samtliga länkar där
// målet är from- eller to-ändpunkt. Varje länk ska rapporteras som
// removed:true och målet självt som lifecycle deleted (planeras av C2/E).
func DetachForEndpoint(all []Link, tenant, endpoint string) []Link {
	var out []Link
	for _, l := range all {
		if l.Tenant == tenant && (l.FromID == endpoint || l.ToID == endpoint) {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FromID != out[j].FromID {
			return out[i].FromID < out[j].FromID
		}
		if out[i].Relation != out[j].Relation {
			return out[i].Relation < out[j].Relation
		}
		return out[i].ToID < out[j].ToID
	})
	return out
}

// CheckSet kontrollerar att en set-mutation får läggas till: måltyp mot
// allowedTargets, max-gräns (duplikat är idempotent och passerar alltid)
// samt hierarkisk max-en. current är antal befintliga utgående länkar,
// member anger om målet redan ingår i mängden.
func CheckSet(spec *Spec, relation, toKind string, current int, member bool) error {
	if member {
		return nil
	}
	hierarchical := relation == HierarchicalRelation || (spec != nil && spec.Hierarchical)
	if hierarchical && current >= 1 {
		return fmt.Errorf("%w: hierarchical relation %q already has a target (use move)", ErrCardinality, relation)
	}
	if spec == nil {
		return nil
	}
	if len(spec.AllowedTargets) > 0 && toKind != "" && !contains(spec.AllowedTargets, toKind) {
		return fmt.Errorf("%w: relation %q target kind %q not in allowed targets", ErrAllowedTarget, relation, toKind)
	}
	if spec.MaxLinks > 0 && current >= spec.MaxLinks {
		return fmt.Errorf("%w: relation %q max %d reached", ErrCardinality, relation, spec.MaxLinks)
	}
	return nil
}

// CheckRemove kontrollerar min-gräns vid enskild bortkoppling.
// Delete-policy (DetachForEndpoint) kringgår min: ett borttaget mål kan
// inte behålla länkar.
func CheckRemove(spec *Spec, relation string, current int) error {
	if spec == nil {
		return nil
	}
	if spec.Name != "" && spec.Name != relation {
		return nil
	}
	if spec.MinLinks > 0 && current-1 < spec.MinLinks {
		return fmt.Errorf("%w: relation %q min %d would be violated", ErrCardinality, relation, spec.MinLinks)
	}
	return nil
}

// CreatesCycle prövar hierarkicykel i minnet: sann om newParent är child
// själv eller nås som ättling till child via parents-kedjan
// (parents: child -> direkt förälder). Generella namn cykelkontrolleras
// aldrig här — endast hierarkiska relationer.
func CreatesCycle(parents map[string]string, child, newParent string) bool {
	if child == "" || newParent == "" || child == newParent {
		return child == newParent && child != ""
	}
	for cur := newParent; cur != ""; {
		if cur == child {
			return true
		}
		next, ok := parents[cur]
		if !ok {
			return false
		}
		cur = next
	}
	return false
}

// LinkFromChange bygger länken för en set-mutation (metadata, ingen filtrering).
func LinkFromChange(c Change) Link {
	return Link{
		Tenant: c.Tenant, Relation: c.Relation, FromID: c.FromID, ToID: c.ToID,
		Revision: c.Revision, ChangedAt: c.ChangedAt,
		FromKind: c.FromKind, FromKindVersion: c.FromKindVersion,
		ToKind: c.ToKind, ToKindVersion: c.ToKindVersion,
	}
}

func contains(list []string, s string) bool {
	return slices.Contains(list, s)
}
