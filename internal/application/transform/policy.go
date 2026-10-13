package transform

import "errors"

// Sentinel-fel som motorn (Fas 2) använder för att styra flödet. De är inte
// valideringsfel utan beskriver körningens utfall.

// ErrNoMatchingRule returneras när ingen regel matchar en källa. Källan
// ignoreras då (ingen entitet skrivs) och loggas på debug-nivå — det är inte
// ett fel, utan motsvarar dagens beteende att okända typer hoppas över.
var ErrNoMatchingRule = errors.New("no matching rule")

// ErrRequiredSourceMissing returneras när en property med required: true
// saknar sin källa. Hela entiteten ska då inte skrivas (till skillnad från
// partiella entiteter), eftersom obligatoriska attribut annars blir
// ofullständiga.
var ErrRequiredSourceMissing = errors.New("required source is missing")

// Beteende (Fas 1, punkt 12):
//
//   - Ingen regel matchar: källan ignoreras, ingen entitet skrivs, ingen retry.
//   - required: true och källan saknas: entiteten skrivs inte (hela entiteten
//     hoppas över).
//   - required: false (default) och källan saknas: propertyn utelämnas medan
//     övriga properties skrivs (partiell entitet). När-satsen `when: present`
//     är default och beskriver exakt detta.
//   - Structured med required delproperty som saknas: hela entiteten hoppas
//     över.
//
// PLAN1050 D (generell giltighet, additiv v1):
//
//   - Skapande: create:true kräver validerad snapshot som full create
//     (giltig PATCH ≠ giltig create); default är merge-or-create med delta
//     för deluppdateringar. Härledningsaktivering är explicit via närvarande
//     ingångar (when: present); aggregat läser endast samma event.
//   - Aggregat: endast derive[sum|min|max|anyTrue|allTrue|coalesce|
//     firstPresent] inom samma event. sum/min/max kräver finita tal och
//     enhetskonfliktkontroll; anyTrue/allTrue är dimensionslösa (C62).
//   - Listor/Structured/repeat: endast via array:true (Relationship),
//     [n]-index och "..."-citerade nycklar. Fria metadata/repeat ger aldrig
//     falsk fullständig garanti: saknad dynamisk/valfri källa är skip,
//     inte ett statiskt fel.
//   - Version/variant: exakta matchningar med tom wildcard; tom
//     thingTypeVersion matchar alla men kräver thingType. configRevision är
//     aldrig matchfilter (proveniens/diagnostik, inte selektion).
//   - Extra mallunderlag är explicit indata till kompatibilitet, aldrig ett
//     dolt obligatoriskt kataloguppslag per mätning. Ingen recency-/
//     ordningsfiltrering införs; sena event är en dokumenterad avgränsning.
//   - NGSI-LD-profilen validerar faktisk serialiserad output
//     (BuildEntity/ValidateEntityWrites). Full context-expansion,
//     versionerad SDM-schemavalidering och brokerns faktiska existens
//     verifieras INTE.
