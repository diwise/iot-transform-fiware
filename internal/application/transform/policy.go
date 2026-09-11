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
