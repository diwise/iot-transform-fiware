# Transformationskontrakt

## Protokollprofil

Referensversion är [ETSI GS CIM 009 V1.8.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.08.01_60/gs_CIM009v010801p.pdf).
Tjänsten använder normaliserad, kompakt NGSI-LD JSON, inte keyValues eller
expanded JSON-LD. Entiteter har ett absolut URI-ID och en regelvald typ.
Property använder `type: Property` och `value`; DateTime använder JSON-LD:s
typade DateTime-värde. GeoProperty använder GeoJSON och Relationship använder
`object`. LanguageProperty använder `languageMap`. Tal måste vara finita,
relationer absoluta URI:er och tider RFC 3339. Enhetskoder väljs av regeln.

Den avsedda grundvalideringen kontrollerar struktur, värdetyper, URI:er,
tider, geometriform och konflikter med strukturfält före brokerskrivning.
Detta är **inte fullständig ETSI-konformitetsvalidering**: JSON-LD-context
hämtas/expanderas inte och brokerkapabiliteter förhandlas inte. Typ- och
attributtermer tolkas genom den effektiva contexten. Fullständig Smart Data
Models-schemavalidering ingår inte; `required` styr mappning, inte modellkonformitet.

Profilen omfattar en attributinstans per namn, skalära Property-värden,
tal-/textlistor, Structured-objekt, LanguageProperty, GeoProperty
(Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon)
och skalära/listformade Relationship-referenser. Regelns context är en URL;
om den saknas används brokerkonfigurationens fallback. Inline-context,
context-arrayer, datasetId, VocabProperty, ListProperty/ListRelationship,
GeometryCollection, temporal lagring och avancerade attributinstanser ingår
inte i denna profil.

Operationer är create-if-absent, merge-or-create, namngiven attributrensning
och delete. Brokersvar och faktisk serialisering måste verifieras separat
mot denna profil. Denna specifikation är målkontraktet för checklistan i
`TRANSFORM.md`; en beskriven funktion är inte i sig bevis på att den redan är
implementerad. Endast verifierade checklistpunkter markeras klara.
