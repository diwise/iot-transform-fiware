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

## Källvyer

Producenternas Go-kontrakt är facit: `iot-core/pkg/messaging/events` för
`message.accepted` och `iot-things-v2/pkg/types` för things v1. Inga modellnamn
eller mål-URN:er härleds från dessa kontrakt.

| Källa | Identitet och typ | Tid och värden |
| --- | --- | --- |
| `message.accepted` | deviceID, tenant, SenML-objektets URN, fullständigt resursnamn och kanal; sensorType är HTTP-berikning | meddelandets timestamp separat från objektheaderns bastid och varje resurs observationstid; numeriskt/booleskt/textvärde och källenhet |
| `things.v1.values` | schema, eventId, tenant, thingId, thingType, thingTypeVersion, configRevision och thing-descriptor | publishedAt är publiceringstid; konsumentens bastid är max(values[].observedAt); varje värde bär propertyId, value, quality, unit, source, messageId, observedAt, receivedAt, computedAt och publishedAt |
| `things.v1.relations` | relationId, type, fromId, toId, tenant, revision, removed, fromThingType och toThingType samt båda descriptors | changedAt är relationsändringstid; removed anger bortkoppling från senast kända mål, ingen värdeobservation |
| `things.v1.lifecycle` | schema, eventId, tenant, thingId, thingType, thingTypeVersion, configRevision, kind och descriptor | changedAt är konfigurationsändringstid; kind är created/updated/deleted, ingen värdeobservation |

En descriptor innehåller thingId, rå name/alternativeName/subType/category,
location (GeoJSON Point eller Polygon i WGS84 lon/lat), metadata samt
variantId/variantVersion. Description och tags finns i metadata, inte som
egna toppnivåfält. Type finns i eventkuvertet och får inte ersättas av subType.
Descriptor beskriver eventets konfigurationsrevision; den är inte ett uppslag
mot dagens sak. Saknade optional-fält skapar inte information.

Skrivbara things-värden exponeras under `values.<propertyId>` endast när
quality är `ok`/`uncertain` och value inte är null. `invalid`, okänd och tom
kvalitet får aldrig ersätta aktuellt brokervärde. Proveniens och råa
observationer måste vara åtkomliga separat från denna skrivbara vy; en regel
får inte kringgå kvalitetsgrinden. Source är producentens identitetssträng,
inte en FIWARE-relation. Subsekundprecision ska bevaras.

Measurement-partitionering sker per observation före regelkörning, även i
preview. Env, koordinater, resurser och bastid ska komma från samma partition.
Kanal är en del av resursidentiteten och **inte port**: accepted-kontraktet
levererar ingen port för matchning. En portregel ska därför avvisas.

Nuvarande adapterluckor att rätta i etapp 4–5: resurser tappar fullständigt
namn/enhet, values tappar delar av proveniensen och kuvertmetadata, Point
saknar rå geometri, lifecycle tappar descriptorfält och relationsadaptern
innehåller en modellberoende URN. Dessa är inte källkontraktets begränsningar.
