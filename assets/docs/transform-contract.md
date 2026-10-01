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
Port är sensorns fysiska anslutning/utgång, kodad i resursidentiteten:
`{device_id}/{object}/{resource}` utan port och
`{device_id}/{port}/{object}/{resource}` med port. Exempel:
`123-456-789/3303/5700` respektive `123-456-789/2/3303/5700`.
Port härleds från observationens motsvarande header (`.../object/0`) efter
SenML-normalisering och partitionering, inte från radio-/transportport eller
fältet Channel. Ingen port är tom sträng; explicit port `0` är strängen `"0"`.
Djupare kanalprefix är inte detta format och får ingen gissad fysisk port.
Kompakt `bn` + `n` och upplösta fullständiga namn tolkas likadant.

Nuvarande adapterluckor att rätta i etapp 4–5: resurser tappar fullständigt
namn/enhet och values tappar delar av proveniensen och kuvertmetadata.
Gemensamma descriptors exponerar nu rå plats, metadata och variantinformation
för values/lifecycle samt båda relationssidorna. Relationsadaptern bygger
inga modellberoende mål-URN:er. Resterande luckor är inte källkontraktets begränsningar.

## Uttryck och frånvaro

Standardmallar använder `{{id}}`: rått `thingId` för things och rått `deviceID`
för measurements. En befintlig GUID/UUID bevaras; ingen ny identitet skapas.
Namn och alternativnamn är separata attribut, utan fallback till ID.
`nameOrID` har tagits bort. Ingen URI-normalisering sker implicit.
`type` är rå thingType; `typeName` är
den generella genvägen subType om satt, annars type. `uuid` är sista segmentet
efter kolon eller slash och garanterar inte UUID-format eller unikhet.
`id`, `name`, `alternativeName` och `subType` bevaras råa. Measurements har
id, deviceID, sensorType, object, port, env, tenant och timestamp; saker har id, type,
typeName, subType, name, alternativeName, uuid, tenant och timestamp.
Detta är standardgenvägar, inte en sluten lista. `{{variabel}}` kan slå upp
vilket exponerat källfält som helst via samma parser som `{{field:variabel}}`,
inklusive nästlade vägar, citerade nycklar och index. Syntax och funktioner
valideras i förväg; dynamiska fälts närvaro kan endast avgöras mot indata.
Källspecifika värdekällor/matchningsfält kontrolleras fortfarande per kind.

`safeURI(expr)` tar exakt ett uttryck och returnerar dess trimmade text
procentkodad med Go `url.PathEscape`. Det är ett segmenthjälpmedel, inte
validering av en komplett URI. UTF-8 kodas bytevis; exempel: `A B/å` blir
`A%20B%2F%C3%A5`. Funktionen är inte idempotent: `%` kodas som `%25`.
Regeln väljer prefix och var segmentkodning ska användas.

`firstPresent(expr, ...)` kräver minst ett argument, utvärderar från vänster
till höger och returnerar första närvarande, icke-tomma text. Saknat fält
eller tom sträng tillåter nästa argument; typ-/syntaxfel är fel. Noll och
false är närvarande, whitespace är rå text. Nästling stöds, liksom
`field:values."temperature.average".value` och nollbaserade `[n]`-index.
Ingen JavaScript, godtycklig Go-template eller implicit modellfunktion stöds.

En saknad källa är frånvarande/null, inte ett typfel. Tom text, tom lista och
tom mapp är saknade skrivvärden; noll och false är skrivbara. Valfri property
utelämnas, `required` undertrycker hela entiteten. Saknad mallkälla ska följa
samma policy för property-värden; i entitets-ID/typ är frånvaro ett fel.
`when: present` uttrycker närvarokravet även för aggregat. Aggregat använder
närvarande ingångar; felaktiga typer är fel. Sum/min/max utan ingång ger
saknad källa, anyTrue/allTrue utan ingång ger false om inte present krävs.
Derive coalesce/firstPresent väljer första icke-tomma närvarande värde.
Alltså finns ingen implicit fallback till en Smart Data Model.

Objekt/listor i malluttryck renderas som JSON; tal/booleska värden som
JSON-skalärer och strängar som rå text. Identitets-/metadatagenvägar har
företräde framför dynamiska fält. Namn får användas uttryckligen i en egen
ID-mall, men dess författare ansvarar då för kollisioner, namnbyten och
tidigare målentiteter. Ingen automatisk rensning eller målidentitetshistorik
införs; samma ansvar gäller vid byte av ID-mall eller modellprefix.

## Regelval

Exakt en regel vinner per partitionerad measurement-observation eller things-
händelse. Alla matchningsvillkor är AND-villkor. Högsta explicita heltals-
`priority` vinner (default 0), därefter högsta specificitet och slutligen
första regeln i snapshotens stabila ordning. DB-snapshots ordnas på position
och därefter regel-ID (seed i filordning, API i skapandeordning);
offline YAML använder fil-/regelordning. Prioritet ändrar aldrig tenantgrinden.

Specificitet: device/subType ger 4 vardera, sensorType/type ger 2 vardera,
object/resource/env/port/tenant och relationens måltyp ger 1 vardera. Event,
relationRemoved och lifecycle är hårda filter. SensorType jämförs
case-insensitivt, övriga värden exakt. `match.port` är ett measurement-filter:
tomt/osatt matchar alla portar och portlösa observationer, `"0"`, `"1"` etc.
matchar den exakta portsträngen. En portspecifik regel får ett extra
specificitetspoäng och vinner över en i övrigt likadan generell regel.
Lifecycle-regler kräver explicit created/updated/deleted, inget wildcard.
Things kräver exakt en dokumenterad eventkälla. Relationens målsida måste
kunna avgränsas separat från from-sidan.

En regel får producera flera entiteter i deklarerad ordning. Namngivna
entitetsreferenser måste avse ett tidigare deklarerat ID; själv-/framåt-
referenser i ID och cykler avvisas. En entitets övriga uttryck kan använda
dess eget redan renderade ID. Alla uttrycksplatser har samma referensregler.
Matchande regler kombineras inte. Ingen match ger ett förklarbart skip utan
broker-/tokenkontakt. En match utan skrivbara properties ger inga skrivningar.
Överlapp/skuggning ska vara diagnostik, inte falska konflikter mellan tenants.

## Utfall

| Situation | Produktion | Administration/preview |
| --- | --- | --- |
| Ingen regel | skip, ingen brokerkontakt | matched=false med orsak |
| Saknad valfri källa | utelämnat attribut | precis käll-/skiporsak |
| Saknad required-källa | undertryckt entitet | matched=true, required-källa angiven |
| Felaktig JSON/identitet/typ | irreparabelt fel, ingen skrivning | klientfel 400 |
| Ogiltig regel | kandidaten publiceras inte; fungerande snapshot behålls | klientfel 400 med regel/entitet/attribut/uttryck |
| Ogiltig renderad NGSI-LD | irreparabelt fel före brokerkontakt | klientfel 400 med mål/attribut |
| DB-/HTTP-/token-/brokerberoendefel | klassificerbart tillfälligt fel, cancellation bevaras | server-/beroendefel, inte maskerat som not-found |
| Partiellt brokersvar | misslyckad leverans med delutfall | rapportera lyckade/misslyckade operationer |
| Revisionskonflikt | ingen mutation | HTTP 409 |
| Bristande skrivägarskap | ingen mutation | HTTP 403 |

Ett fel är inte ett skip. Delvis lyckade flerskrivningar är inte full framgång.
Transportens faktiska retry-/dead-letter-beteende ska verifieras i den låsta
messaging-versionen innan handlers ändras; tabellen bestämmer klassificering,
inte obevisade leveransgarantier. Replay av enskilda operationer kan vara
idempotent utan att sena händelser är ordningsoberoende korrekta.

## Verifieringsstatus 2026-09-30

Verifierade checklistpunkter: etapp 1 punkt 1–5, etapp 2 samtliga punkter,
etapp 3 punkt 1–6. Regressionstester för kommande rättningar (1.6) är ett
fortlöpande arbete. Nästa implementationspunkt är 3.7. Etapp 4–9 och de
separata designbesluten återstår; målprofilen ovan ska därför inte tolkas som
att hela transformationen är färdig eller produktionsacceptansen utförd.

Kört i `iot-transform-fiware` efter ändringarna:

```sh
PG_TEST_PORT=5432 go test ./... -count=1
go vet ./...
PG_TEST_PORT=5432 go test -race ./internal/infrastructure/rules ./internal/presentation/api ./internal/application/transform ./cmd/iot-transform-fiware -count=1
```

Alla dessa kontroller passerade. DB-testerna använde Compose-PostgreSQL på
localhost:5432 med separata databaser `diwise_test_rules` och
`diwise_test_api`. Nya DB-tester för MVCC-snapshot, rollback, cancellation,
revisionskonkurrens och omstart kördes även separat med `-v -count=1` och
rapporterade PASS, inte SKIP. Ändrade Go-filer har formaterats med gofmt.
Ingen full Compose-end-to-end-acceptans eller brokerverifiering enligt etapp
7/9 har utförts för denna ändringsserie.

## Identitetsbeslut och verifiering 2026-10-01

Standard-ID är nu `{{id}}`, och namn/alternativnamn mappas till attribut.
Den nya policyn är verifierad för alla nio medföljande things-modeller med
neutrala GUID:er: namn-, alternativnamns- och undertypsändringar behåller
samma mål-ID i values och lifecycle/delete. Relationsreferenser och rensning
använder samma käll-ID:n. Generella fältmallar, råa descriptors och faktisk
Relationship-listserialisering är verifierade; `nameOrID` tillhandahålls inte.

Efter identitetsändringen passerade `PG_TEST_PORT=5432 go test ./... -count=1`
och `go vet ./...` i denna modul. Race-tester passerade med `-count=1` för
transform, things, config, rules, api och messaging/things. DB-/seedtester
kördes mot de separata testdatabaserna ovan. Alla fyra YAML-exempel i
`iot-things-v2/docs/CONSUMER-BINDING.md` avkodades strikt och validerades med
motorns faktiska validator genom en tillfällig verifieringstestfil som sedan
togs bort.

`diwise-web` använder `{{id}}` som standard och bevarar `array` i klient,
formulär och JSON. `go test ./...` och `go vet ./...` passerade där och i
`iot-things-v2`. Webbens ändrade `.templ` genererades med templ v0.3.1020;
`templ generate -f internal/presentation/web/components/features/rules/rule_form.templ -check`
passerade. Övriga omarkerade TRANSFORM-punkter återstår.

## Portkontrakt och verifiering 2026-10-01

TRANSFORM 4.7 följer nu det fastställda resursformatet ovan. Producentreferens:
`iot-agent/internal/application/decoders/elsys/decoders.elsys.go` bygger
enhets-ID med `/0`, `/1` och `/2`; `pkg/lwm2m/lwm2m.go` lägger därefter till
objekt och resurs. Den låsta SenML-versionen
`v0.0.0-20260909175035-ac3958b9698e` bevarar dessa observationsprefix.

Regressionstester visade före rättningen att port alltid var tom och att en
generell regel skuggade portregeln. Efter rättningen verifieras portlös och
explicit port 0/1/2 med samma device/object/resource, olika värden och olika
mättider. Produktionshandlern skriver fyra rätt avgränsade fragment mot en
kontrollerad brokerklient; offline-preview väljer endast efterfrågad port.
`PG_TEST_PORT=5432 go test ./... -count=1` och `go vet ./...` passerade.
Race-tester med `-count=1` passerade för measurements, transform, api och
messaging/measurements.
