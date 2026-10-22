# Transformationskontrakt

## Protokollprofil

Referensversion är [ETSI GS CIM 009 V1.8.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.08.01_60/gs_CIM009v010801p.pdf).
Tjänsten använder normaliserad, kompakt NGSI-LD JSON, inte keyValues eller
expanded JSON-LD. Entiteter har ett absolut URI-ID och en regelvald typ.
Property använder `type: Property` och `value`; DateTime använder JSON-LD:s
typade DateTime-värde. GeoProperty använder GeoJSON och Relationship använder
`object`. LanguageProperty använder `languageMap`. Tal måste vara finita,
relationer absoluta URI:er och tider RFC 3339. Enhetskoder väljs av regeln.

Grundvalideringen kontrollerar struktur, värdetyper, URI:er,
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

### Implementerad källvy (etapp 4)

Values och lifecycle exponerar kuvertmetadata under `event`: schema, eventId,
tenant, thingId, thingType, thingTypeVersion, configRevision samt publishedAt
respektive kind/changedAt. Relationshändelser exponerar sina kontraktsfält
under både `event` och `relation`, inklusive tenant och båda råa typerna.
Inbäddade relationsändringar i ett values-event bevaras under `relations[]`;
de väljer inte automatiskt modell eller brokeroperation.

`thing` och `relation.from`/`relation.to` innehåller descriptorfält, rå
GeoJSON-location, metadata och variantId/variantVersion. Envelope-ID och typ
styr motorns primära identitet; ett explicit descriptor-ID bevaras också i
descriptorvyn. Rå typ ändras aldrig till subType; `typeName` är en separat
genväg. För values/lifecycle och relationens from-sida finns namn, kategori,
metadata och variantfält även i roten. `geometry` bevarar hela ursprungliga
GeoJSON-objektet för Point/Polygon, inklusive extra koordinater. `location`
är endast Point-genvägen `{latitude,longitude}`; originalet finns alltid
under descriptor.location och geometry. Saknad plats/metadata skapas inte.

`values.<propertyId>` innehåller skrivbart värde och hela observationens
proveniens: schema, eventId, thingId, tenant, propertyId, quality, unit,
source, messageId, configRevision och observedAt/receivedAt/computedAt/publishedAt.
Citerade nycklar används när propertyId innehåller punkt, exempelvis
`values."reading.average".messageId`. Samtliga observationers metadata finns
även i `observations[]`, i trådordning och med `valuePresent` som anger om
originalet hade ett värde. **Denna vy innehåller inga värden**, inte heller
för ok/uncertain: numeriska skrivvärden kan bara hämtas ur den kvalitetsfiltrerade
values-vyn. Varken event eller någon rå payload-kopia exponerar ofiltrerade
värden. Kvalitet/proveniens kan därmed användas utan att invalid/null ersätter
aktuella mätvärden. Konsumentens bastid förblir max av samtliga observedAt,
precis som producentens EventTimestamp; publicerings- och relationstid blir
inte mättid. Alla exponerade källtider bevarar RFC3339Nano-precision och
saknade tider utelämnas.

Measurements exponerar `records[]` i partitionsordning, med fullName (det
upplösta SenML-namnet), name (kortnamn), object (URN), channel (rått
mellanprefix), unit, nativt value och observedAt. Noll och false bevaras,
och upprepade resurser bevaras som separata listposter. Exempel:
`{{records[0].fullName}}`, `{{records[0].unit}}` och
`{{records[0].observedAt}}`; metadata finns också som poster efter resurserna.
`message.timestamp` och `header.observedAt` ger separat meddelande-/bastid,
och `source` är observationens effektiva källmetadata. Korta resursgenvägar
finns kvar; ett fullständigt resursnamn kan också användas som malluppslag.
Propertyns befintliga resource/name-selectors är fortsatt kortnamnsbaserade.

Partitioneringen använder den låsta senml-parserns effektiva observationsmetadata:
lokal env/source/lat/lon överlagrar paketets defaults, även vid upprepade
identiska prefix. Varje partition innehåller sin egen header/resurser och
normaliserade metadata, inte andra observationers metadataposter. Den
normaliserade metadatan är en effektiv vy, inte en kopia av ursprunglig
bn/n-encoding eller metadata-tidsstämplar. Detta används gemensamt av
produktion och preview. SenML:s tidsupplösning följer den låsta parsern;
tillgänglig subsekundprecision bevaras i den exponerade vyn.

Etapp 5 inför den konsekventa tids-/enhetspolicyn nedan. Källvyer bevarar
RFC3339Nano; broker-utdata (DateTime-värden och observedAt) trunkeras till
millisekunder, se tidsmetadata. Modellernas val av tidskällor
granskas separat i etapp 8.
Adaptrarna innehåller inga modellberoende mål-URN:er eller modellval.

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
utelämnas, `required` undertrycker hela entiteten. Saknad mallkälla följer
samma policy för property-värden; i entitets-ID/typ är frånvaro ett fel.
`when: present` uttrycker närvarokravet även för aggregat. Aggregat använder
närvarande ingångar; felaktiga typer är fel. Sum/min/max utan ingång ger
saknad källa. Även anyTrue/allTrue utan ingång ger saknad källa och ingen
skrivning; noll/false får väljas som uttrycklig fallback i regeln.
Derive coalesce/firstPresent väljer första icke-tomma närvarande värde.
Alltså finns ingen implicit fallback till en Smart Data Model.

Närvaro kontrolleras före prefix/konvertering/transform och efter transform.
En tom källa kan därför inte bli en konstgjord URI via ett prefix. Tom
Structured och entiteter utan skrivbara attribut ger inga skrivningar,
även med create; explicit delete/removeAttributes fungerar fortsatt.
Sum/min/max använder samma finita numeriska konvertering som Number (inklusive
numerisk text och bool → 0/1). AnyTrue/allTrue använder bool-/numerisk
konvertering; fel kontrolleras för alla närvarande ingångar före reducering,
även när en tidigare ingång redan avgör boolean-resultatet. Coalesce/firstPresent
väljer första närvarande värdet och måltypen kontrolleras på det valda resultatet.

Objekt/listor i malluttryck renderas som JSON; tal/booleska värden som
JSON-skalärer och strängar som rå text. Identitets-/metadatagenvägar har
företräde framför dynamiska fält. Namn får användas uttryckligen i en egen
ID-mall, men dess författare ansvarar då för kollisioner, namnbyten och
tidigare målentiteter. Ingen automatisk rensning eller målidentitetshistorik
införs; samma ansvar gäller vid byte av ID-mall eller modellprefix.

## Statisk regelvalidering

YAML/seed, regel-API, snapshot och offline-preview använder samma `Validate`.
Statiska entitets-ID:n och `observedBy` måste vara absoluta URI:er, liksom
konstanta Relationship-referenser (även varje listelement efter transform/prefix).
Statiska typer får vara context-termer eller URI:er, men inte innehålla
whitespace/kontrolltecken. Explicit context måste vara en HTTP(S)-URL.
Ingen context hämtas och inga modellnamn eller attributval hårdkodas.

Konstanter och `value` utan malluttryck kontrolleras genom körningens
transformer, prefix och typkonvertering. DateTime kräver RFC 3339; numeriska
resultat och tal i listor måste vara finita. LanguageMap kräver strängvärden.
Kontrollerna omfattar även Structured-delproperties. Konstanta aggregatingångar
kontrolleras för funktionens ingångstyp även när andra ingångar är dynamiska;
när alla ingångar är statiska kontrolleras även aggregatets resultat.
Transformargument måste vara finita och division med noll avvisas oavsett källa.

Dynamiska mallar ersätts inte med gissade värden vid validering. Renderade
ID:n/metadata och GeoJSON-struktur kontrolleras vid körning av etapp 6:s
gemensamma utdata-validerare.
Structured-delvärdens konvertering och gemensam källutvärdering är verifierade
i etapp 3.9. Etapp 5 använder gemensam närvaro-/aggregat-/konverteringspolicy
i statisk validering och körning. Statisk kontroll ersätter inte utdata-validering.

### Verifierad konstruktionsparitet (etapp 3.9)

Vanliga properties, `derive.of` och Structured-delvärden använder samma
källutvärderare. `const`, `value`, `latlon` och `timestamp` kan användas som
aggregatingångar, utöver kindens egna källor. Mallar och entitetsreferenser
utvärderas även där. `object`/`env` är endast meningsfulla tillsammans med
`resource` eller `name` och avvisas annars. Ingångens `prefix` appliceras
före aggregatet, propertyns prefix efter aggregatet och eventuella transformer.
Prefix kräver en sträng eller en lista med enbart strängar; fel typ avvisas
statiskt när den är känd och annars vid körning.

Structured innehåller råa JSON-delvärden, inte nästlade NGSI-LD-attribut.
Delvärden använder samma typkonvertering som attribut efter transform/prefix;
Number, Boolean, text, listor, DateTime, Geo, Relationship och LanguageMap
behåller sina respektive typkontroller. Geo normaliseras till GeoJSON och
Relationship till URI-sträng(ar). Delvärden kan använda `derive`; required
saknad delkälla undertrycker hela entiteten. Nästlad Structured, `array` och
metadata `unit`/`observedAt`/`observedBy` på delvärden avvisas. Etapp 5 utökar
tidsmetadata till alla attributtyper, inklusive DateTime, LanguageMap och
Structured; DateTime behåller JSON-LD:s typade value-form.
Geo använder regelns target även för latlon och utan explicit observationstid.

Referensfel pekar ut regel, entitet, attributväg och uttryck; körningsfel bär
motsvarande kontext även för aggregatingångar och Structured. Ursprungliga
fel bevaras med wrapping. Felkontexten återger inte hela källpayloads.

Detta verifierar konstruktionernas källupplösning och konvertering. Etapp 5
slutför policyn för tomma värden, saknade property-mallar, aggregat utan
ingångar, dynamiska aggregattypfel och tidskällor enligt avsnitten här.
Etapp 6 validerar den faktiska serialiserade NGSI-LD-utdatan enligt den
begränsade profil som beskrivs nedan.

### Regeluppsättningens ID-/typkonflikter

Identiska ID-mallar med olika typmallar avvisas inom samma regel och mellan
regler vars matchningar potentiellt överlappar. Samma ID-/typmall får
återanvändas. Åtskilda tenants och oförenliga matchningsvillkor ger ingen
konflikt; en global regel (tom tenant) kan överlappa alla tenants.

Överlapp följer motorns filter: kind, event, lifecycle och relationRemoved
jämförs exakt, även när event/lifecycle är tomma. Övriga skalära filter, utom
resource, har tomt wildcard; två olika, explicit angivna värden utesluter överlapp.
SensorType jämförs utan hänsyn till skiftläge. Olika resource-villkor utesluter
inte överlapp eftersom en observation kan innehålla flera resurser.
Specificitet och ordning gör inte matchningsmängderna åtskilda. Motorn väljer
fortfarande en vinnande regel; kontrollen är definitionskonsistens, inte
en kombination av matchande regler eller diagnostik av all skuggning.

Kontrollen är statisk och konservativ: den jämför malltext, renderar inga
uttryck och hämtar/expanderar ingen context. Den bevisar inte att olika
ID-mallar aldrig renderar samma ID, att olika typmallar aldrig renderar samma
typ eller att åtskilda händelser aldrig skriver till samma brokerentitet över
tid. Regelns författare ansvarar fortsatt för målidentiteter och modellval.
Regel-API:s mutationer, seed och snapshot använder samma uppsättningskontroll;
offline validate/preview kontrollerar endast den angivna regeln.

## Regelval och prioritet

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
Things kräver exakt en dokumenterad eventkälla. `match.device` avvisas på
things eftersom ingen device-identitet finns där. `match.toType` avgränsar
relationens råa toThingType separat från from-sidan och är endast giltigt
för things.v1.relations. Det jämförs exakt och använder inte målens subType.

En regel får producera flera entiteter i deklarerad ordning. Namngivna
entitetsreferenser måste avse ett tidigare deklarerat ID; själv-/framåt-
referenser i ID och cykler avvisas. En entitets övriga uttryck kan använda
dess eget redan renderade ID. Alla uttrycksplatser har samma referensregler.
Matchande regler kombineras inte. Ingen match ger ett förklarbart skip utan
broker-/tokenkontakt. En match utan skrivbara properties ger inga skrivningar.
Överlapp/skuggning ska vara diagnostik, inte falska konflikter mellan tenants.

Motorns `PotentialOverlaps` rapporterar konservativa par utan att avvisa
överlapp; `ExplainMatch` anger varje faktiskt matchande regels prioritet,
specificitet, stabila index, metadata och skuggningsorsak (lägre prioritet,
lägre specificitet eller senare ordning). Snapshot-omladdning loggar potentiella
överlapp på debug; measurement-vägen loggar förlorande matchningar med regel-ID
och snapshotversion. Ett potentiellt överlapp bevisar inte global skuggning.
Diagnostik i aktiv preview/GUI följer separat i etapp 8.

## Tidsmetadata (etapp 5)

Default observedAt är observationens egen tid. Measurements använder
resursens tid med objektheader som fallback, aldrig meddelande-/väggtid som
fallback. Things-värden läser metadata från samma `values.<property>.value`-
post som värdet. Konstanter, namn, platsmetadata och rena ID-referenser får
ingen implicit observedAt. Alla attributtyper skriver samma valda tid.

| observedAt | Measurement | Things |
| --- | --- | --- |
| osatt | egen resurstid/header-fallback | det upplösta värdets egen observedAt |
| record | upplöst resurstid/header-fallback | upplöst värdes egen observedAt |
| header | objektheaderns bastid, om tillgänglig | values: max observedAt; relation/lifecycle: changedAt |
| message | MessageAccepted.timestamp | values: max observedAt (fallback event.publishedAt utan värdetider); relation/lifecycle: changedAt |
| field:<väg> | record-tid via kortnamn eller explicit tidsfält | explicit tidsfält |
| none | ingen tid | ingen tid |

J2(a): `timestamp: true` använder konsumenttiden (aldrig transporttid):
measurement-meddelandets timestamp respektive things max-observedAt/
changedAt, trunkerad till millisekunder (aldrig avrundad framåt) och tom vid
saknad tid. `{{timestamp}}`
är samma konsumenttid. `event.publishedAt` förblir explicit valbar via
`{{event.publishedAt}}` för diagnostik. De explicita källvyerna gör
observation/publicering/förändring separat valbara som DateTime-värden
och observedAt.

Fältmallar bevarar källmetadata; firstPresent tar metadata från sitt valda
argument, safeURI från sitt argument. Mallar med flera mätuttryck använder
senaste ingångstid och markerar blandade enheter. Numeriska aggregat och
Structured använder senaste närvarande ingångstid; derive coalesce/firstPresent
den valda ingångens tid. Metadata via `name` får ingen implicit aggregattid.
Explicit observedAt kan alltid välja annan dokumenterad tidskälla. Saknad/zero
tid utelämnas; ett närvarande felaktigt field-tidsvärde ger fel. Källtider
bevarar tillgänglig RFC3339Nano-precision, inklusive relativ SenML-tid.
Broker-utdata (DateTime-värden och observedAt) trunkeras alltid till
millisekunder: Orion avvisar nanosekunder (9 decimaler) med
"expected end-of-string". Trunkering, aldrig avrundning, används så att
tiden aldrig flyttas framåt.

## Enheter och explicit konvertering (etapp 5)

Källenheter är åtkomliga i observationsvyer och följer värdet även genom
fältmallar och aggregat. Numeriska aggregat kräver ekvivalenta kända enheter;
omärkta konstanta ingångar behandlas som författarens uttryckliga skalärer.
AnyTrue/allTrue ger logiska, dimensionslösa resultat (C62 när enheten prövas),
inte temperatur-/tryckvärden. Skalära numeriska/boolean-reducerare avvisas
statiskt på måltyper som de aldrig kan producera.
Ingen unitCode skrivs automatiskt från källenheten: `unit` eller convert.to
anger målens kod. En känd källenhet får inte märkas om utan ekvivalens eller
explicit `convert`; blandade mall-enheter kan inte märkas om/konverteras.

Ekvivalenta stavningar/koder är generella: Cel/°C→CEL, K→KEL, ppm/PPM→59,
%/pct/%RH→P1, Pa→PAL, kPa→KPA, m→MTR, mm→MMT, m3/m³→MTQ, S→MHO,
1→C62. Okända koder är endast ekvivalenta vid exakt likhet. Detta är en liten
kodaliaslista, inte fullständig fysikalisk enhets- eller SDM-validering.

`convert: {from: Cel, to: KEL, offset: 273.15}` applicerar
`värde * factor + offset` före befintliga transformsteg; factor default 1,
offset default 0. NumberArray konverterar varje element utan att mutera
källan eller regeluppsättningen. From/to krävs, numeriska argument och
resultat måste vara finita, och olika icke-ekvivalenta enheter kräver en
icke-identisk numerisk omvandling. Känd faktisk enhet måste stämma med from;
vid saknad källenhet är from författarens uttryckliga enhetsdeklaration.
Författaren ansvarar för fysisk korrekthet hos factor/offset. Om unit också
anges måste den vara ekvivalent med to; annars används to som unitCode.
Konvertering stöds endast på Number/NumberArray och kan användas på
Structured-delvärden (som fortsatt inte skriver attributmetadata).

Pa→KPA i standardregeln deklareras nu som explicit faktor 0.001; rums
partOf-regel avgränsar explicit toType till building. Inga sådana modellval
finns i motorn.

## Implementerad NGSI-LD-grundvalidering (etapp 6)

Motor och offline-preview använder samma `BuildEntity`/`ValidateEntityWrites`.
Kontrollen bygger bibliotekets verkliga normaliserade JSON, utan att hämta
context eller göra SDM-uppslag. Produktionsgrindarna verifierar även effektiv
fallback-context före broker/token. Skrivarens sista grind kontrollerar hela
batchen före första operationen, så ett senare strukturfel inte ger en
delvis exekverad batch. Runtime header-kontroll gäller också delete/remove.
Detta förhindrar inte partiella nätverks-/brokersvar efter godkänd validering;
deras leveransutfall och felklassificering hör till etapp 7.

- Entitets-ID:n och referenser kräver absoluta URI:er med giltig percentkodning,
  UTF-8 och utan otillåtna tecken/whitespace/control. URI-schemans semantik
  (utöver HTTP(S)-context) verifieras inte fullständigt.
- Typ-/attributtermer är Unicode-bokstäver/underscore först, följt av
  bokstäver/siffror/underscore/bindestreck/punkt, eller absoluta URI:er.
  JSON-LD-keywords och NGSI-LD-strukturfältskollisioner avvisas.
- Context väljs via regeln, därefter deployment-fallback, därefter bibliotekets
  core-context. Regeln tillåter endast en HTTP(S)-URL utan credentials.
  Biblioteket serialiserar denna singleton som en enelementsarray; det är inte
  stöd för användardefinierade context-arrayer eller inline-context.
- Property skriver scalar number/text/bool, homogen tal-/textlista, Structured
  JSON eller exakt typad DateTime `{"@type":"DateTime","@value":"..."}`.
  GeoProperty skriver GeoJSON, Relationship `object` som URI eller platt URI-lista,
  LanguageProperty en språk-till-text-mapp (språktaggsyntax/`@none`).
- Tal måste vara finita, även i listor och Structured. Datum/observedAt kräver
  strikt RFC3339 med punkt som decimaltecken och högst tre decimaler
  (millisekunder) i broker-utdata; indata med nanosekunder accepteras och
  trunkeras. Go:s
  datumvalidering används efter syntaxtest. Leap seconds ingår inte i profilen.
- ObservedAt stöds på alla attributtyper. ObservedBy kräver en skalär URI-
  Relationship på numerisk Property. UnitCode är en icke-tom kod utan whitespace/
  control på numeriskt värde/lista; enhetsreglerna från etapp 5 gäller fortsatt.
  Okända attributtyper/metadata (t.ex. datasetId) avvisas.
- GeoJSON stöder Point/MultiPoint/LineString/MultiLineString/Polygon/MultiPolygon,
  faktiska finita numeriska koordinater (inte strängar/bool), 2D eller 3D med
  konsekvent dimension i geometrin, WGS84 lon ±180 och lat ±90. LineString har
  minst två positioner; polygon har minst en ring, minst fyra positioner per
  ring och identiska första/sista positioner inklusive eventuell höjd.
  Multigeometrier måste ha minst en komponent och samma regler per komponent.
  Geo-utdatan normaliseras till type/coordinates; bbox/foreign members behålls
  endast i den råa källvyn, inte i målens GeoProperty.

Begränsningar: ingen context-expansion/termupplösning, språkregisterkontroll,
full URI-schema-/ETSI-/SDM-konformitet, orientering/topologi/självskärning/
hål-inneslutning för geometrier, GeometryCollection, datasetId, VocabProperty,
ListProperty/ListRelationship, struktur-/boolean-/date-arrayer som attribut,
avancerade attributinstanser, inbäddad context eller temporal lagring.
Structured-delvärden är rå JSON och saknar egen NGSI-LD-attributmetadata.
`required` styr frånvaro och undertryckning, inte obligatoriska fält i en viss
Smart Data Model. Nya generiska modelltyper/attribut accepteras utan modellnamn
i validatorn, men standardreglernas modellkonformitet granskas separat i etapp 8.

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

## Brokerskrivning och leveransutfall (etapp 7)

Tjänstens produktionsklient är nu en smal skrivklient i transform-modulen.
Den återanvänder bibliotekets entity/fragment/result-typer men läser HTTP-
status och body själv. Den låsta brokerklientens MergeEntityResult innehåller
inga fält och NewMergeEntityResult ignorerar 207-body; den kan därför inte
bevisa full framgång. HTTPError och PartialUpdateError bevarar bounded body
(max 1 MiB) och UpdateResult för diagnostik, men Error-strängar loggar endast
status/antal och inte body, tokens eller credentials.

| Regeloperation | HTTP och framgång | Omtag |
| --- | --- | --- |
| create | POST /ngsi-ld/v1/entities; 201 lyckas, 409 redan finns är OK | ändrar inte en redan existerande entitet |
| merge-or-create | PATCH /ngsi-ld/v1/entities/{escapedId}; 204 eller verifierad 207 lyckas; 404 följs av POST | POST 409-race följs av exakt en PATCH, ingen oändlig lokal retry |
| removeAttributes | samma PATCH med namngivna null-värden; 404 är OK | namngiven rensning upprepas idempotent |
| delete | DELETE /ngsi-ld/v1/entities/{escapedId}; 204 eller 404 lyckas | saknad entitet är redan raderad |

PATCH-null-rensningen är tjänstens dokumenterade broker-/facadeprofil,
inte ett påstående om full ETSI-standardkonformitet för attribut-delete.
Den kräver en broker/facade som stödjer denna operation. Standardens separata
DELETE-attribute-endpoint implementeras inte av denna profil. Content-Type
för JSON-skrivningar är application/ld+json, tenant och user-agent bevaras,
och förvald/regelvald singleton-context skickas enligt etapp 6.

207 måste vara ett entydigt UpdateResult-JSON-dokument med updated/notUpdated.
Tomt, malformed/okänt schema, tomma/dubbla updated-namn eller ofullständig
bekräftelse ger beroendefel, inte framgång. Alla-succeeded 207 kräver lika
många unika bekräftelser som skickade attribut; JSON-LD-termexpansion hämtas
inte för att jämföra compact mot expanded namn. Minst ett notUpdated ger
PartialUpdateError med bevarade uppdaterade/misslyckade attribut/reasons.
Reason-text matchas inte heuristiskt mot permanentfel; delutfall är retrybart.

Pure indata-/regel-/transformations-/utdatafel markeras messaging.Permanent.
Broker HTTP 400/422/413 är irreparabla requestfel, förutom NonexistentTenant
som är ett beroende-/konfigurationsfel. 401/403/408/429/5xx, transport-, token-,
timeout-, okända status-/beroendefel och partiella svar förblir tillfälliga.
Skippad tenant/no-match/inga skrivningar ger nil. Deployment-fallback-context-
fel behandlas som beroendefel, inte permanent skada i varje sensorhändelse.

Messaging-kontraktet verifierades i den låsta
v0.0.0-20260910052156-098a088f6b4f, inklusive dess egna tester:
nil ackas; Permanent reject(false) går till slutlig hook/DLX om kökonfiguration
finns; temporary återpubliceras med samma routing, identitet och spårning,
inkrementerad x-diwise-attempt, confirm-before-ack. Default är fem totala
försök; vid slutlig budget rejectas leveransen. Stopp/misslyckad retry-publicering
nackar med requeue och behåller försöksnumret. DLX-kö/retention är driftkonfiguration,
inte en garanti som transform-tjänsten skapar. Flera handlers kombineras med
permanent dominans i biblioteket; transform undviker att producera blandat
data-/beroendeutfall inom samma measurement-leverans genom att transformera
och validera alla observationer före I/O och sedan avbryta på första skrivfel.

Skrivordning är stabil. WriteBatchError bevarar antal helt slutförda tidigare
skrivningar, misslyckad operation/regelmetadata och den underliggande orsaken,
inklusive eventuell attr-partialitet i den misslyckade operationen. Replay
spelar om leveransen från början; PATCH-merge, create-if-absent, rensning och
delete konvergerar för samma isolerade operation. Ingen persistent dedup,
recency/revisionsspärr eller garanti mot sena/omordnade händelser införs.
En lyckad tidigare skrivning rollbackas inte av ett senare HTTP-fel.

NGSI_CB_TIMEOUT (default 10s) och OAUTH2_TOKEN_TIMEOUT (default 5s) är positiva
Go-durationer. Brokerbudget omfattar hela HTTP-operationen, inklusive eventuell
tokenförberedelse; token har också egen kortare budget. Token refresh/cache och
väntan på cache-/entity-lås kan avbrytas av aktuell request-context. Cache delar
token för samma client credentials och invalidieras efter broker 401; ingen
anonymous fallback vid tokenfel. OAuth-TLS-bool/default bevaras. Fast sleep efter
merge är borttagen: HTTP-svaret är synkron operationsgräns, inte ett bevis på
brokerintern läskonsistens. Standard HTTP-redirect följs inte för skriv-/tokenanrop.

Handler/apply/HTTP/token-spans följer samma parent-context och fel sätts på
operationsspan. OTel HTTP-transport förmedlar aktiv trace-context. Loggar/OTel-
attribut bär tenant, käll-/entitets-ID, regel-ID/revision/snapshot och operation;
producentens eventId tillförs som message_id där det finns. Inga rutinmässiga
fulla sensorpayloads, tokens, credentials eller råa response-body loggas.

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

## Fastställda verksamhetsbeslut

- Saknad/null/tom källa utelämnas; noll/false är närvarande. Attributrensning
  är en explicit operation. Aggregat utan ingångsvärden skriver ingenting.
- Standardtid för dynamiska värden är respektive observationstid, med
  measurement-objektets bastid som fallback. Things-värden använder sin egen
  observedAt. Konstanter och namnmetadata får ingen automatisk observedAt.
  Saknad tid ersätts inte av aktuell klocktid. Regler får välja tidskälla.
- Växthusets co2 behålls i PPM med explicit enhet. Detta är ett avsiktligt
  avsteg från AgriGreenhouse-schemats mg/L, inte en implicit enhetsomvandling.
- `{{id}}` är standard. Portspecifika entiteter väljs i regeln; portarnas
  mätströmmar binds normalt till olika egenskaper i en sak före transformation.
- Exakt en regel vinner. Ingen join mellan separata measurements/things-event
  och inget recency-/revisionsskydd mot gamla händelser införs nu.
- Ett växthus kan referera till flera tankars separata mätentiteter. En tank
  kan tillhöra högst ett växthus. Set/remove/flytt får endast ändra berörd
  referens; inga tankmätvärden summeras eller sammanförs på växthuset.
- Grundläggande NGSI-LD-validering gäller nu. Full versionerad SDM-validering
  är en framtida valfri funktion, inte ett krav för denna etapp.

Besluten är målkontrakt för återstående implementation. Bland annat kräver
dagens enelementslista/rensning av hela attributet en rättning för flera tankar.

### Ägarskap för tankreferenser

Vald implementation är en modellneutral och tenant-isolerad relationsprojektion
i transform-tjänstens befintliga PostgreSQL. Den håller källrelationer och
medlemskap, inte tankarnas mätvärden eller färdiga FIWARE-modellval. Reglerna
bestämmer fortfarande mål-ID, typ och attribut. En delta-händelse måste kunna
bygga den kompletta referensmängden för berört mål och vid flytt även det
tidigare målet. Transaktioner/samordning ska skydda samtidiga instanser;
omstart och brokerfel får inte tappa medlemskap eller felaktigt ackas som
full framgång. Befintliga relationer behöver en explicit bootstrap/återsynk.
Detta är hantering av en relationsmängd, inte join eller aggregering av
separata measurement-/things-värdesevent, och inför inte recency-skydd.
