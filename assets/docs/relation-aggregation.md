# Beständiga aggregerade omvända relationer

## Regelkontrakt

`array: true` är oförändrat: en skalär blir en lista i **det aktuella eventet**.
Den samlar aldrig länkar från flera saker. För en komplett invers mängd används:

```yaml
rules:
  - match: {kind: thing, event: things.v1.relations, relation: partOf, type: tank, toType: greenhouse}
    entities:
      - id: "urn:ngsi-ld:WaterQualityObserved:{{relation.from.thingId}}"
        type: WaterQualityObserved
        properties:
          - {target: refParent, type: Relationship, value: "urn:ngsi-ld:AgriGreenhouse:{{relation.to.thingId}}"}
      - id: "urn:ngsi-ld:AgriGreenhouse:{{relation.to.thingId}}"
        type: AgriGreenhouse
        properties:
          - target: hasWaterQualityObserved
            type: Relationship
            aggregate: {direction: reverse}
            value: "urn:ngsi-ld:WaterQualityObserved:{{relation.from.thingId}}"
```

Den andra entiteten deklarerar ägarskapet för en komplett attributmängd.
Varken modellerna, attributnamnet eller URN-prefixen finns i motorn/projektionen.
Antalet tankar är inte begränsat till fyra. Producentens kardinalitet samt
databasens tidsbudgetar och brokerns storleksgränser gäller fortfarande.

- `direction` måste vara `reverse` (inga implicita aggregat eller nya `array`-beteenden).
- Kräver `match.kind: thing`, `event: things.v1.relations`, `relation`,
  `type` och `toType`. Deklareras på set-regeln, inte `relationRemoved:true`.
- Deklarationen används också för remove, flytt, endpoint-delete och återbyggnad.
  Den är **additiv till** vanlig prioritet/specificitet: vanliga properties körs
  med ordinarie regelval, medan varje aktiv aggregatdeklaration äger sitt attribut.
  Medlemsmängden filtreras med deklarationens tenant/relation/type/toType och
  valfria `thingTypeVersion`. Revision är inte ett matchnings-/ordningsfilter.
- Aggregatentiteten måste ha statisk `type`, merge-semantik och exakt en property.
  Vanliga framåtrelationer placeras i en annan entitet i samma regel.
- Propertyn måste vara `Relationship` med `value`, utan `array`, andra källor,
  `derive`, `transform`, `convert`, `prefix`, `required`, `when` eller tidsmetadata.
  `observedAt: none` tillåts; default för aggregatet är ingen observationstid.
- Ägarens `id` använder `{{relation.toId}}` eller `{{relation.to.thingId}}`.
  Medlemmens `value` använder motsvarande `fromId`/`from.thingId`.
  `{{tenant}}`, `field:`-prefix och befintliga `safeURI`/`firstPresent` fungerar.
  Minst en ändpunkts-ID-referens krävs. Namn, metadata, variant och rollreferenser
  till andra entiteter nekas: de kan inte återbyggas ur lagrets minimala länkar.
- `match.subType` och variantfilter nekas för aggregat eftersom projektionen inte
  lagrar dessa. Detta gäller inte vanliga regler, vars kontrakt är oförändrat.
- En deklaration äger ett mål-ID/attribut per tenant. Överlappande deklarationer
  eller vanliga writes/removeAttributes till samma attributhållare avvisas.
  Ägarskap kontrolleras statiskt för motsvarande ID-mallar och även vid körning
  för konkreta kollisioner. Lägg aldrig en skalär motsvarighet i lifecycle-/värderegler.
- Vanlig validate/API/seed/snapshot-laddning använder samma schema. Inline-preview
  av en regel med aggregat ger 400, inte en missvisande enmedlemslista. Använd
  återbyggnadsverktygets read-only-plan mot komplett projektion/importunderlag.
  Äldre webbklienter som inte känner `aggregate` ska inte användas för att redigera
  dessa regler (fältet kan tappas vid roundtrip).

Fullständigt laddningsbart exempel med framåtrelation, inverse, bortkoppling,
delete och separata lifecycle-/värdeuppdateringar finns i
`deployments/examples/tank-greenhouse/rules.yaml`. Fyra exempel-länkar finns i
`links.json`. Katalogen seedas inte automatiskt. Motsvarande deklaration finns
i cfg-showcase `overlays/test/resources/iot-transform-fiware/models/rules.yaml`.

## Händelser, lagring och städning

`source_links` har primärnyckeln `(tenant, relation, from_id, to_id)` och ett
reverse-index på `(tenant, relation, to_id, from_id)`. Källa och mål behåller
råa sak-ID:n, typer och mallversioner; inga FIWARE-modellnamn behövs i lagret.
Relationshändelser sparas även när ingen regel matchar. Values/lifecycle är
inte en dold kompletterande relationsimport.

| Händelse | Resultat |
| --- | --- |
| Set | Länk upsertas; ägarens **hela** matchande reverse-mängd skrivs. |
| Remove | Endast den länkens medlem försvinner; övriga behålls. |
| Move (`previousToId`) | Remove + set atomärt; både tidigare och nytt mål planeras. Det gamla målets typer läses från den lagrade länken, inte från det nya målet. |
| Sista remove | `removeAttributes: [<target>]` via ordinarie PATCH-null, aldrig tom object-lista. |
| Delete av medlem | Alla berörda källlänkar lossas atomärt; överlevande ägares listor räknas om. Vanlig lifecycle-regel raderar medlemmens brokerentitet. |
| Delete av ägare | In-/utgående länkar lossas i projektionen. Överlevande medlemmars framåtrelationer rensas via vanliga removed-regler. Ingen inverse-merge återskapar den borttagna ägaren i samma batch. |

Producenten `iot-things-v2` **blockerar radering av partOf-förälder med aktiva
barn**. Växthuset måste därför först få sina tankar bortkopplade/flyttade. Den
defensiva transformstädningen gäller ett faktiskt mottaget `deleted`-event;
den ändrar inte producentens hierarki- eller delete-kontrakt. För andra
relationsnamn kan producenten behålla inkommande brutna historiska referenser;
transformens projektion lossar dem när ändpunktens delete kommer. Saknad
framåt-removed- eller lifecycle-delete-regel kan inte ersättas med gissade
attribut/modell-ID:n — ange dem i reglerna.

Delete och dess utgående removed-event kan levereras i olika ordning över
topics. Båda vägarna städar samma beständiga mängd. Mål-ID:n sorteras
lexikografiskt och dedupliceras **efter** mallrendering. Tenant används i
samtliga mängdläsningar, mutationer, pending-poster och brokerklientval.

### Transaktioner och återleverans

Alla projektionsmutationer, detach, import och rebuild-grupper tar samma
transaktionella tenantlås. Komplett snapshot, planering och köläggning hör
ihop; planerings-/DB-fel rullar tillbaka länkar, seen-ID och pending-arbete.
Ingen processlokal lista eller read-modify-write mot brokern används.

Exakt relationsåterleverans dedupliceras beständigt på `relationId`; lifecycle
delete använder sitt `eventId` när det finns. Seen-ID:n behålls efter leverans
och omstart. Varje ny applicering får också en intern trigger-identitet:
en legitim A→B→A under tiden A fortfarande är pending får inte tappas genom
payloadbaserad deduplicering. Rebuild kan avsiktligt kölägga samma state igen.

Migration `006_pending_order.sql` lägger till en monoton `delivery_order`.
Ordinarie `DrainOnce` och workern levererar FIFO per `(tenant, entity_id)`,
äldsta kvarvarande först, med DB-radlåset kvar genom HTTP och kvittering.
En yngre lista/delete får inte passera ett felande eller pågående äldre
arbete; andra entiteter/tenants kan fortsätta. En utgången lease räcker inte
för att passera radlåset. Låg-nivå-API:t ClaimPending/MarkDone är inte detta
leveransprotokoll och ska inte blandas med egna parallella leveransworkers.

Brokerfel behåller arbetet för beständig retry via befintlig worker (5 s).
Delvis lyckade batchar bevarar lyckade operationer. Krasch efter lyckad HTTP
men före kvittering kan återleverera samma idempotenta operation **före**
dess efterföljare. Detta är inte exakt-en-gång-leverans eller en distribuerad
DB/broker-transaktion. Permanenta brokerkonfigurations-/storleksfel kräver
operatörsåtgärd; de ackas inte tyst som framgång i pending-kön.

**Ordningsgräns enligt PLAN1050:** inget nytt recency-/källordningsfilter
införs. Ett känt äldre event kan inte återinföra en borttagen länk via replay,
men ett **aldrig tidigare sett** sent set med äldre revision appliceras och
kan återställa länken (även efter delete). Producentens revisioner är metadata,
inte konsumentwatermarks. Samtidighet, FIFO och omstartstester bevisar inte
ordningsoberoende konvergens. Godtycklig omordning av osedda äldre event kräver
ett separat kontraktsbeslut om källordning/watermarks. Vid sådan divergens
krävs en ny auktoritativ import; spela inte okontrollerat historiska set-event.

## Aktivering och befintliga relationer

En regeländring/aktivering laddar bara om reglerna. Den kör **inte** en dold
bootstrap och innebär inte att gamla länkar finns. Kör nedanstående som en
separat, uttrycklig administrativ operation efter att tjänstens version är
uppdaterad. Dessa instruktioner är inte en automatisk driftsättning.

1. Vid uppgradering: låt **gamla pending-kön dräneras** och stoppa gamla workers
   före migration/nya workers. Tidigare rader har ingen säker historisk
   leveransordning att härleda vid BIGSERIAL-backfill. Blanda inte gamla och nya
   workerbinärer. Aktivera `THINGS_V1_VALUES=true` för things-konsumenterna.
2. Säkerställ rätt installerad regelversion (globala regler via seed,
   tenantregler via vanligt revisionsskyddat API). Ändra inte regler under rebuild.
3. Avgör om `source_links` täcker alla aktuella relationer. Länkar som verkligen
   behandlats med projektionen aktiv finns kvar även om ingen regel då matchade.
   En tom/ofullständig projektion, gamla perioder utan konsument och felande
   händelser i DLQ kan inte läkas av enbart rebuild.
4. Finns komplett underlag i transformlagret: kör först read-only-plan och
   därefter explicit köläggning (samma `POSTGRES_*` och `DEFAULT_CONTEXT_URL`
   som tjänsten; ange inte lösenord på kommandoraden):

   ```sh
   go run ./cmd/rebuild-relations -tenant example -relation partOf
   go run ./cmd/rebuild-relations -tenant example -relation partOf -apply
   ```

   JSON-svaret visar regel-snapshotversion, länk-/gruppantal samt planerade
   respektive kölagda writes. **Noll grupper/writes betyder inte komplett historik.**
   Verktyget gör inga migrations-, broker-, RabbitMQ- eller tjänstestart-anrop.
   Den vanliga workern levererar kön; följ fel/pending tills den är tom.

5. Saknas äldre underlag: pausa konfigurations-/relationsskrivningar i källan,
   dränera dess gamla outbox och transportköer, pausa transformens konsumenter.
   Ta sedan en **fullständig aktuell** `GET /api/v1/things/export?tenant=example`
   från things-v2 (kräver `things.read`). Svaret är ObjectSpec[], inte Link[].
   Exportera inte en paginerad saklista som om den vore en komplett snapshot.
   Verktyget löser målens faktiska typ/version i samma tenant ur exporten och
   nekar saknade mål, typkonflikter och dubblettsaker. ObjectSpec saknar
   revisions-/tidsmetadata; importens revision 0 betyder okänd, inte nyare.

   ```sh
   go run ./cmd/rebuild-relations -tenant example -things-export things-export.json
   go run ./cmd/rebuild-relations -tenant example -things-export things-export.json -quiesced -apply
   ```

   Alternativt tillhandahåll komplett neutralt Link[] med tenant/relation/
   fromId/toId/fromKind/toKind (valfria typernas versioner, revision, changedAt).
   `deployments/examples/tank-greenhouse/links.json` visar formatet:

   ```sh
   go run ./cmd/rebuild-relations -tenant example -import links.json
   go run ./cmd/rebuild-relations -tenant example -import links.json -quiesced -apply
   ```

   Import ersätter **hela den valda tenantens** länkprojektion atomärt och
   planerar även gamla mål som blivit tomma. Andra tenants/seen-ID:n ändras
   inte. Import går inte att begränsa med `-relation`. Det finns inget säkert
   upsert/prune-watermark för att samtidigt blanda en gammal export och live-
   remove: därför krävs paus/`-quiesced`. Befintliga BootstrapUpsert/Prune är
   låg-nivåverktyg, inte en lösning på den samtidighetsrisken.
   En avsiktlig import utan några länkar alls kräver dessutom `-allow-empty`:
   en felaktigt tom fil får inte tyst rensa hela tenanten.

6. Starta inflödet igen när importen är committad; workern återtar retry.
   Den nya regeln måste vara aktiv även för samtidiga framtida live-event.
   Import replayar stabila ID-baserade forward-regler; den återskapar inte
   namn/geometri eller mätvärdeshistorik som inte finns i Link-underlaget.

`-models-dir deployments/examples/tank-greenhouse` kan användas med read-only
plan för kandidatregler. `-apply` tillåter endast installerade DB-regler.
En fullständig export är ett operatörsansvar: varken käll-API:t eller verktyget
kan bevisa att en handbyggd partiell fil är komplett. Byten av ID-mall,
attributnamn eller borttagning av aggregatregler städar inte tidigare broker-
attribut automatiskt; planera explicit removeAttributes/migrering före byte.
En tom projektion saknar information om historiska brokerägare utan länkar.

## Verifieringens omfattning

Neutrala motor- och PostgreSQL-tester täcker fyra/fler medlemmar, deduplicerad
sortering, remove/flytt/sista-remove, ändpunktsdelete, kända dubbletter och den
osedda sena-event-gränsen, tenant-isolering, flera instanser, samtidiga drains,
utgången lease under pågående HTTP, retry/omstart, rollback och import/rebuild.
Cancellation under leverans/import verifieras tillsammans med leveransspannens
felstatus och länk till den lagrade transportcontexten.
Handler-tester använder faktisk projektion + regelmotor + pending-brokeradapter.
Modelltester laddar det körbara tank/greenhouse-exemplet och kontrollerar
NGSI-LD-payload samt att lifecycle-/värdewrites inte rör inverse-attributet.
Gamla `array:true`- och övriga regressionstester körs oförändrat.
Ingen live-driftsättning eller fullständig SDM-/JSON-LD-expansion ingår.
