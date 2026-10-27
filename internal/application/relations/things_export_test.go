package relations

import "testing"

func TestThingsExportImportContract(t *testing.T) {
	const raw = `[
	{"tenant":"t","thingId":"a","templateId":"sample","templateVersion":"1","relations":[{"name":"partOf","targetThingId":"b","targetTemplateId":"group"}]},
	{"tenant":"t","thingId":"b","templateId":"group","templateVersion":"2"},
	{"tenant":"other","thingId":"a","templateId":"unrelated","relations":[{"name":"partOf","targetThingId":"b"}]}
	]`
	links, err := LinksFromThingsExport([]byte(raw), "t")
	if err != nil || len(links) != 1 {
		t.Fatal(links, err)
	}
	l := links[0]
	if l.FromKind != "sample" || l.ToKind != "group" || l.FromKindVersion != "1" || l.ToKindVersion != "2" || l.Revision != 0 || !l.ChangedAt.IsZero() {
		t.Fatal(l)
	}
	// Another tenant's object must never satisfy a missing target.
	if _, err := LinksFromThingsExport([]byte(raw), "other"); err == nil {
		t.Fatal("cross-tenant target accepted")
	}
	for _, invalid := range []string{
		`[{"tenant":"t","thingId":"a","templateId":"sample","relations":[{"name":"partOf","targetThingId":"missing"}]}]`,
		`[{"tenant":"t","thingId":"a","templateId":"sample"},{"tenant":"t","thingId":"a","templateId":"sample"}]`,
		`[{"tenant":"t","thingId":"a","templateId":"sample","relations":[{"name":"partOf","targetThingId":"b","targetTemplateId":"fake"}]},{"tenant":"t","thingId":"b","templateId":"actual"}]`,
	} {
		if _, err := LinksFromThingsExport([]byte(invalid), "t"); err == nil {
			t.Fatal("invalid export accepted", invalid)
		}
	}
}
