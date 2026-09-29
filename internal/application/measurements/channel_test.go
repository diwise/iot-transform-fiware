package measurements

import (
	"testing"

	"github.com/diwise/senml"
	"github.com/matryer/is"
)

func TestChannelFromName(t *testing.T) {
	is := is.New(t)

	is.Equal(channelFromName("dev-1/5700"), "")
	is.Equal(channelFromName("dev-1/0"), "")
	is.Equal(channelFromName("dev-1/ch1/5700"), "ch1")
	is.Equal(channelFromName("dev-1/a/b/5700"), "a/b")
}

func TestRecordsPreserveChannelWithoutChangingName(t *testing.T) {
	is := is.New(t)

	v := 1.0
	pack := senml.Pack{
		{Name: "dev-1/0", StringValue: "urn:oma:lwm2m:ext:3303"},
		{Name: "dev-1/ch1/5700", Value: &v},
		{Name: "dev-1/5700", Value: &v},
	}

	recs := recordsFromPack(pack, "urn:oma:lwm2m:ext:3303")
	is.Equal(len(recs), 2)
	is.Equal(recs[0].Name, "5700")
	is.Equal(recs[0].Channel, "ch1")
	is.Equal(recs[1].Name, "5700")
	is.Equal(recs[1].Channel, "")
}
