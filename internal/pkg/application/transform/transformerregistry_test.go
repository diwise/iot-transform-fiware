package transform

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/diwise/iot-core/pkg/measurements"
	"github.com/farshidtz/senml/v2"
)

func TestWeatherObservedMapping(t *testing.T) {
	temp := 22.2
	is, pack := testSetup(t, "3303", measurements.Temperature, "air", &temp, nil, "")
	r := NewTransformerRegistry()

	tr := r.GetTransformerForSensorType(context.Background(), transformerName(pack))

	is.True(isFunc(tr))
	is.Equal("WeatherObserved", getFuncName(tr))
}

func TestWaterQualityObservedMapping(t *testing.T) {
	temp := 22.2
	is, pack := testSetup(t, "3303", measurements.Temperature, "water", &temp, nil, "")
	r := NewTransformerRegistry()

	tr := r.GetTransformerForSensorType(context.Background(), transformerName(pack))

	is.True(isFunc(tr))
	is.Equal("WaterQualityObserved", getFuncName(tr))
}

func TestLifeBuoyMapping(t *testing.T) {
	vb := true
	is, pack := testSetup(t, "3302", measurements.Presence, "lifebuoy", nil, &vb, "")
	r := NewTransformerRegistry()

	tr := r.GetTransformerForSensorType(context.Background(), transformerName(pack))

	is.True(isFunc(tr))
	is.Equal("Lifebuoy", getFuncName(tr))
}

func TestWaterConsumptionMapping(t *testing.T) {
	v := 1009.0
	is, pack := testSetup(t, "3424", measurements.CumulatedWaterVolume, "", &v, nil, "")
	r := NewTransformerRegistry()

	tr := r.GetTransformerForSensorType(context.Background(), transformerName(pack))

	is.True(isFunc(tr))
	is.Equal("WaterConsumptionObserved", getFuncName(tr))
}

func transformerName(p senml.Pack) string {
	tn := fmt.Sprintf("%s/%s", p[0].BaseName, p[2].StringValue)
	return strings.TrimSuffix(tn, "/")
}

func isFunc(v interface{}) bool {
	return reflect.TypeOf(v).Kind() == reflect.Func
}

func getFuncName(v interface{}) string {
	n := runtime.FuncForPC(reflect.ValueOf(v).Pointer()).Name()
	i := strings.LastIndex(n, ".") + 1
	return n[i:]
}
