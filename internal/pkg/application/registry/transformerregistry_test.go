package registry

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func TestWeatherObservedMapping(t *testing.T) {
	is := is.New(t)
	r := NewTransformerRegistry()

	tr := r.GetTransformerForMeasurement(context.Background(), "urn:oma:lwm2m:ext:3303/air")

	is.True(isFunc(tr))
	is.Equal("WeatherObserved", getFuncName(tr))
}

/*
	func TestLifeBuoyMapping(t *testing.T) {
		is := is.New(t)
		r := NewTransformerRegistry()

		tr := r.GetTransformerForMeasurement(context.Background(), "urn:oma:lwm2m:ext:3302/lifebuoy")

		is.True(isFunc(tr))
		is.Equal("Lifebuoy", getFuncName(tr))
	}
*/
func TestWaterConsumptionMapping(t *testing.T) {
	is := is.New(t)
	r := NewTransformerRegistry()

	tr := r.GetTransformerForMeasurement(context.Background(), "urn:oma:lwm2m:ext:3424")

	is.True(isFunc(tr))
	is.Equal("WaterConsumptionObserved", getFuncName(tr))
}

func isFunc(v interface{}) bool {
	return reflect.TypeOf(v).Kind() == reflect.Func
}

func getFuncName(v interface{}) string {
	n := runtime.FuncForPC(reflect.ValueOf(v).Pointer()).Name()
	i := strings.LastIndex(n, ".") + 1
	return n[i:]
}
