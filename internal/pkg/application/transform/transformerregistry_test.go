package transform

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestWeatherObservedMapping(t *testing.T) {
	temp := 22.2
	is, pack := testSetup(t, "3303", "Temperature", "air", &temp, nil, "")

	r := NewTransformerRegistry()
	tr := r.DesignateTransformers(context.Background(), pack[0].BaseName+"/"+pack[2].StringValue)

	is.True(isFunc(tr))
	is.Equal("WeatherObserved", getFuncName(tr))
}

func TestWaterQualityObservedMapping(t *testing.T) {
	temp := 22.2
	is, pack := testSetup(t, "3303", "Temperature", "water", &temp, nil, "")

	r := NewTransformerRegistry()

	tr := r.DesignateTransformers(context.Background(), pack[0].BaseName+"/"+pack[2].StringValue)

	is.True(isFunc(tr))
	is.Equal("WaterQualityObserved", getFuncName(tr))
}

func isFunc(v interface{}) bool {
	return reflect.TypeOf(v).Kind() == reflect.Func
}

func getFuncName(v interface{}) string {
	n := runtime.FuncForPC(reflect.ValueOf(v).Pointer()).Name()
	i := strings.LastIndex(n, ".") + 1
	return n[i:]
}
