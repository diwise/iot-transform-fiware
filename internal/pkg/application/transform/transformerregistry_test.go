package transform

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestWeatherObservedMapping(t *testing.T) {
	is := testSetup(t)

	r := NewTransformerRegistry()
	tr := r.DesignateTransformers(context.Background(), "Temperature", "temperature")

	is.True(isFunc(tr))
	is.Equal("WeatherObserved", getFuncName(tr))
}

func TestWaterQualityObservedMapping(t *testing.T) {
	is := testSetup(t)

	r := NewTransformerRegistry()
	tr := r.DesignateTransformers(context.Background(), "Temperature", "temperature/water")

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
