package main

import (
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/matryer/is"
)

// Samtliga levererade modeller i deployments/models ska ladda och validera.
func TestShippedModelsLoadAndValidate(t *testing.T) {
	is := is.New(t)

	rules, err := config.LoadModelsDir("../../deployments/models")
	is.NoErr(err)
	is.True(len(rules) > 0)
}
