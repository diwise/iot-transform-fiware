package main

import (
	"testing"
	"time"
)

func TestStage7TimeoutBudgetConfiguration(t *testing.T) {
	flags := defaultFlags()
	broker, token, err := timeoutBudgets(flags)
	if err != nil || broker != 10*time.Second || token != 5*time.Second {
		t.Fatalf("defaults: %v %v %v", broker, token, err)
	}
	for _, value := range []string{"0s", "-1s", "not-duration"} {
		flags[brokerTimeout] = value
		if _, _, err := timeoutBudgets(flags); err == nil {
			t.Fatal("invalid timeout accepted")
		}
	}
}
