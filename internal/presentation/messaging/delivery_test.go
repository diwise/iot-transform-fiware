package messaging

import (
	"errors"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	rmq "github.com/diwise/messaging-golang/pkg/messaging"
)

func TestStage7DeliveryClassification(t *testing.T) {
	if rmq.ClassifyHandlerError(DataError(errors.New("invalid output"))) != rmq.OutcomePermanent {
		t.Fatal("irreparable data retryable")
	}
	for _, status := range []int{400, 401, 403, 413, 422, 429, 500, 503} {
		err := DependencyError(&contextbroker.HTTPError{Method: "PATCH", Status: status})
		want := rmq.OutcomeTemporary
		if status == 400 || status == 413 || status == 422 {
			want = rmq.OutcomePermanent
		}
		if got := rmq.ClassifyHandlerError(err); got != want {
			t.Fatalf("status %d: %v want %v", status, got, want)
		}
	}
	if rmq.ClassifyHandlerError(DependencyError(&contextbroker.PartialUpdateError{})) != rmq.OutcomeTemporary {
		t.Fatal("partial update should be retryable")
	}
}
