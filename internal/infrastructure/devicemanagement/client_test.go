package devicemanagement

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestSensorTypeCachesUntilTTL(t *testing.T) {
	is := is.New(t)

	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"data":{"sensorProfile":{"decoder":"temperature"}}}`))
	}))
	defer srv.Close()

	now := time.Unix(0, 0)
	c := New(srv.URL, srv.Client(), time.Minute)
	c.now = func() time.Time { return now }

	st, err := c.SensorType(context.Background(), "dev-1")
	is.NoErr(err)
	is.Equal(st, "temperature")

	_, err = c.SensorType(context.Background(), "dev-1")
	is.NoErr(err)
	is.Equal(hits, 1) // cache hit inom TTL

	now = now.Add(2 * time.Minute)

	_, err = c.SensorType(context.Background(), "dev-1")
	is.NoErr(err)
	is.Equal(hits, 2) // TTL har gått ut
}

func TestSensorTypeErrorsAreNotCached(t *testing.T) {
	is := is.New(t)

	hits := 0
	fail := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"sensorProfile":{"decoder":"noise"}}}`))
	}))
	defer srv.Close()

	c := New(srv.URL, srv.Client(), DefaultTTL)

	_, err := c.SensorType(context.Background(), "dev-2")
	is.True(err != nil)

	fail = false

	st, err := c.SensorType(context.Background(), "dev-2")
	is.NoErr(err)
	is.Equal(st, "noise")
	is.Equal(hits, 2) // felet cachades inte
}

func TestSensorTypeNotFound(t *testing.T) {
	is := is.New(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(srv.URL, srv.Client(), DefaultTTL)

	_, err := c.SensorType(context.Background(), "missing")
	is.True(errors.Is(err, ErrNotFound))
}

func TestSensorTypeWithoutProfile(t *testing.T) {
	is := is.New(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{}}`))
	}))
	defer srv.Close()

	c := New(srv.URL, srv.Client(), DefaultTTL)

	st, err := c.SensorType(context.Background(), "dev-3")
	is.NoErr(err)
	is.Equal(st, "")
}
