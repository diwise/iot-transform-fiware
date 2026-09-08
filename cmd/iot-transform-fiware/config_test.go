package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"testing"

	"github.com/matryer/is"
)

func withCleanFlags(t *testing.T, args []string) {
	t.Helper()

	oldArgs := os.Args
	oldCommandLine := flag.CommandLine
	t.Cleanup(func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
	})

	flag.CommandLine = flag.NewFlagSet(args[0], flag.ContinueOnError)
	os.Args = args
}

// HARM-004: locks all current defaults.
func TestDefaultFlags(t *testing.T) {
	is := is.New(t)

	flags := defaultFlags()

	expected := map[FlagType]string{
		listenAddress:      "0.0.0.0",
		servicePort:        "8080",
		controlPort:        "8000",
		contextbrokerUrl:   "http://context-broker",
		oauth2ClientId:     "",
		oauth2ClientSecret: "",
		oauth2TokenUrl:     "",
		oauth2InsecureURL:  "true",
		logLevel:           "debug",
	}

	is.Equal(len(flags), len(expected))
	for key, want := range expected {
		is.Equal(flags[key], want)
	}
}

// HARM-004: locks env override precedence over defaults.
func TestEnvOverrides(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-transform-fiware"})

	t.Setenv("NGSI_CB_URL", "http://cb:8080")
	t.Setenv("OAUTH2_TOKEN_URL", "http://token")
	t.Setenv("OAUTH2_CLIENT_ID", "id")
	t.Setenv("OAUTH2_CLIENT_SECRET", "secret")
	t.Setenv("OAUTH2_REALM_INSECURE", "false")
	t.Setenv("LOG_LEVEL", "info")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[contextbrokerUrl], "http://cb:8080")
	is.Equal(flags[oauth2TokenUrl], "http://token")
	is.Equal(flags[oauth2ClientId], "id")
	is.Equal(flags[oauth2ClientSecret], "secret")
	is.Equal(flags[oauth2InsecureURL], "false")
	is.Equal(flags[logLevel], "info")
}

// HARM-004: locks CLI-over-env precedence. -loglevel is the only CLI flag.
func TestCLIOverridesEnv(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-transform-fiware", "-loglevel=error"})

	t.Setenv("LOG_LEVEL", "info")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[logLevel], "error")
}

// BASE-002: LISTEN_ADDRESS and CONTROL_PORT follow the same env
// convention as the reference service. SERVICE_PORT is still read but
// unused since the service starts no public server.
func TestServerAddressFlagsConfigurable(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-transform-fiware"})

	t.Setenv("LISTEN_ADDRESS", "127.0.0.1")
	t.Setenv("CONTROL_PORT", "9001")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[listenAddress], "127.0.0.1")
	is.Equal(flags[controlPort], "9001")
}

// HARM-004: locks log level parsing, including the silent debug fallback.
func TestParseLogLevel(t *testing.T) {
	is := is.New(t)

	is.Equal(parseLogLevel("debug"), slog.LevelDebug)
	is.Equal(parseLogLevel("info"), slog.LevelInfo)
	is.Equal(parseLogLevel("warn"), slog.LevelWarn)
	is.Equal(parseLogLevel("warning"), slog.LevelWarn)
	is.Equal(parseLogLevel("error"), slog.LevelError)
	is.Equal(parseLogLevel("bogus"), slog.LevelDebug)
}
