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

	expected := map[flagType]string{
		listenAddress:      "0.0.0.0",
		controlPort:        "8000",
		servicePort:        "8080",
		contextbrokerUrl:   "http://context-broker",
		oauth2ClientId:     "",
		oauth2ClientSecret: "",
		oauth2TokenUrl:     "",
		oauth2InsecureURL:  "true",
		logLevel:           "debug",
		modelsDir:          "/opt/diwise/config/models",
		deviceMgmtUrl:      "http://iot-device-mgmt",
		defaultContextUrl:  "https://raw.githubusercontent.com/diwise/context-broker/refs/heads/main/assets/jsonldcontexts/default-context.jsonld",
		thingsV1Values:     "false",
		policiesFile:       "/opt/diwise/config/authz.rego",
		authzAccessObject:  "true",
		dbHost:             "",
		dbUser:             "",
		dbPassword:         "",
		dbPort:             "5432",
		dbName:             "diwise",
		dbSSLMode:          "disable",
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
	t.Setenv("MODELS_DIR", "/tmp/models")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[contextbrokerUrl], "http://cb:8080")
	is.Equal(flags[oauth2TokenUrl], "http://token")
	is.Equal(flags[oauth2ClientId], "id")
	is.Equal(flags[oauth2ClientSecret], "secret")
	is.Equal(flags[oauth2InsecureURL], "false")
	is.Equal(flags[logLevel], "info")
	is.Equal(flags[modelsDir], "/tmp/models")
}

// HARM-004: locks CLI-over-env precedence. -loglevel is the only CLI flag.
func TestCLIOverridesEnv(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-transform-fiware", "-loglevel=error"})

	t.Setenv("LOG_LEVEL", "info")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[logLevel], "error")
}

// BASE-002: LISTEN_ADDRESS, CONTROL_PORT and SERVICE_PORT follow the same
// env convention as the reference service. The public models API reversed
// PILOT-003: SERVICE_PORT is effective again.
func TestServerAddressFlagsConfigurable(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-transform-fiware"})

	t.Setenv("LISTEN_ADDRESS", "127.0.0.1")
	t.Setenv("CONTROL_PORT", "9001")
	t.Setenv("SERVICE_PORT", "9090")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[listenAddress], "127.0.0.1")
	is.Equal(flags[controlPort], "9001")
	is.Equal(flags[servicePort], "9090")
}

// Rule storage and policy flags follow env overrides.
func TestStorageAndPolicyEnvOverrides(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-transform-fiware"})

	t.Setenv("POSTGRES_HOST", "pg")
	t.Setenv("POSTGRES_DBNAME", "fiware")
	t.Setenv("POLICIES_FILE", "/tmp/authz.rego")
	t.Setenv("AUTHZ_ACCESS_OBJECT_ENABLED", "true")

	_, flags := parseExternalConfig(context.Background(), defaultFlags())

	is.Equal(flags[dbHost], "pg")
	is.Equal(flags[dbName], "fiware")
	is.Equal(flags[policiesFile], "/tmp/authz.rego")
	is.True(accessObjectEnabled(flags))
}

// AUTHZ_ACCESS_OBJECT_ENABLED uses strconv semantics: invalid values keep
// the legacy tenants model.
func TestAccessObjectEnabled(t *testing.T) {
	is := is.New(t)
	is.True(accessObjectEnabled(flagMap{authzAccessObject: "true"}))
	is.True(!accessObjectEnabled(flagMap{authzAccessObject: "false"}))
	is.True(!accessObjectEnabled(flagMap{authzAccessObject: "yes"}))
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

// REV-015: only the exact string "true" disables TLS verification.
// The test targets the production seam so a changed interpretation
// breaks it.
func TestOAuthInsecureInterpretation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  bool
	}{
		{"exact true", "true", true},
		{"uppercase TRUE", "TRUE", false},
		{"numeric 1", "1", false},
		{"false", "false", false},
		{"empty", "", false},
		{"invalid", "bogus", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			flags := defaultFlags()
			flags[oauth2InsecureURL] = tc.value

			is.Equal(oauthInsecure(flags), tc.want)
		})
	}
}

// E7: THINGS_V1_VALUES gatar prenumerationen på versionerade värdesevent.
// Endast exakt "true" slår på; default är av (explicit cutover-beslut).
func TestThingsV1ValuesFlag(t *testing.T) {
	is := is.New(t)
	withCleanFlags(t, []string{"iot-transform-fiware"})

	_, flags := parseExternalConfig(context.Background(), defaultFlags())
	is.Equal(flags[thingsV1Values], "false")
	is.True(!thingsV1ValuesEnabled(flags))

	withCleanFlags(t, []string{"iot-transform-fiware"})
	t.Setenv("THINGS_V1_VALUES", "true")
	_, flags = parseExternalConfig(context.Background(), defaultFlags())
	is.True(thingsV1ValuesEnabled(flags))
}

func TestThingsV1ValuesFlagRejectsLookalikes(t *testing.T) {
	is := is.New(t)

	for _, v := range []string{"TRUE", "1", "yes", " true"} {
		withCleanFlags(t, []string{"iot-transform-fiware"})
		t.Setenv("THINGS_V1_VALUES", v)
		_, flags := parseExternalConfig(context.Background(), defaultFlags())
		is.True(!thingsV1ValuesEnabled(flags))
	}
}
