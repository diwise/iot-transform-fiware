package transform

import (
	"fmt"
	"time"
)

// BrokerPrecision är den maximala bråkdelsprecision Orion accepterar för
// DateTime-värden och observedAt. Orion NGSv2 stöder millisekunder sedan
// 2.5.0; Orion-LD avvisar mer än 6 decimaler (mikrosekunder) med
// "expected end-of-string" vid TIME-position 15. Millisekunder är den
// gemensamma nämnaren och matchar systemtidsstämplarnas precision.
// Trunkering (inte avrundning) används för att aldrig flytta tiden framåt.
const BrokerPrecision = time.Millisecond

// FormatBrokerTime formaterar en tid för NGSI-LD-utdata: UTC, trunkerad till
// millisekunder, RFC3339Nano (skriver högst tre decimaler, trimmar nollor).
// Nolltid ger tom sträng (utelämnad tid).
func FormatBrokerTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Truncate(BrokerPrecision).Format(time.RFC3339Nano)
}

// NormalizeBrokerTimestamp normaliserar ett RFC3339Nano-tidssträngvärde till
// broker-kompatibel millisekundprecision. Indata med nanosekunder accepteras
// (producentkontraktet bevarar dem) men utdata trunkeras alltid.
func NormalizeBrokerTimestamp(value string) (string, error) {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return "", fmt.Errorf("invalid date time: %w", err)
	}
	if t.IsZero() {
		return value, nil
	}
	return FormatBrokerTime(t), nil
}
