package things

import "time"

func testLocation() Location {
	return Location{Latitude: 62.0, Longitude: 17.0}
}

func testTime() time.Time {
	return time.Date(2024, 11, 19, 10, 49, 59, 0, time.UTC)
}
