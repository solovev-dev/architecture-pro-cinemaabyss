package main

import (
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	defaultPort             = "8000"
	defaultMonolithURL      = "http://127.0.0.1:8080"
	defaultMoviesServiceURL = "http://127.0.0.1:8081"
	defaultEventsServiceURL = "http://127.0.0.1:8082"

	minMigrationPercent = 0
	maxMigrationPercent = 100
)

// Config is the proxy process configuration, loaded once at startup.
// Upstream URLs are stored as given and are not parsed or dialed here:
// a bad URL fails later, on the request that uses that upstream.
type Config struct {
	Port                   string
	MonolithURL            string
	MoviesServiceURL       string
	EventsServiceURL       string
	GradualMigration       bool
	MoviesMigrationPercent int
}

// LoadConfig reads configuration from the environment.
// Empty PORT and upstream URL variables fall back to local defaults.
// MOVIES_MIGRATION_PERCENT is clamped to 0..100. When gradual migration is
// on and the value is not an integer, it becomes 0 and a warning is logged.
// When gradual migration is off, the percent is unused.
func LoadConfig() Config {
	gradual := parseGradualMigration(os.Getenv("GRADUAL_MIGRATION"))
	rawPercent := os.Getenv("MOVIES_MIGRATION_PERCENT")
	percent, ok := parseMigrationPercent(rawPercent)
	if gradual && !ok {
		log.Printf("warning: MOVIES_MIGRATION_PERCENT %q is not an integer; using 0", rawPercent)
	}

	return Config{
		Port:                   envOr("PORT", defaultPort),
		MonolithURL:            envOr("MONOLITH_URL", defaultMonolithURL),
		MoviesServiceURL:       envOr("MOVIES_SERVICE_URL", defaultMoviesServiceURL),
		EventsServiceURL:       envOr("EVENTS_SERVICE_URL", defaultEventsServiceURL),
		GradualMigration:       gradual,
		MoviesMigrationPercent: percent,
	}
}

func envOr(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

// parseGradualMigration reports whether gradual movie migration is enabled.
// True values are "true", "1", "yes", and "on", compared case-insensitively
// after trimming space. Any other value, including empty, is false.
func parseGradualMigration(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "1", "yes", "on":
		return true
	default:
		return false
	}
}

// parseMigrationPercent parses an integer percentage and clamps it to 0..100.
// ok is false when value is not an integer; percent is then 0.
// The caller decides whether a failed parse is a warning.
func parseMigrationPercent(value string) (percent int, ok bool) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, false
	}
	if parsed < minMigrationPercent {
		return minMigrationPercent, true
	}
	if parsed > maxMigrationPercent {
		return maxMigrationPercent, true
	}
	return parsed, true
}
