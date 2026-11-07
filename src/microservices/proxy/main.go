package main

import (
	"log"
	"net/http"
)

const healthBody = "Strangler Fig Proxy is healthy"

func main() {
	cfg := LoadConfig()
	logConfig(cfg)

	log.Printf("Starting proxy service on port %s", cfg.Port)
	log.Fatal(http.ListenAndServe(":"+cfg.Port, newRouter(cfg)))
}

func logConfig(cfg Config) {
	percentNote := ""
	if !cfg.GradualMigration {
		percentNote = " (unused while gradual migration is off)"
	}
	log.Printf(
		"proxy config: port=%q monolith_url=%q movies_service_url=%q events_service_url=%q gradual_migration=%t movies_migration_percent=%d%s",
		cfg.Port,
		cfg.MonolithURL,
		cfg.MoviesServiceURL,
		cfg.EventsServiceURL,
		cfg.GradualMigration,
		cfg.MoviesMigrationPercent,
		percentNote,
	)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(healthBody))
}
