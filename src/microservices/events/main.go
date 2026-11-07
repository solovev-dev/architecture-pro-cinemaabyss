package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const httpShutdownTimeout = 10 * time.Second

type healthResponse struct {
	Status bool `json:"status"`
}

func main() {
	cfg := loadConfig()
	log.Printf("events service configured with kafka brokers %v", cfg.KafkaBrokers)

	api := newEventAPI(newProducer(cfg.KafkaBrokers))
	server := newHTTPServer(cfg.Port, api)

	// Consumers dial in the background. ListenAndServe stays available while
	// the broker is still starting.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var consumers sync.WaitGroup
	startConsumers(ctx, cfg.KafkaBrokers, &consumers)
	go serveHTTP(server, stop)

	<-ctx.Done()
	log.Printf("events service shutting down")
	consumers.Wait()
	if err := api.producer.Close(); err != nil {
		log.Printf("close kafka writer: %v", err)
	}
	shutdownHTTP(server)
}

func newHTTPServer(port string, api *eventAPI) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/events/health", handleHealth)
	mux.HandleFunc("POST /api/events/movie", api.handleMovieEvent)
	mux.HandleFunc("POST /api/events/user", api.handleUserEvent)
	mux.HandleFunc("POST /api/events/payment", api.handlePaymentEvent)

	return &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
	}
}

func serveHTTP(server *http.Server, stop context.CancelFunc) {
	log.Printf("events service listening on %s", server.Addr)
	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("events service listen: %v", err)
		stop()
	}
}

func shutdownHTTP(server *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("shutdown http server: %v", err)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(healthResponse{Status: true}); err != nil {
		log.Printf("write health response: %v", err)
	}
}
