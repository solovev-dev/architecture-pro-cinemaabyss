package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

const (
	upstreamNameMonolith = "monolith"
	upstreamNameMovies   = "movies-service"

	upstreamTimeout = 5 * time.Second
	badGatewayBody  = "bad gateway"
)

// routeMoviesToNewService reports whether one exact /api/movies request
// goes to movies-service.
//
// When gradual is false the migration is finished: every request goes to
// movies-service and percent is ignored, including 0.
// When gradual is true, the request goes to movies-service only when
// roll < percent. roll is 0..99, so percent 0 never matches and percent
// 100 always matches.
func routeMoviesToNewService(gradual bool, percent int, roll int) bool {
	if !gradual {
		return true
	}
	return roll < percent
}

func newRouter(cfg Config) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)

	transport := newUpstreamTransport()
	monolith := newUpstreamProxy(cfg.MonolithURL, transport)
	movies := newUpstreamProxy(cfg.MoviesServiceURL, transport)
	events := newUpstreamProxy(cfg.EventsServiceURL, transport)

	mux.HandleFunc("/api/movies", handleExactMovies(cfg, monolith, movies))
	// Monolith registers only the exact /api/movies path, so the subtree
	// must not be rolled by percent or /api/movies/health would 404.
	mux.HandleFunc("/api/movies/", handleMoviesSubtree(cfg, movies))

	mux.Handle("/api/events", events)
	mux.Handle("/api/events/", events)

	mountMonolithPrefixes(mux, monolith)
	mux.Handle("/", monolith)

	return mux
}

func handleExactMovies(cfg Config, monolith, movies http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		roll := rand.Intn(100)
		upstream := monolith
		name := upstreamNameMonolith
		if routeMoviesToNewService(cfg.GradualMigration, cfg.MoviesMigrationPercent, roll) {
			upstream = movies
			name = upstreamNameMovies
		}
		logMovieRoute(r, cfg, name)
		upstream.ServeHTTP(w, r)
	}
}

func handleMoviesSubtree(cfg Config, movies http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logMovieRoute(r, cfg, upstreamNameMovies)
		movies.ServeHTTP(w, r)
	}
}

func logMovieRoute(r *http.Request, cfg Config, upstreamName string) {
	log.Printf(
		"movie route: method=%s path=%s gradual_migration=%t movies_migration_percent=%d upstream=%s",
		r.Method,
		r.URL.Path,
		cfg.GradualMigration,
		cfg.MoviesMigrationPercent,
		upstreamName,
	)
}

// mountMonolithPrefixes pins users, payments, and subscriptions to the
// monolith. A strict prefix keeps /api/usersomething on the catch-all,
// which is also the monolith. The same catch-all covers every other path.
func mountMonolithPrefixes(mux *http.ServeMux, monolith http.Handler) {
	prefixes := []string{"/api/users", "/api/payments", "/api/subscriptions"}
	for _, prefix := range prefixes {
		mux.Handle(prefix, monolith)
		mux.Handle(prefix+"/", monolith)
	}
}

func newUpstreamProxy(rawURL string, transport http.RoundTripper) http.Handler {
	target, err := parseUpstreamURL(rawURL)
	if err != nil {
		return badGatewayHandler(err)
	}
	proxy := newReverseProxy(target, transport)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), upstreamTimeout)
		defer cancel()
		proxy.ServeHTTP(w, r.WithContext(ctx))
	})
}

// parseUpstreamURL checks the address without dialing it. A bad URL must
// not stop process startup; the request that needs it gets 502.
func parseUpstreamURL(rawURL string) (*url.URL, error) {
	target, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid upstream URL %q: %w", rawURL, err)
	}
	scheme := strings.ToLower(target.Scheme)
	if (scheme != "http" && scheme != "https") || target.Host == "" {
		return nil, fmt.Errorf("invalid upstream URL %q", rawURL)
	}
	return target, nil
}

func newReverseProxy(target *url.URL, transport http.RoundTripper) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = target.Scheme
			pr.Out.URL.Host = target.Host
			pr.Out.URL.Path = pr.In.URL.Path
			pr.Out.URL.RawPath = pr.In.URL.RawPath
			pr.Out.URL.RawQuery = pr.In.URL.RawQuery
			pr.Out.Host = target.Host
		},
		Transport:    transport,
		ErrorHandler: proxyErrorHandler,
	}
}

func newUpstreamTransport() *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   upstreamTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ResponseHeaderTimeout: upstreamTimeout,
		TLSHandshakeTimeout:   upstreamTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
	}
}

func proxyErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	log.Printf("upstream error: method=%s path=%s err=%v", r.Method, r.URL.Path, err)
	http.Error(w, badGatewayBody, http.StatusBadGateway)
}

func badGatewayHandler(err error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyErrorHandler(w, r, err)
	})
}
