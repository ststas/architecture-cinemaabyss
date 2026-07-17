package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Port                   string
	MonolithURL            *url.URL
	MoviesServiceURL       *url.URL
	EventsServiceURL       *url.URL
	GradualMigration       bool
	MoviesMigrationPercent int
}

type ProxyServer struct {
	config        Config
	monolithProxy *httputil.ReverseProxy
	moviesProxy   *httputil.ReverseProxy
	eventsProxy   *httputil.ReverseProxy
	randomMu      sync.Mutex
	random        *rand.Rand
}

func main() {
	config := loadConfig()

	server := &ProxyServer{
		config:        config,
		monolithProxy: newReverseProxy(config.MonolithURL),
		moviesProxy:   newReverseProxy(config.MoviesServiceURL),
		eventsProxy:   newReverseProxy(config.EventsServiceURL),
		random:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.healthHandler)
	mux.HandleFunc("/", server.proxyHandler)

	log.Printf("Starting proxy service on port %s", config.Port)
	log.Printf("Movies migration: gradual=%t percent=%d", config.GradualMigration, config.MoviesMigrationPercent)
	log.Fatal(http.ListenAndServe(":"+config.Port, mux))
}

func loadConfig() Config {
	return Config{
		Port:                   getEnv("PORT", "8000"),
		MonolithURL:            mustParseURL(getEnv("MONOLITH_URL", "http://localhost:8080")),
		MoviesServiceURL:       mustParseURL(getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")),
		EventsServiceURL:       mustParseURL(getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")),
		GradualMigration:       parseBool(getEnv("GRADUAL_MIGRATION", "false")),
		MoviesMigrationPercent: clampPercent(getEnvAsInt("MOVIES_MIGRATION_PERCENT", 0)),
	}
}

func (s *ProxyServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Strangler Fig Proxy is healthy"))
}

func (s *ProxyServer) proxyHandler(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/api/movies"):
		s.routeMovies(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/events"):
		log.Printf("Proxying %s %s to events-service", r.Method, r.URL.RequestURI())
		s.eventsProxy.ServeHTTP(w, r)
	default:
		log.Printf("Proxying %s %s to monolith", r.Method, r.URL.RequestURI())
		s.monolithProxy.ServeHTTP(w, r)
	}
}

func (s *ProxyServer) routeMovies(w http.ResponseWriter, r *http.Request) {
	if s.shouldRouteMoviesToService() {
		log.Printf("Proxying %s %s to movies-service", r.Method, r.URL.RequestURI())
		s.moviesProxy.ServeHTTP(w, r)
		return
	}

	log.Printf("Proxying %s %s to monolith movie fallback", r.Method, r.URL.RequestURI())
	s.monolithProxy.ServeHTTP(w, r)
}

func (s *ProxyServer) shouldRouteMoviesToService() bool {
	if !s.config.GradualMigration {
		return false
	}

	if s.config.MoviesMigrationPercent <= 0 {
		return false
	}

	if s.config.MoviesMigrationPercent >= 100 {
		return true
	}

	s.randomMu.Lock()
	value := s.random.Intn(100)
	s.randomMu.Unlock()

	return value < s.config.MoviesMigrationPercent
}

func newReverseProxy(target *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director

	proxy.Director = func(r *http.Request) {
		originalHost := r.Host
		originalDirector(r)
		r.Host = target.Host
		r.Header.Set("X-Forwarded-Host", originalHost)
		r.Header.Set("X-Forwarded-Proto", target.Scheme)
	}

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("Proxy error for %s %s: %v", r.Method, r.URL.RequestURI(), err)
		http.Error(w, "Bad gateway", http.StatusBadGateway)
	}

	return proxy
}

func getEnv(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("Invalid %s=%q, using %d", key, value, fallback)
		return fallback
	}

	return parsed
}

func parseBool(value string) bool {
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false
	}
	return parsed
}

func clampPercent(value int) int {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

func mustParseURL(rawURL string) *url.URL {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		log.Fatalf("Invalid service URL %q: %v", rawURL, err)
	}

	if parsed.Scheme == "" || parsed.Host == "" {
		log.Fatalf("Invalid service URL %q: scheme and host are required", rawURL)
	}

	return parsed
}
