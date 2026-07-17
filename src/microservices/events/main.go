package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	movieEventType   = "movie"
	userEventType    = "user"
	paymentEventType = "payment"

	movieEventsTopic   = "movie-events"
	userEventsTopic    = "user-events"
	paymentEventsTopic = "payment-events"
)

type Config struct {
	Port         string
	KafkaBrokers []string
}

type EventEnvelope struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Source     string          `json:"source"`
	ReceivedAt time.Time       `json:"received_at"`
	Payload    json.RawMessage `json:"payload"`
}

type EventService struct {
	config  Config
	writers map[string]*kafka.Writer
}

func main() {
	config := loadConfig()
	service := newEventService(config)
	defer service.close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	service.startConsumers(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", service.healthHandler)
	mux.HandleFunc("/api/events/movie", service.createEventHandler(movieEventType, movieEventsTopic))
	mux.HandleFunc("/api/events/user", service.createEventHandler(userEventType, userEventsTopic))
	mux.HandleFunc("/api/events/payment", service.createEventHandler(paymentEventType, paymentEventsTopic))

	server := &http.Server{
		Addr:              ":" + config.Port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("Events service shutdown error: %v", err)
		}
	}()

	log.Printf("Starting events service on port %s", config.Port)
	log.Printf("Kafka brokers: %s", strings.Join(config.KafkaBrokers, ","))

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func loadConfig() Config {
	return Config{
		Port:         getEnv("PORT", "8082"),
		KafkaBrokers: splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092")),
	}
}

func newEventService(config Config) *EventService {
	writers := map[string]*kafka.Writer{}
	for _, topic := range []string{movieEventsTopic, userEventsTopic, paymentEventsTopic} {
		writers[topic] = &kafka.Writer{
			Addr:         kafka.TCP(config.KafkaBrokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireOne,
		}
	}

	return &EventService{
		config:  config,
		writers: writers,
	}
}

func (s *EventService) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

func (s *EventService) createEventHandler(eventType string, topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
			return
		}

		envelope := EventEnvelope{
			ID:         newEventID(),
			Type:       eventType,
			Source:     "events-service-api",
			ReceivedAt: time.Now().UTC(),
			Payload:    payload,
		}

		messageValue, err := json.Marshal(envelope)
		if err != nil {
			http.Error(w, "Failed to encode event", http.StatusInternalServerError)
			return
		}

		if err := s.publishEvent(r.Context(), topic, envelope.ID, messageValue); err != nil {
			log.Printf("Failed to publish %s event to %s: %v", eventType, topic, err)
			http.Error(w, "Failed to publish event", http.StatusBadGateway)
			return
		}

		log.Printf("Published %s event %s to topic %s", eventType, envelope.ID, topic)
		writeJSON(w, http.StatusCreated, map[string]string{
			"status":   "success",
			"event_id": envelope.ID,
			"topic":    topic,
		})
	}
}

func (s *EventService) publishEvent(ctx context.Context, topic string, eventID string, value []byte) error {
	writer := s.writers[topic]
	message := kafka.Message{
		Key:   []byte(eventID),
		Value: value,
		Time:  time.Now(),
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		lastErr = writer.WriteMessages(publishCtx, message)
		cancel()

		if lastErr == nil {
			return nil
		}

		time.Sleep(time.Duration(attempt) * time.Second)
	}

	return lastErr
}

func (s *EventService) startConsumers(ctx context.Context) {
	topics := []string{movieEventsTopic, userEventsTopic, paymentEventsTopic}
	for _, topic := range topics {
		go s.consumeTopic(ctx, topic)
	}
}

func (s *EventService) consumeTopic(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  s.config.KafkaBrokers,
		Topic:    topic,
		GroupID:  "events-service-" + topic,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Second,
	})
	defer reader.Close()

	log.Printf("Started Kafka consumer for topic %s", topic)

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Kafka consumer error for topic %s: %v", topic, err)
			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("Processed event from topic %s partition=%d offset=%d key=%s payload=%s",
			topic,
			message.Partition,
			message.Offset,
			string(message.Key),
			string(message.Value),
		)
	}
}

func (s *EventService) close() {
	for topic, writer := range s.writers {
		if err := writer.Close(); err != nil {
			log.Printf("Failed to close Kafka writer for topic %s: %v", topic, err)
		}
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, response any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to write JSON response: %v", err)
	}
}

func getEnv(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func newEventID() string {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return time.Now().UTC().Format("20060102150405.000000000")
	}
	return hex.EncodeToString(bytes)
}
