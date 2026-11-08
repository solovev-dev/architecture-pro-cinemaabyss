package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"time"
)

const (
	maxEventBodyBytes = 1 << 20

	eventTypeMovie   = "movie"
	eventTypeUser    = "user"
	eventTypePayment = "payment"

	eventStatusSuccess = "success"
)

type eventEnvelope struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp string          `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

type eventResponse struct {
	Status    string        `json:"status"`
	Partition int           `json:"partition"`
	Offset    int64         `json:"offset"`
	Event     eventEnvelope `json:"event"`
}

type errorBody struct {
	Error string `json:"error"`
}

type validationError struct {
	message string
}

func (e *validationError) Error() string {
	return e.message
}

type eventKind struct {
	topic       string
	eventType   string
	entityField string
	suffixField string
	numbers     []string
	strings     []string
}

var (
	movieEventKind = eventKind{
		topic:       topicMovieEvents,
		eventType:   eventTypeMovie,
		entityField: "movie_id",
		suffixField: "action",
		numbers:     []string{"movie_id"},
		strings:     []string{"title", "action"},
	}
	userEventKind = eventKind{
		topic:       topicUserEvents,
		eventType:   eventTypeUser,
		entityField: "user_id",
		suffixField: "action",
		numbers:     []string{"user_id"},
		strings:     []string{"action", "timestamp"},
	}
	paymentEventKind = eventKind{
		topic:       topicPaymentEvents,
		eventType:   eventTypePayment,
		entityField: "payment_id",
		suffixField: "status",
		numbers:     []string{"payment_id", "user_id", "amount"},
		strings:     []string{"status", "timestamp"},
	}
)

type eventAPI struct {
	producer *producer
}

func newEventAPI(producer *producer) *eventAPI {
	return &eventAPI{producer: producer}
}

func (a *eventAPI) handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	a.createEvent(w, r, movieEventKind)
}

func (a *eventAPI) handleUserEvent(w http.ResponseWriter, r *http.Request) {
	a.createEvent(w, r, userEventKind)
}

func (a *eventAPI) handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	a.createEvent(w, r, paymentEventKind)
}

func (a *eventAPI) createEvent(w http.ResponseWriter, r *http.Request, kind eventKind) {
	envelope, key, err := parseEvent(r, kind)
	if err != nil {
		writeValidation(w, err)
		return
	}
	a.publishEvent(w, r.Context(), kind, key, envelope)
}

func (a *eventAPI) publishEvent(w http.ResponseWriter, parent context.Context, kind eventKind, key string, envelope eventEnvelope) {
	value, err := json.Marshal(envelope)
	if err != nil {
		log.Printf("marshal %s event: %v", kind.eventType, err)
		writeError(w, http.StatusInternalServerError, "failed to write event")
		return
	}

	ctx, cancel := context.WithTimeout(parent, kafkaWriteTimeout)
	defer cancel()

	partition, offset, err := a.producer.Publish(ctx, kind.topic, key, value)
	if err != nil {
		log.Printf("publish %s: %v", kind.topic, err)
		writeError(w, http.StatusInternalServerError, "failed to write event")
		return
	}

	log.Printf("published %s key=%s partition=%d offset=%d", kind.topic, key, partition, offset)
	writeJSON(w, http.StatusCreated, eventResponse{
		Status:    eventStatusSuccess,
		Partition: partition,
		Offset:    offset,
		Event:     envelope,
	})
}

func parseEvent(r *http.Request, kind eventKind) (eventEnvelope, string, error) {
	fields, payload, err := readObject(r)
	if err != nil {
		return eventEnvelope{}, "", err
	}
	return buildEvent(kind, fields, payload)
}

func readObject(r *http.Request) (map[string]json.RawMessage, json.RawMessage, error) {
	body, err := readBody(r)
	if err != nil {
		return nil, nil, err
	}

	payload := bytes.TrimSpace(body)
	if len(payload) == 0 || payload[0] != '{' {
		return nil, nil, &validationError{message: "invalid json"}
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil || fields == nil {
		return nil, nil, &validationError{message: "invalid json"}
	}
	return fields, json.RawMessage(payload), nil
}

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	// A nil ResponseWriter keeps the oversized-body status under our control.
	r.Body = http.MaxBytesReader(nil, r.Body, maxEventBodyBytes)

	body, err := io.ReadAll(r.Body)
	if err == nil {
		return body, nil
	}

	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return nil, &validationError{message: "request body too large"}
	}
	return nil, &validationError{message: "invalid json"}
}

func buildEvent(kind eventKind, fields map[string]json.RawMessage, payload json.RawMessage) (eventEnvelope, string, error) {
	numbers, err := requireNumbers(fields, kind.numbers)
	if err != nil {
		return eventEnvelope{}, "", err
	}
	texts, err := requireStrings(fields, kind.strings)
	if err != nil {
		return eventEnvelope{}, "", err
	}

	entityID := numbers[kind.entityField]
	suffix := texts[kind.suffixField]
	if entityID == "" || suffix == "" {
		return eventEnvelope{}, "", &validationError{message: "invalid event id"}
	}

	return eventEnvelope{
		ID:        kind.eventType + "-" + entityID + "-" + suffix,
		Type:      kind.eventType,
		Timestamp: resolveTimestamp(fields),
		Payload:   payload,
	}, entityID, nil
}

func requireNumbers(fields map[string]json.RawMessage, names []string) (map[string]string, error) {
	values := make(map[string]string, len(names))
	for _, name := range names {
		value, err := requireNumber(fields, name)
		if err != nil {
			return nil, err
		}
		values[name] = value
	}
	return values, nil
}

func requireStrings(fields map[string]json.RawMessage, names []string) (map[string]string, error) {
	values := make(map[string]string, len(names))
	for _, name := range names {
		value, err := requireNonEmptyString(fields, name)
		if err != nil {
			return nil, err
		}
		values[name] = value
	}
	return values, nil
}

func requireNumber(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", &validationError{message: name + " is required"}
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	token, err := dec.Token()
	if err != nil {
		return "", &validationError{message: name + " must be a number"}
	}
	number, ok := token.(json.Number)
	if !ok || number.String() == "" {
		return "", &validationError{message: name + " must be a number"}
	}
	if _, err := dec.Token(); err != io.EOF {
		return "", &validationError{message: name + " must be a number"}
	}
	return number.String(), nil
}

func requireNonEmptyString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", &validationError{message: name + " is required"}
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" {
		return "", &validationError{message: name + " must be a non-empty string"}
	}
	return value, nil
}

func resolveTimestamp(fields map[string]json.RawMessage) string {
	raw, ok := fields["timestamp"]
	if ok {
		var value string
		if err := json.Unmarshal(raw, &value); err == nil {
			// RFC3339Nano accepts plain RFC3339 and the fractional form Postman sends.
			if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
				return parsed.UTC().Format(time.RFC3339)
			}
		}
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func writeValidation(w http.ResponseWriter, err error) {
	message := "invalid json"
	var vErr *validationError
	if errors.As(err, &vErr) && vErr.message != "" {
		message = vErr.message
	}
	writeError(w, http.StatusBadRequest, message)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorBody{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("write response: %v", err)
	}
}
