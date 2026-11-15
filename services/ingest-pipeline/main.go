package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

type config struct {
	maxRawBytes                                                                                                                   int
	mode, addr, dbPath, brokers, topic, group, auditToken, opsToken, fieldToken, internalToken, bnpbReprocessURL, bnpbInternalKey string
	maxBytes                                                                                                                      int
}
type app struct {
	cfg                config
	db                 *store
	producer, consumer *kgo.Client
}
type reportEnvelope struct {
	MessageID      string          `json:"message_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	ReceiptID      string          `json:"receipt_id"`
	CorrelationID  string          `json:"correlation_id"`
	Payload        json.RawMessage `json:"payload"`
	ReceivedAt     time.Time       `json:"received_at"`
}
type fieldReport struct {
	ReportID    string          `json:"report_id,omitempty"`
	ReportedAt  string          `json:"reported_at,omitempty"`
	Description string          `json:"description,omitempty"`
	Location    json.RawMessage `json:"location,omitempty"`
}

func main() {
	cfg := loadConfig()
	if cfg.auditToken == "" || cfg.opsToken == "" || cfg.fieldToken == "" || cfg.internalToken == "" {
		log.Fatal("AUDIT_TOKEN, OPS_TOKEN, FIELD_REPORT_TOKEN, and PIPELINE_INTERNAL_TOKEN are required")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.dbPath), 0750); err != nil {
		log.Fatal(err)
	}
	db, err := openStore(cfg.dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.db.Close()
	var producer, consumer *kgo.Client
	if cfg.mode != "consumer" {
		producer, err = kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...), kgo.ClientID("ingest-pipeline-producer"), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordRetries(5))
		if err != nil {
			log.Fatal(err)
		}
		defer producer.Close()
	}
	if cfg.mode != "api" {
		consumer, err = kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...), kgo.ClientID("field-report-consumer"), kgo.ConsumerGroup(cfg.group), kgo.ConsumeTopics(cfg.topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
		if err != nil {
			log.Fatal(err)
		}
		defer consumer.Close()
	}
	a := &app{cfg: cfg, db: db, producer: producer, consumer: consumer}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.mode != "api" {
		go a.consumeReports(ctx)
	}
	if cfg.mode != "consumer" {
		go a.retryLoop(ctx)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.health)
	if cfg.mode != "consumer" {
		mux.HandleFunc("POST /field-reports", a.withBearer(cfg.fieldToken, a.submitFieldReport))
		mux.HandleFunc("POST /internal/dlq/park", a.withBearer(cfg.internalToken, a.parkFailure))
		mux.HandleFunc("GET /internal/audit/unprocessed", a.withBearer(cfg.auditToken, a.listUnprocessed))
		mux.HandleFunc("POST /internal/ops/reprocess", a.withBearer(cfg.opsToken, a.reprocess))
		mux.HandleFunc("GET /internal/ops/messages/{id}", a.withBearer(cfg.opsToken, a.getRawFailure))
	}
	server := &http.Server{Addr: cfg.addr, Handler: correlation(mux), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(c)
	}()
	log.Printf("ingest pipeline listening addr=%s topic=%s", cfg.addr, cfg.topic)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func loadConfig() config {
	max := envInt("MAX_MESSAGE_BYTES", 256*1024)
	maxRaw := envInt("MAX_QUARANTINE_BYTES", 4*1024*1024)
	mode := env("PIPELINE_MODE", "all")
	if mode != "all" && mode != "api" && mode != "consumer" {
		log.Fatalf("PIPELINE_MODE must be all, api, or consumer")
	}
	return config{mode: mode, addr: env("HTTP_ADDR", ":8094"), dbPath: env("PIPELINE_DB_PATH", "./data/ingest.db"), brokers: env("KAFKA_BROKERS", "localhost:9092"), topic: env("FIELD_REPORT_TOPIC", "field-reports"), group: env("FIELD_REPORT_GROUP", "field-report-consumer-group"), auditToken: os.Getenv("AUDIT_TOKEN"), opsToken: os.Getenv("OPS_TOKEN"), fieldToken: os.Getenv("FIELD_REPORT_TOKEN"), internalToken: os.Getenv("PIPELINE_INTERNAL_TOKEN"), bnpbReprocessURL: strings.TrimRight(os.Getenv("BNPB_REPROCESS_URL"), "/"), bnpbInternalKey: os.Getenv("BNPB_INTERNAL_KEY"), maxBytes: max, maxRawBytes: maxRaw}
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	v, e := strconv.Atoi(os.Getenv(k))
	if e != nil || v < 1 {
		return d
	}
	return v
}
func (a *app) withBearer(token string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]string{"code": "unauthorized", "message": "invalid or missing bearer token"}})
			return
		}
		next(w, r)
	}
}
func (a *app) health(w http.ResponseWriter, r *http.Request) {
	if err := a.db.db.PingContext(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *app) submitFieldReport(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 200 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key is required and must be at most 200 bytes")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(a.cfg.maxBytes))
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "field report exceeds configured size limit")
		return
	}
	if !utf8.Valid(body) || !json.Valid(body) {
		writeError(w, http.StatusBadRequest, "field report must be valid UTF-8 JSON")
		return
	}
	if depth, err := jsonDepth(body); err != nil || depth > 64 {
		writeError(w, http.StatusBadRequest, "field report JSON nesting exceeds 64 levels")
		return
	}
	var report fieldReport
	if err := json.Unmarshal(body, &report); err != nil {
		writeError(w, http.StatusBadRequest, "invalid field report")
		return
	}
	if strings.TrimSpace(report.Description) == "" && len(report.Location) == 0 {
		writeError(w, http.StatusBadRequest, "field report must include a description or location")
		return
	}
	corr := r.Header.Get("X-Correlation-ID")
	if corr == "" {
		corr = uuid.NewString()
	}
	receipt := uuid.NewString()
	env := reportEnvelope{MessageID: uuid.NewString(), IdempotencyKey: key, ReceiptID: receipt, CorrelationID: corr, Payload: body, ReceivedAt: time.Now().UTC()}
	encoded, err := json.Marshal(env)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not encode field report")
		return
	}
	receipt, err = a.db.acceptReport(r.Context(), key, receipt, corr, encoded, body)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	stored, published, err := a.db.reportOutbox(r.Context(), key)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not read durable intake state")
		return
	}
	if err := json.Unmarshal(stored, &env); err != nil {
		writeError(w, http.StatusInternalServerError, "stored report envelope is invalid")
		return
	}
	if !published {
		record := &kgo.Record{Topic: a.cfg.topic, Key: []byte(key), Value: stored, Headers: []kgo.RecordHeader{{Key: "x-correlation-id", Value: []byte(env.CorrelationID)}}}
		if err := a.producer.ProduceSync(r.Context(), record).FirstErr(); err != nil {
			writeError(w, http.StatusServiceUnavailable, "report is durably queued locally but broker acknowledgement is pending; retry with the same Idempotency-Key")
			return
		}
		if err := a.db.markReportPublished(r.Context(), key); err != nil {
			writeError(w, http.StatusServiceUnavailable, "broker acknowledged report but receipt update is pending; retry with the same Idempotency-Key")
			return
		}
	}
	w.Header().Set("Location", "/field-reports/"+url.PathEscape(receipt))
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "accepted", "receipt_id": receipt, "message_id": env.MessageID, "durability": "broker-acknowledged"})
}

type parkRequest struct {
	MessageID     string    `json:"message_id"`
	Source        string    `json:"source"`
	SourceRefID   string    `json:"source_ref_id"`
	ReceivedAt    time.Time `json:"received_at"`
	CorrelationID string    `json:"correlation_id"`
	Reason        string    `json:"reason"`
	Retryable     bool      `json:"retryable"`
	Attempts      int       `json:"attempts"`
	RawBase64     string    `json:"raw_base64"`
}

func (a *app) parkFailure(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, int64(a.cfg.maxRawBytes)*2)
	var req parkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid quarantine request")
		return
	}
	if req.Source == "" || req.Reason == "" || req.RawBase64 == "" {
		writeError(w, http.StatusBadRequest, "source, reason, and raw_base64 are required")
		return
	}
	raw, err := base64.StdEncoding.DecodeString(req.RawBase64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "raw_base64 must be valid base64")
		return
	}
	if len(raw) > a.cfg.maxRawBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "quarantine payload exceeds absolute storage limit")
		return
	}
	if req.MessageID == "" {
		req.MessageID = stableFailureID(req.Source, req.SourceRefID, raw)
	}
	created, err := a.db.park(r.Context(), failedMessage{MessageID: req.MessageID, Source: req.Source, SourceRefID: req.SourceRefID, ReceivedAt: req.ReceivedAt, CorrelationID: req.CorrelationID, Reason: req.Reason, Retryable: req.Retryable, Attempts: req.Attempts, Raw: raw})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to durably quarantine message")
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"message_id": req.MessageID, "stored": true, "created": created})
}

type unprocessedItem struct {
	MessageID  string    `json:"message_id"`
	Source     string    `json:"source"`
	ReceivedAt time.Time `json:"received_at"`
	Reason     string    `json:"reason"`
	Retryable  bool      `json:"retryable"`
	Attempts   int       `json:"attempts"`
	Status     string    `json:"status"`
}

type dlqCursor struct {
	Source     string `json:"source"`
	ReceivedAt string `json:"received_at"`
	MessageID  string `json:"message_id"`
}

func (a *app) listUnprocessed(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	if q.Get("limit") != "" {
		n, e := strconv.Atoi(q.Get("limit"))
		if e != nil || n < 1 || n > 1000 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 1000")
			return
		}
		limit = n
	}
	afterAt, afterID := "", ""
	if c := q.Get("cursor"); c != "" {
		b, e := base64.RawURLEncoding.DecodeString(c)
		if e != nil {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		var cursor dlqCursor
		if e = json.Unmarshal(b, &cursor); e != nil || cursor.Source != q.Get("source") || cursor.MessageID == "" {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		if _, e = time.Parse(time.RFC3339Nano, cursor.ReceivedAt); e != nil {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		afterAt, afterID = cursor.ReceivedAt, cursor.MessageID
	}
	items, hasMore, err := a.db.list(r.Context(), q.Get("source"), afterAt, afterID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not read unprocessed messages")
		return
	}
	out := make([]unprocessedItem, 0, len(items))
	for _, m := range items {
		out = append(out, unprocessedItem{m.MessageID, m.Source, m.ReceivedAt, m.Reason, m.Retryable, m.Attempts, m.Status})
	}
	var next any
	if hasMore && len(items) > 0 {
		last := items[len(items)-1]
		cursor, _ := json.Marshal(dlqCursor{Source: q.Get("source"), ReceivedAt: last.ReceivedAt.UTC().Format(time.RFC3339Nano), MessageID: last.MessageID})
		next = base64.RawURLEncoding.EncodeToString(cursor)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "next_cursor": next})
}

func (a *app) reprocess(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var req struct {
		MessageID string `json:"message_id"`
	}
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.MessageID == "" {
		writeError(w, http.StatusBadRequest, "message_id is required")
		return
	}
	m, err := a.db.getFailure(r.Context(), req.MessageID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "message not found")
		} else {
			writeError(w, http.StatusInternalServerError, "could not load message")
		}
		return
	}
	if m.Status == "recovered" {
		writeJSON(w, http.StatusOK, map[string]any{"message_id": m.MessageID, "status": "recovered", "replayed": true})
		return
	}
	if err := a.replayStored(r.Context(), m); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message_id": m.MessageID, "status": "recovered", "replayed": false})
}

func (a *app) replayStored(parent context.Context, m failedMessage) error {
	if m.Source != "PVMBG" {
		return fmt.Errorf("no replay processor is registered for source %s", m.Source)
	}
	earlier, err := a.db.hasEarlierPending(parent, m)
	if err != nil {
		return fmt.Errorf("verify replay order: %w", err)
	}
	if earlier {
		return errors.New("an earlier unprocessed message for this source_ref_id must be replayed first")
	}
	if a.cfg.bnpbReprocessURL == "" {
		return errors.New("BNPB_REPROCESS_URL is not configured; canonical replay is unavailable")
	}
	if err := a.db.incrementAttempt(parent, m.MessageID); err != nil {
		return fmt.Errorf("record replay attempt: %w", err)
	}
	body, _ := json.Marshal(map[string]any{"message_id": m.MessageID, "source": m.Source, "source_ref_id": m.SourceRefID, "received_at": m.ReceivedAt, "correlation_id": m.CorrelationID, "raw_base64": base64.StdEncoding.EncodeToString(m.Raw)})
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.bnpbReprocessURL+"/internal/reprocess", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Key", a.cfg.bnpbInternalKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("canonical replay request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("canonical replay rejected the message (HTTP %d)", resp.StatusCode)
	}
	if err := a.db.markRecovered(parent, m.MessageID); err != nil {
		return fmt.Errorf("canonical replay succeeded but recovery mark is pending: %w", err)
	}
	return nil
}

func (a *app) retryLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			items, err := a.db.listRetrying(ctx, 50)
			if err != nil {
				log.Printf("DLQ retry scan failed: %v", err)
				continue
			}
			for _, m := range items {
				delay := time.Duration(1<<min(m.Attempts, 5)) * 250 * time.Millisecond
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				if err := a.replayStored(ctx, m); err != nil {
					log.Printf("DLQ retry pending message_id=%s attempts=%d error=%v", m.MessageID, m.Attempts+1, err)
				} else {
					log.Printf("DLQ retry recovered message_id=%s", m.MessageID)
				}
			}
		}
	}
}

func (a *app) consumeReports(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := a.consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if err := fetches.Err(); err != nil {
			log.Printf("field report consumer poll failed: %v", err)
			time.Sleep(250 * time.Millisecond)
			continue
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			if len(rec.Value) > a.cfg.maxBytes {
				a.parkRecord(ctx, rec, "payload_too_large: exceeds configured processing limit")
				return
			}
			if !utf8.Valid(rec.Value) || !json.Valid(rec.Value) {
				a.parkRecord(ctx, rec, "malformed_json_or_invalid_utf8")
				return
			}
			if depth, err := jsonDepth(rec.Value); err != nil || depth > 64 {
				a.parkRecord(ctx, rec, "json_nesting_exceeds_64_levels")
				return
			}
			var env reportEnvelope
			if err := json.Unmarshal(rec.Value, &env); err != nil {
				_, parkErr := a.db.park(ctx, failedMessage{Source: "FIELD_REPORT", Reason: "malformed_report_envelope: " + err.Error(), Retryable: false, Raw: rec.Value})
				if parkErr == nil {
					_ = a.consumer.CommitRecords(ctx, rec)
				}
				return
			}
			if env.IdempotencyKey == "" || !utf8.Valid(env.Payload) || !json.Valid(env.Payload) {
				_, parkErr := a.db.park(ctx, failedMessage{MessageID: env.MessageID, Source: "FIELD_REPORT", SourceRefID: env.IdempotencyKey, ReceivedAt: env.ReceivedAt, CorrelationID: env.CorrelationID, Reason: "invalid_field_report_envelope", Retryable: false, Raw: rec.Value})
				if parkErr == nil {
					_ = a.consumer.CommitRecords(ctx, rec)
				}
				return
			}
			inserted, err := a.db.markReportProcessed(ctx, env.IdempotencyKey, env.ReceiptID, env.Payload)
			if err != nil {
				log.Printf("field report left uncommitted key=%s error=%v", env.IdempotencyKey, err)
				return
			}
			if inserted {
				log.Printf("field report processed receipt_id=%s idempotency_key=%s correlation_id=%s result=success", env.ReceiptID, env.IdempotencyKey, env.CorrelationID)
			} else {
				log.Printf("field report processed receipt_id=%s idempotency_key=%s correlation_id=%s result=duplicate", env.ReceiptID, env.IdempotencyKey, env.CorrelationID)
			}
			if err := a.consumer.CommitRecords(ctx, rec); err != nil {
				log.Printf("field report commit failed receipt_id=%s error=%v", env.ReceiptID, err)
			}
		})
	}
}

func (a *app) parkRecord(ctx context.Context, rec *kgo.Record, reason string) {
	_, err := a.db.park(ctx, failedMessage{Source: "FIELD_REPORT", Reason: reason, Retryable: false, Attempts: 1, Raw: rec.Value})
	if err != nil {
		log.Printf("poison field report left uncommitted topic=%s partition=%d offset=%d error=%v", rec.Topic, rec.Partition, rec.Offset, err)
		return
	}
	if err := a.consumer.CommitRecords(ctx, rec); err != nil {
		log.Printf("poison field report commit failed topic=%s partition=%d offset=%d error=%v", rec.Topic, rec.Partition, rec.Offset, err)
	}
}

func correlation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-ID")
		if id == "" || len(id) > 128 {
			id = uuid.NewString()
		}
		w.Header().Set("X-Correlation-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), correlationKey{}, id)))
	})
}

type correlationKey struct{}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, status int, msg string) {
	codes := map[int]string{http.StatusBadRequest: "invalid_request", http.StatusUnauthorized: "unauthorized", http.StatusForbidden: "forbidden", http.StatusNotFound: "not_found", http.StatusConflict: "conflict", http.StatusRequestEntityTooLarge: "payload_too_large", http.StatusUnprocessableEntity: "unprocessable", http.StatusServiceUnavailable: "unavailable", http.StatusInternalServerError: "internal_error"}
	code := codes[status]
	if code == "" {
		code = "request_failed"
	}
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}
func stableFailureID(source, ref string, raw []byte) string {
	h := sha256.New()
	h.Write([]byte(source))
	h.Write([]byte{0})
	h.Write([]byte(ref))
	h.Write([]byte{0})
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil))
}

func jsonDepth(b []byte) (int, error) {
	depth, max := 0, 0
	quoted, escaped := false, false
	for _, c := range b {
		if quoted {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		switch c {
		case '"':
			quoted = true
		case '{', '[':
			depth++
			if depth > max {
				max = depth
			}
		case '}', ']':
			depth--
			if depth < 0 {
				return 0, fmt.Errorf("unbalanced JSON")
			}
		}
	}
	if quoted || depth != 0 {
		return 0, fmt.Errorf("incomplete JSON")
	}
	return max, nil
}

func (a *app) getRawFailure(w http.ResponseWriter, r *http.Request) {
	m, err := a.db.getFailure(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "message not found")
		} else {
			writeError(w, http.StatusInternalServerError, "could not read message")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message_id": m.MessageID, "source": m.Source, "source_ref_id": m.SourceRefID, "received_at": m.ReceivedAt, "reason": m.Reason, "retryable": m.Retryable, "attempts": m.Attempts, "status": m.Status, "raw_base64": base64.StdEncoding.EncodeToString(m.Raw)})
}
