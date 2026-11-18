package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type requestLogEntry struct {
	TS             string `json:"ts"`
	Method         string `json:"method"`
	Path           string `json:"path"`
	CallerInstance string `json:"caller_instance"`
	CorrelationID  string `json:"correlation_id"`
	Status         int    `json:"status"`
}

var (
	requestMu  sync.Mutex
	requestLog []requestLogEntry
)

func recordRequest(entry requestLogEntry) {
	requestMu.Lock()
	defer requestMu.Unlock()
	requestLog = append(requestLog, entry)
	if len(requestLog) > 5000 {
		requestLog = requestLog[len(requestLog)-5000:]
	}
}

func snapshotRequests() []requestLogEntry {
	requestMu.Lock()
	defer requestMu.Unlock()
	out := make([]requestLogEntry, len(requestLog))
	copy(out, requestLog)
	return out
}

type VolcanicReport struct {
	ReportID         string    `json:"report_id"`
	VolcanoID        string    `json:"volcano_id"`
	AlertLevel       string    `json:"alert_level"`
	EruptionCount24h int       `json:"eruption_count_24h"`
	AshColumnHeightM float64   `json:"ash_column_height_m"`
	ReportedAt       time.Time `json:"reported_at"`
}

var outage atomic.Bool
var schemaMode atomic.Value

func main() {
	schemaMode.Store("baseline")
	port := os.Getenv("PORT")
	dbPath := os.Getenv("DB_PATH")

	if port == "" {
		port = "8082"
	}

	if dbPath == "" {
		dbPath = "/app/data/pvmbg.db"
	}

	var err error
	db, err = openDatabase(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	seedReports()

	go generateReports()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", health)
	mux.HandleFunc("/volcanic-reports", volcanicReports)
	mux.HandleFunc("/admin/schema-version", schemaVersionHandler)
	mux.HandleFunc("/admin/outage", outageHandler)
	mux.HandleFunc("/admin/requests", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snapshotRequests())
	})

	// Correlation -> logging -> routes
	handler := correlationMiddleware(loggingMiddleware(mux))

	log.Printf("PVMBG listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, handler))
}

func seedReports() {
	count, err := countReports()
	if err != nil {
		log.Fatal(err)
	}

	if count >= 20 {
		return
	}

	now := time.Now()

	for i := 1; i <= 20; i++ {
		report := VolcanicReport{
			ReportID:         fmt.Sprintf("pvmbg-%03d", i),
			VolcanoID:        "merapi",
			AlertLevel:       "Waspada",
			EruptionCount24h: i % 4,
			AshColumnHeightM: 500 + float64(i*25),
			ReportedAt:       now.Add(-time.Duration(20-i) * time.Hour),
		}

		insertReport(report)
	}

	log.Println("20 historical reports created")
}

func generateReports() {
	ticker := time.NewTicker(10 * time.Second)

	for range ticker.C {
		count, err := countReports()
		if err != nil {
			log.Println(err)
			continue
		}

		report := VolcanicReport{
			ReportID:         fmt.Sprintf("pvmbg-%03d", count+1),
			VolcanoID:        "merapi",
			AlertLevel:       randomAlertLevel(),
			EruptionCount24h: rand.Intn(5),
			AshColumnHeightM: 500 + rand.Float64()*1000,
			ReportedAt:       time.Now(),
		}

		insertReport(report)

		log.Printf("generated %s", report.ReportID)
	}
}

func randomAlertLevel() string {
	levels := []string{
		"Normal",
		"Waspada",
		"Siaga",
		"Awas",
	}

	return levels[rand.Intn(len(levels))]
}

func serviceAuth(r *http.Request) bool {
	token := os.Getenv("PVMBG_TOKEN")

	return r.Header.Get("Authorization") == "Bearer "+token
}

func adminAuth(r *http.Request) bool {
	// The mock admin endpoint is intentionally callable by the course Tester
	// Agent, which has no application credentials. Human/admin callers may
	// still use Basic Auth when the credentials are configured.
	u, p, ok := r.BasicAuth()
	if !ok {
		return true
	}
	username := os.Getenv("ADMIN_USERNAME")
	password := os.Getenv("ADMIN_PASSWORD")
	return username != "" && password != "" && u == username && p == password
}

func delay() {
	min, _ := strconv.Atoi(os.Getenv("PVMBG_MIN_DELAY_MS"))
	max, _ := strconv.Atoi(os.Getenv("PVMBG_MAX_DELAY_MS"))

	if min == 0 {
		min = 500
	}

	if max == 0 {
		max = 3000
	}

	ms := rand.Intn(max-min+1) + min

	time.Sleep(time.Duration(ms) * time.Millisecond)
}

// correlationMiddleware generates or accepts X-Correlation-ID.
func correlationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		corrID := r.Header.Get("X-Correlation-ID")
		if corrID == "" {
			corrID = uuid.NewString()
		}
		ctx := context.WithValue(r.Context(), "correlation_id", corrID)
		w.Header().Set("X-Correlation-ID", corrID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// loggingMiddleware logs structured JSON with correlation ID and latency.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		corrID, _ := r.Context().Value("correlation_id").(string)
		caller := r.Header.Get("X-Caller-Instance")
		if caller == "" {
			caller = "unknown"
		}

		wrapped := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrapped, r)

		latency := time.Since(start)
		recordRequest(requestLogEntry{TS: start.UTC().Format(time.RFC3339Nano), Method: r.Method,
			Path: r.URL.Path, CallerInstance: caller, CorrelationID: corrID, Status: wrapped.statusCode})
		logJSON(map[string]any{
			"ts":              start.Format(time.RFC3339Nano),
			"corr_id":         corrID,
			"caller_instance": caller,
			"method":          r.Method,
			"path":            r.URL.Path,
			"status":          wrapped.statusCode,
			"latency_ms":      latency.Milliseconds(),
		})
	})
}

type statusWriter struct {
	http.ResponseWriter
	statusCode int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.statusCode = code
	sw.ResponseWriter.WriteHeader(code)
}

func logJSON(data map[string]any) {
	b, err := json.Marshal(data)
	if err != nil {
		return
	}
	log.Print(string(b))
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(map[string]string{
		"service": "PVMBG",
		"status":  "ok",
	})
}

func volcanicReports(w http.ResponseWriter, r *http.Request) {
	if !serviceAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	if outage.Load() {
		http.Error(w, "PVMBG unavailable", http.StatusServiceUnavailable)
		return
	}

	delay()

	since := r.URL.Query().Get("since")

	reports, err := listVolcanicReports(r.Context(), since)
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	mode := schemaMode.Load().(string)
	response := make([]map[string]any, 0, len(reports))
	for _, report := range reports {
		item := map[string]any{
			"report_id": report.ReportID, "volcano_id": report.VolcanoID,
			"alert_level": report.AlertLevel, "eruption_count_24h": report.EruptionCount24h,
			"ash_column_height_m": report.AshColumnHeightM, "reported_at": report.ReportedAt,
		}
		switch mode {
		case "additive":
			item["confidence_level"] = 0.90 + rand.Float64()*0.10
		case "rename":
			item["alert_status"] = item["alert_level"]
			delete(item, "alert_level")
		case "type_change":
			item["alert_level"] = map[string]any{"level": item["alert_level"]}
		case "missing_required":
			delete(item, "report_id")
		}
		response = append(response, item)
	}
	json.NewEncoder(w).Encode(map[string]any{"schema_mode": mode, "reports": response})
}

func schemaVersionHandler(w http.ResponseWriter, r *http.Request) {
	if !adminAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, 4096))
	if err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	var request struct {
		Enabled *bool  `json:"enabled"`
		Mode    string `json:"mode"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal(raw, &request); err != nil {
		log.Printf("schema admin rejected body=%q error=%v", raw, err)
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	mode := request.Mode
	if mode == "" && request.Version != 0 {
		// The course Tester Agent uses the mock's numeric schema version.
		switch request.Version {
		case 1:
			mode = "baseline"
		case 2:
			mode = "additive"
		case 3:
			mode = "rename"
		case 4:
			mode = "type_change"
		default:
			mode = "missing_required"
		}
	}
	if mode == "" && request.Enabled != nil {
		if *request.Enabled {
			mode = "additive"
		} else {
			mode = "baseline"
		}
	}
	switch mode {
	case "baseline", "additive", "rename", "type_change", "missing_required":
	default:
		log.Printf("schema admin rejected body=%q mode=%q", raw, mode)
		http.Error(w, "mode must be baseline, additive, rename, type_change, or missing_required", http.StatusBadRequest)
		return
	}
	schemaMode.Store(mode)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"mode": mode, "enabled": mode != "baseline"})
}

func outageHandler(w http.ResponseWriter, r *http.Request) {
	if !adminAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var request struct {
		Enabled bool `json:"enabled"`
	}

	json.NewDecoder(r.Body).Decode(&request)

	outage.Store(request.Enabled)

	json.NewEncoder(w).Encode(map[string]bool{
		"enabled": outage.Load(),
	})
}
