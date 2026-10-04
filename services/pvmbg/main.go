package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type VolcanicReport struct {
	ReportID         string    `json:"report_id"`
	VolcanoID        string    `json:"volcano_id"`
	AlertLevel       string    `json:"alert_level"`
	EruptionCount24h int       `json:"eruption_count_24h"`
	AshColumnHeightM float64   `json:"ash_column_height_m"`
	ReportedAt       time.Time `json:"reported_at"`
}

var outage atomic.Bool
var schemaVersion atomic.Bool

func main() {
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

	// Middleware chain: correlation → logging → routes
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
	username := os.Getenv("ADMIN_USERNAME")
	password := os.Getenv("ADMIN_PASSWORD")

	u, p, ok := r.BasicAuth()

	return ok && u == username && p == password
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

		wrapped := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrapped, r)

		latency := time.Since(start)
		logJSON(map[string]any{
			"ts":         start.Format(time.RFC3339Nano),
			"corr_id":    corrID,
			"method":     r.Method,
			"path":       r.URL.Path,
			"status":     wrapped.statusCode,
			"latency_ms": latency.Milliseconds(),
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

	if schemaVersion.Load() {
		type NewReport struct {
			VolcanicReport
			ConfidenceLevel float64 `json:"confidence_level"`
		}

		var response []NewReport

		for _, report := range reports {
			response = append(response, NewReport{
				VolcanicReport:  report,
				ConfidenceLevel: 0.90 + rand.Float64()*0.10,
			})
		}

		json.NewEncoder(w).Encode(map[string]any{
			"reports": response,
		})

		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"reports": reports,
	})
}

func schemaVersionHandler(w http.ResponseWriter, r *http.Request) {
	if !adminAuth(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var request struct {
		Enabled bool `json:"enabled"`
	}

	json.NewDecoder(r.Body).Decode(&request)

	schemaVersion.Store(request.Enabled)

	json.NewEncoder(w).Encode(map[string]bool{
		"enabled": schemaVersion.Load(),
	})
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
