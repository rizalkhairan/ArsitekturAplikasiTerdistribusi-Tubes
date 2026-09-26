package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

type VolcanicReport struct {
	ReportID         string    `json:"report_id"`
	VolcanoID        string    `json:"volcano_id"`
	AlertLevel       string    `json:"alert_level"`
	EruptionCount24h int       `json:"eruption_count_24h"`
	AshColumnHeightM float64   `json:"ash_column_height_m"`
	ReportedAt       time.Time `json:"reported_at"`
}

var outage bool
var schemaVersion bool

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

	http.HandleFunc("/health", health)
	http.HandleFunc("/volcanic-reports", volcanicReports)
	http.HandleFunc("/admin/schema-version", schemaVersionHandler)
	http.HandleFunc("/admin/outage", outageHandler)

	log.Printf("PVMBG listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
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

	if outage {
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

	if schemaVersion {
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

	schemaVersion = request.Enabled

	json.NewEncoder(w).Encode(map[string]bool{
		"enabled": schemaVersion,
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

	outage = request.Enabled

	json.NewEncoder(w).Encode(map[string]bool{
		"enabled": outage,
	})
}
