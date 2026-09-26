package main

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	pb "github.com/rizalkhairan/distributed-system/bmkg/proto"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

var db *sql.DB

func openDatabase(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	database.SetMaxOpenConns(1)
	if _, err := database.Exec(schema); err != nil {
		database.Close()
		return nil, err
	}
	log.Println("SQLite initialized")
	return database, nil
}

func insertEvent(event *pb.SeismicEvent) {
	_, err := db.Exec(`
		INSERT INTO seismic_events (
			event_id, magnitude, depth_km, epicenter_lat, epicenter_lon,
			region_name, occurred_at, potential_tsunami
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, event.EventId, event.Magnitude, event.DepthKm, event.EpicenterLat,
		event.EpicenterLon, event.RegionName, event.OccurredAt, event.PotentialTsunami)
	if err != nil {
		log.Println(err)
		return
	}

	if event.PotentialTsunami {
		levels := []string{"Waspada", "Siaga", "Awas"}
		zones, _ := json.Marshal([]string{event.RegionName})
		occurredAt, _ := time.Parse(time.RFC3339, event.OccurredAt)

		_, err := db.Exec(`
			INSERT INTO tsunami_warnings (
				warning_id, related_event_id, threat_level, affected_zones, estimated_arrival
			) VALUES (?, ?, ?, ?, ?)
		`, "tsunami-"+event.EventId, event.EventId, levels[rand.Intn(len(levels))],
			string(zones), occurredAt.Add(30*time.Minute).Format(time.RFC3339))
		if err != nil {
			log.Println(err)
		}
	}
}

func countEvents() (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM seismic_events").Scan(&count)
	return count, err
}

func listSeismicEvents(ctx context.Context, since string) ([]*pb.SeismicEvent, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT event_id, magnitude, depth_km, epicenter_lat, epicenter_lon,
		       region_name, occurred_at, potential_tsunami
		FROM seismic_events WHERE occurred_at >= ? ORDER BY occurred_at
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []*pb.SeismicEvent
	for rows.Next() {
		event := &pb.SeismicEvent{}
		if err := rows.Scan(&event.EventId, &event.Magnitude, &event.DepthKm,
			&event.EpicenterLat, &event.EpicenterLon, &event.RegionName,
			&event.OccurredAt, &event.PotentialTsunami); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func listTsunamiWarnings(ctx context.Context, since string) ([]*pb.TsunamiWarning, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT warning_id, related_event_id, threat_level, affected_zones, estimated_arrival
		FROM tsunami_warnings WHERE estimated_arrival >= ? ORDER BY estimated_arrival
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var warnings []*pb.TsunamiWarning
	for rows.Next() {
		warning := &pb.TsunamiWarning{}
		var zones string
		if err := rows.Scan(&warning.WarningId, &warning.RelatedEventId,
			&warning.ThreatLevel, &zones, &warning.EstimatedArrival); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(zones), &warning.AffectedZones); err != nil {
			return nil, err
		}
		warnings = append(warnings, warning)
	}
	return warnings, rows.Err()
}
