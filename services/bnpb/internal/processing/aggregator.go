package processing

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	pb "github.com/rizalkhairan/distributed-system/bmkg/proto"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

type Batch struct {
	Hazards []database.HazardEvent
	Cursors database.Cursors
}

func AggregateBMKG(ctx context.Context, db *sql.DB, events []*pb.SeismicEvent, warnings []*pb.TsunamiWarning, cursors database.Cursors) (Batch, error) {
	next := database.Cursors{
		database.SeismicFeed: cursors[database.SeismicFeed],
		database.TsunamiFeed: cursors[database.TsunamiFeed],
	}
	for _, event := range events {
		if err := next.Advance(database.SeismicFeed, event.OccurredAt); err != nil {
			return Batch{}, err
		}
	}
	for _, warning := range warnings {
		if err := next.Advance(database.TsunamiFeed, warning.EstimatedArrival); err != nil {
			return Batch{}, err
		}
	}
	
	// A warning can arrive for an event older than the seismic cursor.
	events, warnings, err := mergeBMKG(ctx, db, events, warnings)
	if err != nil {
		return Batch{}, err
	}
	byEvent := make(map[string][]*pb.TsunamiWarning)
	for _, warning := range warnings {
		byEvent[warning.RelatedEventId] = append(byEvent[warning.RelatedEventId], warning)
	}

	hazards := make([]database.HazardEvent, 0, len(events))
	var failures []error
	for _, event := range events {
		hazard, err := seismicHazard(event, byEvent[event.EventId])
		if err != nil {
			failures = append(failures, fmt.Errorf("event %s: %w", event.EventId, err))
			continue
		}
		hazards = append(hazards, hazard)
	}
	return Batch{Hazards: hazards, Cursors: next}, errors.Join(failures...)
}

func seismicHazard(event *pb.SeismicEvent, warnings []*pb.TsunamiWarning) (database.HazardEvent, error) {
	occurredAt, err := time.Parse(time.RFC3339Nano, event.OccurredAt)
	if err != nil {
		return database.HazardEvent{}, err
	}
	severity := "NORMAL"
	if event.Magnitude >= 6.5 {
		severity = "SIAGA"
	} else if event.Magnitude >= 5 {
		severity = "WASPADA"
	}
	attributes := map[string]any{
		"magnitude": event.Magnitude, "depth_km": event.DepthKm,
		"potential_tsunami": event.PotentialTsunami,
	}
	if len(warnings) > 0 {
		// Warning severity overrides magnitude; the highest related warning wins.
		ranks := map[string]int{"NORMAL": 0, "WASPADA": 1, "SIAGA": 2, "AWAS": 3}
		severity = "NORMAL"
		for _, warning := range warnings {
			level := strings.ToUpper(warning.ThreatLevel)
			rank, ok := ranks[level]
			if !ok {
				return database.HazardEvent{}, fmt.Errorf("unknown threat level %q", warning.ThreatLevel)
			}
			if rank > ranks[severity] {
				severity = level
			}
		}
		attributes["tsunami_warnings"] = warnings
	}
	return database.HazardEvent{
		Source: "BMKG", SourceRefID: event.EventId, HazardType: "SEISMIC",
		Severity: severity, AreaName: event.RegionName,
		Latitude: event.EpicenterLat, Longitude: event.EpicenterLon,
		OccurredAt: occurredAt, Attributes: attributes,
	}, nil
}

// AggregatePVMBG enriches reports with volcano references and maps canonical hazards.
func AggregatePVMBG(ctx context.Context, db *sql.DB, reports []json.RawMessage, cursors database.Cursors) (Batch, error) {
	next := database.Cursors{database.VolcanicFeed: cursors[database.VolcanicFeed]}
	hazards := make([]database.HazardEvent, 0, len(reports))
	var failures []error
	for i, raw := range reports {
		hazard, err := volcanicHazard(ctx, db, raw)
		if err == nil {
			err = next.Advance(database.VolcanicFeed, hazard.OccurredAt.Format(time.RFC3339Nano))
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("report %d: %w", i, err))
			continue
		}
		hazards = append(hazards, hazard)
	}
	return Batch{Hazards: hazards, Cursors: next}, errors.Join(failures...)
}

func volcanicHazard(ctx context.Context, db *sql.DB, raw json.RawMessage) (database.HazardEvent, error) {
	var report struct {
		ReportID   string    `json:"report_id"`
		VolcanoID  string    `json:"volcano_id"`
		AlertLevel string    `json:"alert_level"`
		ReportedAt time.Time `json:"reported_at"`
	}
	if err := json.Unmarshal(raw, &report); err != nil {
		return database.HazardEvent{}, err
	}
	volcano, err := database.FindVolcano(ctx, db, report.VolcanoID)
	if err != nil {
		return database.HazardEvent{}, fmt.Errorf("volcano reference %q: %w", report.VolcanoID, err)
	}
	// Keep every field without a direct canonical equivalent, including new fields.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return database.HazardEvent{}, err
	}
	for _, key := range []string{"report_id", "alert_level", "reported_at"} {
		delete(fields, key)
	}
	attributes := make(map[string]any, len(fields))
	for key, value := range fields {
		attributes[key] = value
	}
	return database.HazardEvent{
		Source: "PVMBG", SourceRefID: report.ReportID, HazardType: "VOLCANIC",
		Severity: strings.ToUpper(report.AlertLevel), AreaName: volcano.AreaName,
		Latitude: volcano.Latitude, Longitude: volcano.Longitude,
		OccurredAt: report.ReportedAt, Attributes: attributes,
	}, nil
}
