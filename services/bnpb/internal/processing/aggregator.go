package processing

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	pb "github.com/rizalkhairan/distributed-system/bmkg/proto"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

type Batch struct {
	Hazards  []database.HazardEvent
	Cursors  database.Cursors
	Failures []FailedMessage
}

// FailedMessage retains an upstream record that failed mapping so the caller can
// durably park the original bytes before advancing its source cursor.
type FailedMessage struct {
	SourceRefID string
	Raw         []byte
	Reason      string
	Retryable   bool
	ReceivedAt  time.Time
}

// ErrNotFound lets a storage-backed lookup signal "no prior projection yet".
var ErrNotFound = errors.New("not found")

// Lookup retrieves a previously stored hazard for merge-on-warning.
type Lookup func(ctx context.Context, source, refID string) (database.HazardEvent, error)

func AggregateBMKG(ctx context.Context, db *sql.DB, events []*pb.SeismicEvent, warnings []*pb.TsunamiWarning, cursors database.Cursors) (Batch, error) {
	return AggregateBMKGWithLookup(ctx, func(ctx context.Context, source, refID string) (database.HazardEvent, error) {
		return database.FindHazard(ctx, db, source, refID)
	}, events, warnings, cursors)
}

func AggregateBMKGWithLookup(ctx context.Context, lookup Lookup, events []*pb.SeismicEvent, warnings []*pb.TsunamiWarning, cursors database.Cursors) (Batch, error) {
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
	events, warnings, err := mergeBMKGWithLookup(ctx, lookup, events, warnings)
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
	failed := make([]FailedMessage, 0)
	for i, raw := range reports {
		refID, receivedAt := pvmbgIdentityAndTime(raw)
		if !receivedAt.IsZero() {
			if err := next.Advance(database.VolcanicFeed, receivedAt.Format(time.RFC3339Nano)); err != nil {
				return Batch{}, err
			}
		}
		hazard, err := volcanicHazard(ctx, db, raw)
		if err != nil {
			if refID == "" {
				refID = fmt.Sprintf("unidentified-%d", i)
			}
			failed = append(failed, FailedMessage{SourceRefID: refID, Raw: append([]byte(nil), raw...), Reason: "schema_incompatible: " + err.Error(), Retryable: false, ReceivedAt: receivedAt})
			continue
		}
		hazards = append(hazards, hazard)
	}
	return Batch{Hazards: hazards, Cursors: next, Failures: failed}, nil
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
	if strings.TrimSpace(report.ReportID) == "" {
		return database.HazardEvent{}, fmt.Errorf("required field report_id is missing or empty")
	}
	if strings.TrimSpace(report.VolcanoID) == "" {
		return database.HazardEvent{}, fmt.Errorf("required field volcano_id is missing or empty")
	}
	if strings.TrimSpace(report.AlertLevel) == "" {
		return database.HazardEvent{}, fmt.Errorf("required field alert_level is missing or empty")
	}
	if report.ReportedAt.IsZero() {
		return database.HazardEvent{}, fmt.Errorf("required field reported_at is missing or invalid")
	}
	report.AlertLevel = strings.ToUpper(strings.TrimSpace(report.AlertLevel))
	if report.AlertLevel != "NORMAL" && report.AlertLevel != "WASPADA" && report.AlertLevel != "SIAGA" && report.AlertLevel != "AWAS" {
		return database.HazardEvent{}, fmt.Errorf("unsupported alert_level %q", report.AlertLevel)
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

// MapVolcanicReport maps a quarantined report during an explicitly authorized
// reprocess. v1 is strict; v2 accepts the documented PVMBG rename/type/removal
// compatibility changes and gives records without report_id a stable raw hash ID.
func MapVolcanicReport(ctx context.Context, db *sql.DB, raw json.RawMessage, version string) (database.HazardEvent, error) {
	if version == "" || version == "pvmbg-v1" {
		return volcanicHazard(ctx, db, raw)
	}
	if version != "pvmbg-v2" {
		return database.HazardEvent{}, fmt.Errorf("unsupported PVMBG processor version %q", version)
	}
	if !json.Valid(raw) {
		return database.HazardEvent{}, fmt.Errorf("invalid JSON report")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return database.HazardEvent{}, err
	}
	var reportID, volcanoID, alertLevel, reportedAt string
	_ = json.Unmarshal(fields["report_id"], &reportID)
	_ = json.Unmarshal(fields["volcano_id"], &volcanoID)
	if len(fields["alert_level"]) > 0 {
		_ = json.Unmarshal(fields["alert_level"], &alertLevel)
	}
	if alertLevel == "" && len(fields["alert_status"]) > 0 {
		_ = json.Unmarshal(fields["alert_status"], &alertLevel)
	}
	if alertLevel == "" && len(fields["alert_level"]) > 0 {
		var wrapped struct {
			Level string `json:"level"`
		}
		if err := json.Unmarshal(fields["alert_level"], &wrapped); err == nil {
			alertLevel = wrapped.Level
		}
	}
	_ = json.Unmarshal(fields["reported_at"], &reportedAt)
	if reportID == "" {
		sum := sha256.Sum256(raw)
		reportID = "pvmbg-v2-" + hex.EncodeToString(sum[:])
	}
	if volcanoID == "" || alertLevel == "" || reportedAt == "" {
		return database.HazardEvent{}, fmt.Errorf("required PVMBG fields are still missing")
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, reportedAt)
	if err != nil {
		return database.HazardEvent{}, fmt.Errorf("reported_at: %w", err)
	}
	alertLevel = strings.ToUpper(strings.TrimSpace(alertLevel))
	if alertLevel != "NORMAL" && alertLevel != "WASPADA" && alertLevel != "SIAGA" && alertLevel != "AWAS" {
		return database.HazardEvent{}, fmt.Errorf("unsupported alert level %q", alertLevel)
	}
	volcano, err := database.FindVolcano(ctx, db, volcanoID)
	if err != nil {
		return database.HazardEvent{}, fmt.Errorf("volcano reference %q: %w", volcanoID, err)
	}
	attributes := make(map[string]any, len(fields))
	for key, value := range fields {
		if key == "report_id" || key == "volcano_id" || key == "alert_level" || key == "alert_status" || key == "reported_at" {
			continue
		}
		var decoded any
		if err := json.Unmarshal(value, &decoded); err != nil {
			return database.HazardEvent{}, fmt.Errorf("attribute %s: %w", key, err)
		}
		attributes[key] = decoded
	}
	return database.HazardEvent{Source: "PVMBG", SourceRefID: reportID, HazardType: "VOLCANIC", Severity: alertLevel, AreaName: volcano.AreaName, Latitude: volcano.Latitude, Longitude: volcano.Longitude, OccurredAt: occurredAt, Attributes: attributes}, nil
}

func pvmbgIdentityAndTime(raw []byte) (string, time.Time) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		sum := sha256.Sum256(raw)
		return "raw-" + hex.EncodeToString(sum[:]), time.Time{}
	}
	var id string
	_ = json.Unmarshal(fields["report_id"], &id)
	if id == "" {
		sum := sha256.Sum256(raw)
		id = "pvmbg-v2-" + hex.EncodeToString(sum[:])
	}
	var at time.Time
	_ = json.Unmarshal(fields["reported_at"], &at)
	return id, at
}
