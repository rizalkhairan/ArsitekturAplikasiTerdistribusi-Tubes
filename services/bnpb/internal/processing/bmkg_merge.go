package processing

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pb "github.com/rizalkhairan/distributed-system/bmkg/proto"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

func mergeBMKG(ctx context.Context, db *sql.DB, events []*pb.SeismicEvent, warnings []*pb.TsunamiWarning) ([]*pb.SeismicEvent, []*pb.TsunamiWarning, error) {
	byEvent := make(map[string]*pb.SeismicEvent)
	byWarning := make(map[string]*pb.TsunamiWarning)
	affected := make(map[string]bool)
	for _, event := range events {
		byEvent[event.EventId] = event
		affected[event.EventId] = true
	}
	for _, warning := range warnings {
		affected[warning.RelatedEventId] = true
	}

	for refID := range affected {
		stored, err := database.FindHazard(ctx, db, "BMKG", refID)
		if errors.Is(err, sql.ErrNoRows) && byEvent[refID] != nil {
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("BMKG event %s: %w", refID, err)
		}
		var attributes struct {
			Magnitude        float64              `json:"magnitude"`
			DepthKM          float64              `json:"depth_km"`
			PotentialTsunami bool                 `json:"potential_tsunami"`
			Warnings         []*pb.TsunamiWarning `json:"tsunami_warnings"`
		}
		raw, err := json.Marshal(stored.Attributes)
		if err != nil {
			return nil, nil, err
		}
		if err := json.Unmarshal(raw, &attributes); err != nil {
			return nil, nil, err
		}
		if byEvent[refID] == nil {
			byEvent[refID] = &pb.SeismicEvent{
				EventId: refID, Magnitude: attributes.Magnitude, DepthKm: attributes.DepthKM,
				PotentialTsunami: attributes.PotentialTsunami,
				RegionName:       stored.AreaName, EpicenterLat: stored.Latitude, EpicenterLon: stored.Longitude,
				OccurredAt: stored.OccurredAt.Format(time.RFC3339Nano),
			}
		}
		for _, warning := range attributes.Warnings {
			byWarning[warning.WarningId] = warning
		}
	}
	for _, warning := range warnings {
		byWarning[warning.WarningId] = warning // Incoming fields replace cached fields.
	}
	mergedEvents := make([]*pb.SeismicEvent, 0, len(byEvent))
	for _, event := range byEvent {
		mergedEvents = append(mergedEvents, event)
	}
	mergedWarnings := make([]*pb.TsunamiWarning, 0, len(byWarning))
	for _, warning := range byWarning {
		mergedWarnings = append(mergedWarnings, warning)
	}
	return mergedEvents, mergedWarnings, nil
}
