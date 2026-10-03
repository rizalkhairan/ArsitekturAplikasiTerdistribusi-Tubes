package filter

import (
	"encoding/json"
	"log"
)

// SummaryFields are the only fields visible to media-scope clients.
var SummaryFields = []string{
	"hazard_id", "source", "hazard_type", "severity", "area_name", "occurred_at",
}

// FullFields include everything (field/internal scope).
var FullFields = []string{
	"hazard_id", "source", "source_ref_id", "hazard_type", "severity",
	"area_name", "latitude", "longitude", "occurred_at", "ingested_at", "attributes",
}

// FilterHazard strips fields based on scope. Media gets summary only.
func FilterHazard(hazard map[string]any, scope string) map[string]any {
	allowed := SummaryFields
	if scope == "full" {
		allowed = FullFields
	}

	filtered := make(map[string]any)
	for _, field := range allowed {
		if val, ok := hazard[field]; ok {
			filtered[field] = val
		}
	}
	return filtered
}

// FilterHazards applies FilterHazard to a slice.
func FilterHazards(hazards []map[string]any, scope string) []map[string]any {
	result := make([]map[string]any, len(hazards))
	for i, h := range hazards {
		result[i] = FilterHazard(h, scope)
	}
	return result
}

// ToMap converts a HazardEvent to a map for filtering.
func ToMap(hazard any) map[string]any {
	data, err := json.Marshal(hazard)
	if err != nil {
		log.Printf("filter: marshal error: %v", err)
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		log.Printf("filter: unmarshal error: %v", err)
		return map[string]any{}
	}
	return m
}
