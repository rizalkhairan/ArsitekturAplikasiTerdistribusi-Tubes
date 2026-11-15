package processing

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestPVMBGBreakingSchemaRecordsCanBeReprocessed(t *testing.T) {
	db := openPVMBGTestDB(t)
	var err error
	defer db.Close()
	fixtures := map[string]string{
		"rename":  `{"report_id":"pvmbg-rename","volcano_id":"merapi","alert_status":"SIAGA","reported_at":"2026-11-15T00:00:00Z","eruption_count_24h":1}`,
		"type":    `{"report_id":"pvmbg-type","volcano_id":"merapi","alert_level":{"level":"AWAS"},"reported_at":"2026-11-15T00:00:00Z","eruption_count_24h":2}`,
		"missing": `{"volcano_id":"merapi","alert_level":"WASPADA","reported_at":"2026-11-15T00:00:00Z","eruption_count_24h":3}`,
	}
	for name, raw := range fixtures {
		if _, err := volcanicHazard(context.Background(), db, []byte(raw)); err == nil {
			t.Errorf("strict v1 mapper accepted %s schema", name)
		}
		event, err := MapVolcanicReport(context.Background(), db, []byte(raw), "pvmbg-v2")
		if err != nil {
			t.Errorf("v2 mapper rejected %s schema: %v", name, err)
			continue
		}
		if event.Source != "PVMBG" || event.HazardType != "VOLCANIC" || event.SourceRefID == "" {
			t.Errorf("invalid canonical event for %s: %+v", name, event)
		}
		if name == "rename" && event.Severity != "SIAGA" {
			t.Errorf("rename mapped severity=%s", event.Severity)
		}
		if name == "type" && event.Severity != "AWAS" {
			t.Errorf("type-change mapped severity=%s", event.Severity)
		}
		if name == "missing" && event.SourceRefID == "pvmbg-missing" {
			t.Error("missing report_id must get deterministic derived identity")
		}
	}
	first, err := MapVolcanicReport(context.Background(), db, []byte(fixtures["missing"]), "pvmbg-v2")
	if err != nil {
		t.Fatal(err)
	}
	second, err := MapVolcanicReport(context.Background(), db, []byte(fixtures["missing"]), "pvmbg-v2")
	if err != nil {
		t.Fatal(err)
	}
	if first.SourceRefID != second.SourceRefID {
		t.Fatalf("replay identity changed: %s != %s", first.SourceRefID, second.SourceRefID)
	}
}

func TestPVMBGUnknownAdditiveFieldIsPreserved(t *testing.T) {
	db := openPVMBGTestDB(t)
	defer db.Close()
	raw := []byte(`{"report_id":"pvmbg-additive","volcano_id":"merapi","alert_level":"WASPADA","reported_at":"2026-11-15T00:00:00Z","eruption_count_24h":2,"confidence_level":0.97}`)
	event, err := MapVolcanicReport(context.Background(), db, raw, "pvmbg-v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := event.Attributes["confidence_level"]; !ok {
		t.Fatalf("additive field missing from attributes: %+v", event.Attributes)
	}
}

func openPVMBGTestDB(t *testing.T) *sql.DB {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", "-")
	db, err := sql.Open("sqlite", "file:"+name+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE volcano_references (volcano_id TEXT PRIMARY KEY, area_name TEXT NOT NULL, latitude REAL NOT NULL, longitude REAL NOT NULL); INSERT INTO volcano_references VALUES ('merapi','Gunung Merapi',-7.54,110.446);`); err != nil {
		t.Fatal(err)
	}
	return db
}
