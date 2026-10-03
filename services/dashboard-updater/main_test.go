package main

import "testing"

func TestValidateEvent(t *testing.T) {
	payload := []byte(`{"event_id":"evt-1","hazard_id":"haz-1","source":"BMKG","hazard_type":"SEISMIC","severity":"SIAGA","area_name":"Jakarta","correlation_id":"corr-1","attributes":{}}`)
	if _, err := validateEvent(payload); err != nil {
		t.Fatalf("validateEvent() error = %v", err)
	}
	if _, err := validateEvent([]byte(`{"event_id":"evt-1"}`)); err == nil {
		t.Fatal("validateEvent() error = nil, want missing required fields error")
	}
}

func TestDeduperIsBoundedAndSuppressesDuplicates(t *testing.T) {
	deduper := newDeduper(2)
	if !deduper.addIfNew("one") || !deduper.addIfNew("two") {
		t.Fatal("expected initial event identifiers to be new")
	}
	if deduper.addIfNew("one") {
		t.Fatal("expected duplicate event identifier to be suppressed")
	}
	if !deduper.addIfNew("three") {
		t.Fatal("expected third event identifier to be new")
	}
	if !deduper.addIfNew("one") {
		t.Fatal("expected oldest identifier to be evicted from the bounded cache")
	}
}
