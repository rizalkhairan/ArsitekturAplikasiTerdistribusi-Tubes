package main

import "testing"

func TestValidateEvent(t *testing.T) {
	valid := []byte(`{"event_id":"evt","hazard_id":"haz","hazard_type":"VOLCANIC","severity":"AWAS","area_name":"Merapi","correlation_id":"corr","attributes":{}}`)
	if _, err := validateEvent(valid); err != nil {
		t.Fatalf("validateEvent() error = %v", err)
	}
	if _, err := validateEvent([]byte(`{"event_id":"evt","attributes":{}}`)); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestDeduperSuppressesRedelivery(t *testing.T) {
	deduper := newDeduper(2)
	if !deduper.addIfNew("evt") {
		t.Fatal("first delivery should be new")
	}
	if deduper.addIfNew("evt") {
		t.Fatal("redelivery should be suppressed")
	}
}
