package main

import "testing"

func TestValidateEvent(t *testing.T) {
	payload := []byte(`{"event_id":"evt","hazard_id":"haz","source":"PVMBG","hazard_type":"VOLCANIC","severity":"WASPADA","area_name":"Merapi","correlation_id":"corr","attributes":{"count":1}}`)
	if _, err := validateEvent(payload); err != nil {
		t.Fatalf("validateEvent() error = %v", err)
	}
	if _, err := validateEvent([]byte(`not-json`)); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestDeduperSuppressesRedeliveryWithinBound(t *testing.T) {
	deduper := newDeduper(2)
	if !deduper.addIfNew("event-1") {
		t.Fatal("first delivery should be new")
	}
	if deduper.addIfNew("event-1") {
		t.Fatal("redelivery should be suppressed")
	}
	if !deduper.addIfNew("event-2") || !deduper.addIfNew("event-3") {
		t.Fatal("new event identifiers should be processed")
	}
	if !deduper.addIfNew("event-1") {
		t.Fatal("oldest event identifier should be evicted from bounded cache")
	}
}
