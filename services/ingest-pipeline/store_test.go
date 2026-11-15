package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestParkPreservesRawBytesAndDeduplicates(t *testing.T) {
	s, err := openStore("file:" + strings.ReplaceAll(t.Name(), "/", "-") + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	raw := []byte{'{', '"', 'x', '"', ':', 0xff, '}'}
	m := failedMessage{MessageID: "m1", Source: "PVMBG", SourceRefID: "r1", ReceivedAt: time.Date(2026, 11, 15, 1, 2, 3, 0, time.UTC), Reason: "invalid_utf8", Raw: raw}
	created, err := s.park(context.Background(), m)
	if err != nil || !created {
		t.Fatalf("park()=(%v,%v)", created, err)
	}
	created, err = s.park(context.Background(), m)
	if err != nil || created {
		t.Fatalf("duplicate park()=(%v,%v)", created, err)
	}
	got, err := s.getFailure(context.Background(), "m1")
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Raw) != string(raw) {
		t.Fatalf("raw bytes changed: %v != %v", got.Raw, raw)
	}
	if got.Status != "parked" || got.Retryable {
		t.Fatalf("unexpected status: %+v", got)
	}
}

func TestRecoveredMessagesDisappearAndReplayOrderIsGuarded(t *testing.T) {
	s, err := openStore("file:" + strings.ReplaceAll(t.Name(), "/", "-") + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	ctx := context.Background()
	at := time.Date(2026, 11, 15, 1, 0, 0, 0, time.UTC)
	first := failedMessage{MessageID: "a", Source: "PVMBG", SourceRefID: "same", ReceivedAt: at, Reason: "schema", Raw: []byte(`{}`)}
	second := failedMessage{MessageID: "b", Source: "PVMBG", SourceRefID: "same", ReceivedAt: at.Add(time.Second), Reason: "schema", Raw: []byte(`{}`)}
	if _, err := s.park(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := s.park(ctx, second); err != nil {
		t.Fatal(err)
	}
	blocked, err := s.hasEarlierPending(ctx, second)
	if err != nil || !blocked {
		t.Fatalf("second replay should be blocked: %v %v", blocked, err)
	}
	if err := s.markRecovered(ctx, first.MessageID); err != nil {
		t.Fatal(err)
	}
	blocked, err = s.hasEarlierPending(ctx, second)
	if err != nil || blocked {
		t.Fatalf("second replay should be unblocked: %v %v", blocked, err)
	}
	items, hasMore, err := s.list(ctx, "PVMBG", "", "", 10)
	if err != nil || hasMore || len(items) != 1 || items[0].MessageID != "b" {
		t.Fatalf("unprocessed list mismatch items=%+v hasMore=%v err=%v", items, hasMore, err)
	}
}

func TestFieldReportProcessingIsDurableAndIdempotent(t *testing.T) {
	s, err := openStore("file:" + strings.ReplaceAll(t.Name(), "/", "-") + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	payload := []byte(`{"description":"water rising"}`)
	inserted, err := s.markReportProcessed(context.Background(), "key-1", "receipt-1", payload)
	if err != nil || !inserted {
		t.Fatalf("first process=(%v,%v)", inserted, err)
	}
	inserted, err = s.markReportProcessed(context.Background(), "key-1", "receipt-1", payload)
	if err != nil || inserted {
		t.Fatalf("redelivery process=(%v,%v)", inserted, err)
	}
	var stored []byte
	if err := s.db.QueryRow(`SELECT payload FROM processed_field_reports WHERE idempotency_key=?`, "key-1").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if string(stored) != string(payload) {
		t.Fatalf("processed report not durably retained: %s", stored)
	}
}

func TestFieldReportIntakeUsesStableIdempotencyReceipt(t *testing.T) {
	s, err := openStore("file:" + strings.ReplaceAll(t.Name(), "/", "-") + "?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer s.db.Close()
	ctx := context.Background()
	body := []byte(`{"description":"water rising"}`)
	first := []byte(`{"message_id":"m1","receipt_id":"r1"}`)
	retry := []byte(`{"message_id":"m2","receipt_id":"r2"}`)
	receipt, err := s.acceptReport(ctx, "key-1", "r1", "c1", first, body)
	if err != nil || receipt != "r1" {
		t.Fatalf("first intake=(%q,%v)", receipt, err)
	}
	receipt, err = s.acceptReport(ctx, "key-1", "r2", "c2", retry, body)
	if err != nil || receipt != "r1" {
		t.Fatalf("retry intake=(%q,%v), want original receipt", receipt, err)
	}
	if _, err = s.acceptReport(ctx, "key-1", "r3", "c3", retry, []byte(`{"description":"different"}`)); err == nil {
		t.Fatal("expected changed-body idempotency conflict")
	}
	stored, published, err := s.reportOutbox(ctx, "key-1")
	if err != nil || published || string(stored) != string(first) {
		t.Fatalf("outbox retry changed the envelope: %s published=%v err=%v", stored, published, err)
	}
}

func TestJSONDepthAndUTF8Validation(t *testing.T) {
	valid := []byte(`{"a":[1,{"b":2}]}`)
	depth, err := jsonDepth(valid)
	if err != nil || depth != 3 {
		t.Fatalf("jsonDepth=(%d,%v), want 3,nil", depth, err)
	}
	tooDeep := []byte("[[[[[0]]]]]")
	depth, err = jsonDepth(tooDeep)
	if err != nil || depth != 5 {
		t.Fatalf("jsonDepth=(%d,%v), want 5,nil", depth, err)
	}
	if _, err = jsonDepth([]byte(`{"a":`)); err == nil {
		t.Fatal("expected incomplete JSON error")
	}
}
