package canonical

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

)

type fakeShards struct {
	units        []unit
	rows         map[int64][]copyRow
	fail         map[int64]bool
	delay        time.Duration
	mu           sync.Mutex
	active, peak int
	changed      bool
	calls        int
	all          []copyRow
	allOK        bool
}

func fixtureShards() *fakeShards {
	return &fakeShards{units: []unit{{1, -2147483648, -1}, {2, 0, 1073741823}, {3, 1073741824, 2147483647}}, rows: map[int64][]copyRow{}, fail: map[int64]bool{}}
}
func (f *fakeShards) inventory(context.Context) ([]unit, error) {
	f.calls++
	if f.changed && f.calls > 1 {
		return nil, errors.New("metadata unavailable")
	}
	return f.units, nil
}
func (f *fakeShards) readUnit(ctx context.Context, u unit, _ Query) ([]copyRow, error) {
	f.mu.Lock()
	f.active++
	if f.active > f.peak {
		f.peak = f.active
	}
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.active--; f.mu.Unlock() }()
	if f.fail[u.ID] {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	select {
	case <-time.After(f.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return f.rows[u.ID], nil
}
func (f *fakeShards) readAllPage(_ context.Context, _ Query, pos *cursorPos, limit int) ([]copyRow, error) {
	if !f.allOK {
		return nil, errors.New("distributed read unsupported")
	}
	rows := append([]copyRow(nil), f.all...)
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Event.IngestedAt.Equal(rows[j].Event.IngestedAt) {
			return rows[i].Event.HazardID < rows[j].Event.HazardID
		}
		return rows[i].Event.IngestedAt.Before(rows[j].Event.IngestedAt)
	})
	out := []copyRow{}
	for _, row := range rows {
		if pos == nil || row.Event.IngestedAt.After(pos.At) ||
			(row.Event.IngestedAt.Equal(pos.At) && row.Event.HazardID > pos.ID) {
			out = append(out, row)
		}
		if len(out) == limit {
			break
		}
	}
	return out, nil
}
func (f *fakeShards) readAll(context.Context, Query) ([]copyRow, error) {
	if !f.allOK {
		return nil, errors.New("distributed read unsupported")
	}
	return f.all, nil
}
func row(id string, version int64, operation string) copyRow {
	return copyRow{version, operation, Event{HazardID: id, Source: "SATELIT", SourceRefID: id, IngestedAt: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), Attributes: map[string]any{}}}
}

func TestGatherVersionsTombstonesAndOrdering(t *testing.T) {
	f := fixtureShards()
	old := row("a", 1, "upsert")
	latest := row("a", 2, "upsert")
	latest.Event.Severity = "AWAS"
	f.rows[1] = []copyRow{row("z", 1, "upsert"), old, latest, row("removed", 1, "upsert")}
	f.rows[2] = []copyRow{row("b", 1, "upsert"), latest, row("removed", 2, "retract")}
	for i := 0; i < 10; i++ {
		events, missing, err := gather(context.Background(), f, Query{})
		if err != nil || len(missing) != 0 {
			t.Fatalf("gather: %v %v", missing, err)
		}
		events = selectEvents(events, Query{})
		if len(events) != 3 || events[0].HazardID != "a" || events[1].HazardID != "b" || events[2].HazardID != "z" || events[0].Severity != "AWAS" {
			t.Fatalf("merge: %+v", events)
		}
	}
	f.rows[3] = []copyRow{row("a", 2, "upsert")}
	_, _, err := gather(context.Background(), f, Query{})
	requireCode(t, err, "inconsistent_copies")
	// A newer copy arriving first cannot hide conflicts between older copies.
	conflicting := old
	conflicting.Event.Severity = "NORMAL"
	for _, copies := range [][]copyRow{{old, conflicting, latest}, {latest, old, conflicting}} {
		f = fixtureShards()
		f.rows[1] = copies
		_, _, err = gather(context.Background(), f, Query{})
		requireCode(t, err, "inconsistent_copies")
	}
}

func TestGatherTimeoutCoverageAndRecovery(t *testing.T) {
	f := fixtureShards()
	f.rows[1] = []copyRow{row("a", 1, "upsert")}
	f.fail[2] = true
	start := time.Now()
	events, missing, err := gather(context.Background(), f, Query{})
	if err != nil || len(events) != 1 || !reflect.DeepEqual(missing, []string{"shard-2"}) {
		t.Fatalf("partial: %+v %v %v", events, missing, err)
	}
	if time.Since(start) > 2*unitTimeout || f.peak > readWorkers {
		t.Fatalf("unbounded read: %v, peak %d", time.Since(start), f.peak)
	}
	f.fail[2] = false
	f.rows[2] = []copyRow{row("b", 1, "upsert")}
	events, missing, err = gather(context.Background(), f, Query{})
	if err != nil || len(events) != 2 || len(missing) != 0 {
		t.Fatalf("recovery: %v %v", missing, err)
	}
	f.fail = map[int64]bool{1: true, 2: true, 3: true}
	events, missing, err = gather(context.Background(), f, Query{})
	if err != nil || len(events) != 0 || len(missing) != 3 {
		t.Fatalf("total outage: %v %v", missing, err)
	}
}

func TestGatherConcurrencyAndExactSourceIdentity(t *testing.T) {
	f := fixtureShards()
	f.units = nil
	f.delay = 20 * time.Millisecond
	for i := int64(0); i < 8; i++ {
		start := -2147483648 + i*536870912
		f.units = append(f.units, unit{i + 1, start, start + 536870911})
	}
	a, b := row("a", 1, "upsert"), row("b", 1, "upsert")
	a.Event.SourceRefID = "same-ref"
	b.Event.SourceRefID = "same-ref"
	b.Event.Source = "BMKG"
	b.Event.IngestedAt = b.Event.IngestedAt.In(time.FixedZone("offset", 7*3600))
	f.rows[1] = []copyRow{a}
	f.rows[8] = []copyRow{b}
	events, missing, err := gather(context.Background(), f, Query{})
	if err != nil || len(missing) != 0 || len(events) != 2 || f.peak != len(f.units) {
		t.Fatalf("concurrency/source identity: events=%d peak=%d %v %v", len(events), f.peak, missing, err)
	}
	events = selectEvents(events, Query{})
	if events[0].HazardID != "a" || events[1].HazardID != "b" {
		t.Fatal("equal instants did not use ID tie-breaker")
	}
}

// The distributed fast path must produce the same fold as the per-shard walk
// and must not touch shard metadata when it succeeds.
func TestGatherDistributedFastPath(t *testing.T) {
	f := fixtureShards()
	f.allOK = true
	f.all = []copyRow{row("a", 1, "upsert"), row("b", 2, "upsert"), row("gone", 1, "upsert"), row("gone", 2, "retract")}
	events, missing, err := gather(context.Background(), f, Query{})
	if err != nil || len(missing) != 0 || len(events) != 2 {
		t.Fatalf("distributed read: events=%d missing=%v err=%v", len(events), missing, err)
	}
	if f.calls != 0 {
		t.Fatalf("fast path read shard metadata %d times", f.calls)
	}
}

// A page reads only the rows after the cursor, and the extra row tells the
// caller that another page exists.
func TestGatherPageKeysetPagination(t *testing.T) {
	f := fixtureShards()
	f.allOK = true
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	mk := func(id string, minute int) copyRow {
		r := row(id, 1, "upsert")
		r.Event.IngestedAt = base.Add(time.Duration(minute) * time.Minute)
		return r
	}
	f.all = []copyRow{mk("a", 1), mk("b", 2), mk("c", 3)}

	first, _, err := gatherPage(context.Background(), f, Query{Limit: 2}, nil)
	if err != nil || len(first) != 3 {
		t.Fatalf("first page should carry one lookahead row: %d %v", len(first), err)
	}
	pos := cursorPos{At: first[1].IngestedAt, ID: first[1].HazardID}
	next, _, err := gatherPage(context.Background(), f, Query{Limit: 2}, &pos)
	if err != nil || len(next) != 1 || next[0].HazardID != "c" {
		t.Fatalf("second page: %v %v", next, err)
	}
	encoded := encodeCursor(first[1])
	decoded, err := decodeCursor(encoded)
	if err != nil || !decoded.At.Equal(pos.At) || decoded.ID != "b" {
		t.Fatalf("cursor round trip: %+v %v", decoded, err)
	}
}

func TestGatherRejectsUnknownCoverageAndCaps(t *testing.T) {
	f := fixtureShards()
	f.units = f.units[:2]
	_, _, err := gather(context.Background(), f, Query{})
	requireCode(t, err, "coverage_unavailable")
	f = fixtureShards()
	f.changed = true
	_, _, err = gather(context.Background(), f, Query{})
	requireCode(t, err, "coverage_changed")
	f = fixtureShards()
	large := row("large", 1, "upsert")
	large.Event.Attributes["payload"] = strings.Repeat("x", maxReadBytes+1)
	f.rows[1] = []copyRow{large}
	_, _, err = gather(context.Background(), f, Query{})
	requireCode(t, err, "snapshot_too_large")
}

func TestCursorValidation(t *testing.T) {
	for _, cursor := range []string{"garbage", strings.Repeat("a", 513)} {
		if _, err := decodeCursor(cursor); err == nil {
			t.Fatal("invalid cursor accepted")
		}
	}
	event := row("a", 1, "upsert").Event
	cluster := event.IngestedAt.Add(90 * time.Minute)
	decoded, err := decodeCursor(encodeCursor(Event{IngestedAt: cluster, HazardID: "hz-1"}))
	if err != nil || !decoded.At.Equal(cluster) || decoded.ID != "hz-1" {
		t.Fatalf("cursor round trip: %+v %v", decoded, err)
	}
}

func TestQueryValidationAndInclusiveIngestionFilter(t *testing.T) {
	for _, q := range []Query{{Source: "unknown"}, {Since: "yesterday"}, {Limit: -1}, {Limit: 1001}} {
		if q.validate() == nil {
			t.Fatal("invalid query accepted", q)
		}
	}
	q := Query{Since: "2026-11-01T07:00:00+07:00"}
	if err := q.validate(); err != nil || q.Since != "2026-11-01T00:00:00Z" || q.Limit != 100 {
		t.Fatal(q, err)
	}
	events := selectEvents([]Event{row("a", 1, "upsert").Event}, q)
	if len(events) != 1 {
		t.Fatal("inclusive ingestion bound rejected")
	}
	for _, cursor := range []string{"garbage", strings.Repeat("a", 513)} {
		if _, err := decodeCursor(cursor); err == nil {
			t.Fatal("invalid cursor accepted")
		}
	}
}
