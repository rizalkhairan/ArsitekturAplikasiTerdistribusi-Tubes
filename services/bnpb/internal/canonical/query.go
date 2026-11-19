package canonical

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

)

const (
	readTimeout  = 60 * time.Second
	unitTimeout  = 8 * time.Second
	readWorkers  = 16
	maxReadBytes = 16 << 20
	snapshotTTL  = 300 * time.Second
)

type Query struct {
	Source string `json:"source"`
	Since  string `json:"since"`
	Limit  int    `json:"-"`
	Cursor string `json:"-"`
}

type Page struct {
	Items            []Event  `json:"items"`
	NextCursor       *string  `json:"next_cursor"`
	Partial          bool     `json:"partial"`
	UnavailableUnits []string `json:"unavailable_units"`
}

// unit describes a logical Citus shard, never an individual physical copy.
type unit struct {
	ID       int64
	Min, Max int64
}

func (u unit) name() string { return fmt.Sprintf("shard-%d", u.ID) }

type copyRow struct {
	Version   int64  `json:"version"`
	Operation string `json:"operation"`
	Event     Event  `json:"event"`
}

type shardReader interface {
	inventory(context.Context) ([]unit, error)
	readUnit(context.Context, unit, Query) ([]copyRow, error)
	// readAllPage serves one page through a single Citus query, ordered by
	// (ingested_at, hazard_id) and bounded by the keyset position.
	readAllPage(context.Context, Query, *cursorPos, int) ([]copyRow, error)
	// readAll is the unbounded variant used by full traversals.
	readAll(context.Context, Query) ([]copyRow, error)
}

func storageError(code, message string) error { return &Error{503, code, message} }

func (q *Query) validate() error {
	if q.Source != "" && !validSource(q.Source) {
		return invalid("unknown source")
	}
	if q.Limit == 0 {
		q.Limit = 100
	}
	if q.Limit < 1 || q.Limit > 1000 {
		return invalid("limit must be 1..1000")
	}
	if q.Since != "" {
		t, err := time.Parse(time.RFC3339Nano, q.Since)
		if err != nil {
			return invalid("since must be an RFC3339 timestamp")
		}
		q.Since = t.UTC().Format(time.RFC3339Nano)
	}
	return nil
}

// gather either accounts for the entire logical hash space or fails. A unit
// that cannot be read completely contributes no records, including tombstones.
func gather(ctx context.Context, reader shardReader, q Query) ([]Event, []string, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	// Fast path: one distributed query. Citus fails the whole query when a
	// placement is unreachable, so success means every shard answered.
	if rows, fastErr := reader.readAll(ctx, q); fastErr == nil {
		events, mergeErr := mergeRows(rows)
		return events, nil, mergeErr
	} else {
		log.Printf("canonical distributed read unavailable (%v); falling back to per-shard traversal", fastErr)
	}
	units, err := reader.inventory(ctx)
	if err != nil {
		log.Printf("canonical inventory before traversal: %v", err)
		return nil, nil, storageError("coverage_unavailable", "cannot discover complete shard inventory")
	}
	if !validInventory(units) {
		return nil, nil, storageError("coverage_unavailable", "shard inventory does not cover the hash space")
	}
	type result struct {
		index int
		rows  []copyRow
		err   error
	}
	jobs := make(chan int, len(units))
	results := make(chan result, readWorkers)
	for i := range units {
		jobs <- i
	}
	close(jobs)
	var workers sync.WaitGroup
	for n := 0; n < readWorkers; n++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				unitCtx, stop := context.WithTimeout(ctx, unitTimeout)
				rows, readErr := reader.readUnit(unitCtx, units[i], q)
				stop()
				select {
				case results <- result{i, rows, readErr}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() { workers.Wait(); close(results) }()
	missing := []string{}
	seenUnits := make(map[int]bool)
	collected := []copyRow{}
	bytes := 0
	for result := range results {
		seenUnits[result.index] = true
		if result.err != nil {
			missing = append(missing, units[result.index].name())
			continue
		}
		for _, row := range result.rows {
			raw, marshalErr := json.Marshal(row)
			if marshalErr != nil {
				return nil, nil, marshalErr
			}
			bytes += len(raw)
			if bytes > maxReadBytes {
				return nil, nil, storageError("snapshot_too_large", "read exceeds 16 MiB; narrow source/since")
			}
			collected = append(collected, row)
		}
	}
	for i, u := range units {
		if !seenUnits[i] {
			missing = append(missing, u.name())
		}
	}
	// Rebalancing may change placements safely, but splitting/removing shards
	// changes the traversal's universe and must never be reported as complete.
	after, err := reader.inventory(ctx)
	if err != nil {
		log.Printf("canonical inventory after traversal: %v", err)
		return nil, nil, storageError("coverage_changed", "shard inventory changed or could not be verified; retry a fresh traversal")
	}
	if !sameInventory(units, after) {
		log.Printf("canonical inventory changed before=%v after=%v", units, after)
		return nil, nil, storageError("coverage_changed", "shard inventory changed or could not be verified; retry a fresh traversal")
	}
	events, err := mergeRows(collected)
	if err != nil {
		return nil, nil, err
	}
	sort.Strings(missing)
	return events, missing, nil
}

type versionKey struct {
	identity string
	version  int64
}

// mergeRows folds one row per identity, keeping the highest version and
// rejecting copies that disagree about immutable fields. Retracted identities
// are dropped from the result but stay visible in storage.
func mergeRows(rows []copyRow) ([]Event, error) {
	byIdentity := map[string]copyRow{}
	digests := map[versionKey]string{}
	for _, row := range rows {
		if row.Version < 1 || (row.Operation != "upsert" && row.Operation != "retract") || row.Event.HazardID == "" {
			return nil, storageError("inconsistent_copies", "invalid stored event")
		}
		key := Key(row.Event.Source, row.Event.SourceRefID)
		old, exists := byIdentity[key]
		if exists && (old.Event.HazardID != row.Event.HazardID || !old.Event.IngestedAt.Equal(row.Event.IngestedAt)) {
			return nil, storageError("inconsistent_copies", "copies disagree on immutable identity")
		}
		vk := versionKey{key, row.Version}
		digest := hash(row)
		if previous, seen := digests[vk]; seen && previous != digest {
			return nil, storageError("inconsistent_copies", "copies disagree at the same version")
		}
		digests[vk] = digest
		if !exists || row.Version > old.Version {
			byIdentity[key] = row
		}
	}
	events := []Event{}
	ids := map[string]bool{}
	for _, row := range byIdentity {
		if ids[row.Event.HazardID] {
			return nil, storageError("inconsistent_copies", "hazard ID belongs to multiple identities")
		}
		ids[row.Event.HazardID] = true
		if row.Operation == "upsert" {
			events = append(events, row.Event)
		}
	}
	return events, nil
}

func validInventory(units []unit) bool {
	if len(units) == 0 || len(units) > 1024 {
		return false
	}
	next := int64(-2147483648)
	ids := map[int64]bool{}
	for _, u := range units {
		if u.ID <= 0 || ids[u.ID] || u.Min != next || u.Max < u.Min {
			return false
		}
		ids[u.ID] = true
		next = u.Max + 1
	}
	return next == 2147483648
}
func sameInventory(a, b []unit) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func selectEvents(events []Event, q Query) []Event {
	selected := []Event{}
	var since time.Time
	if q.Since != "" {
		since, _ = time.Parse(time.RFC3339Nano, q.Since)
	}
	for _, event := range events {
		if (q.Source == "" || q.Source == event.Source) && !event.IngestedAt.Before(since) {
			selected = append(selected, event)
		}
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].IngestedAt.Equal(selected[j].IngestedAt) {
			return selected[i].HazardID < selected[j].HazardID
		}
		return selected[i].IngestedAt.Before(selected[j].IngestedAt)
	})
	return selected
}

type cursorPos struct {
	At time.Time
	ID string
}

// Cursors are keyset positions, not snapshot offsets: a page reads only the
// rows after the previous page, so paging never materialises the whole table.
func encodeCursor(event Event) string {
	return base64.RawURLEncoding.EncodeToString([]byte(event.IngestedAt.UTC().Format(time.RFC3339Nano) + "\x00" + event.HazardID))
}

func decodeCursor(cursor string) (cursorPos, error) {
	if len(cursor) > 512 {
		return cursorPos{}, invalid("invalid cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return cursorPos{}, invalid("invalid cursor")
	}
	parts := strings.SplitN(string(raw), "\x00", 2)
	if len(parts) != 2 || parts[1] == "" {
		return cursorPos{}, invalid("invalid cursor")
	}
	at, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return cursorPos{}, invalid("invalid cursor")
	}
	return cursorPos{At: at, ID: parts[1]}, nil
}

func afterCursor(event Event, pos *cursorPos) bool {
	if pos == nil {
		return true
	}
	if event.IngestedAt.After(pos.At) {
		return true
	}
	return event.IngestedAt.Equal(pos.At) && event.HazardID > pos.ID
}

// gatherPage returns at most q.Limit+1 events after pos. The extra row tells
// the caller whether another page exists.
func gatherPage(ctx context.Context, reader shardReader, q Query, pos *cursorPos) ([]Event, []string, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	if rows, fastErr := reader.readAllPage(ctx, q, pos, q.Limit+1); fastErr == nil {
		events, mergeErr := mergeRows(rows)
		if mergeErr != nil {
			return nil, nil, mergeErr
		}
		return selectEvents(events, q), nil, nil
	} else {
		log.Printf("canonical distributed read unavailable (%v); falling back to per-shard traversal", fastErr)
	}
	events, missing, err := gather(ctx, reader, q)
	if err != nil {
		return nil, nil, err
	}
	remaining := []Event{}
	for _, event := range selectEvents(events, q) {
		if afterCursor(event, pos) {
			remaining = append(remaining, event)
		}
	}
	if len(remaining) > q.Limit+1 {
		remaining = remaining[:q.Limit+1]
	}
	return remaining, missing, nil
}

func (s *Store) Query(ctx context.Context, q Query) (Page, error) {
	if err := q.validate(); err != nil {
		return Page{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var pos *cursorPos
	if q.Cursor != "" {
		decoded, err := decodeCursor(q.Cursor)
		if err != nil {
			return Page{}, err
		}
		pos = &decoded
	}
	events, missing, err := gatherPage(ctx, s, q, pos)
	if err != nil {
		return Page{}, err
	}
	page := Page{Items: []Event{}, Partial: len(missing) > 0, UnavailableUnits: missing}
	if len(events) > q.Limit {
		events = events[:q.Limit]
		next := encodeCursor(events[len(events)-1])
		page.NextCursor = &next
	}
	page.Items = events
	if page.UnavailableUnits == nil {
		page.UnavailableUnits = []string{}
	}
	return page, nil
}
