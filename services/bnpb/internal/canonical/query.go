package canonical

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	readTimeout  = 6 * time.Second
	unitTimeout  = 400 * time.Millisecond
	readWorkers  = 4
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
	units, err := reader.inventory(ctx)
	if err != nil {
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
	byIdentity := map[string]copyRow{}
	type versionKey struct {
		identity string
		version  int64
	}
	digests := map[versionKey]string{}
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
			key := Key(row.Event.Source, row.Event.SourceRefID)
			old, exists := byIdentity[key]
			if row.Version < 1 || (row.Operation != "upsert" && row.Operation != "retract") || row.Event.HazardID == "" {
				return nil, nil, storageError("inconsistent_copies", "invalid stored event")
			}
			if exists && (old.Event.HazardID != row.Event.HazardID || !old.Event.IngestedAt.Equal(row.Event.IngestedAt)) {
				return nil, nil, storageError("inconsistent_copies", "copies disagree on immutable identity")
			}
			vk := versionKey{key, row.Version}
			digest := hash(row)
			if previous, seen := digests[vk]; seen && previous != digest {
				return nil, nil, storageError("inconsistent_copies", "copies disagree at the same version")
			}
			digests[vk] = digest
			if !exists || row.Version > old.Version {
				byIdentity[key] = row
			}
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
	if err != nil || !sameInventory(units, after) {
		return nil, nil, storageError("coverage_changed", "shard inventory changed or could not be verified; retry a fresh traversal")
	}
	events := []Event{}
	ids := map[string]bool{}
	for _, row := range byIdentity {
		if ids[row.Event.HazardID] {
			return nil, nil, storageError("inconsistent_copies", "hazard ID belongs to multiple identities")
		}
		ids[row.Event.HazardID] = true
		if row.Operation == "upsert" {
			events = append(events, row.Event)
		}
	}
	sort.Strings(missing)
	return events, missing, nil
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

type snapshot struct {
	ID      string
	Secret  []byte
	Query   Query
	Items   []Event
	Missing []string
	Expires time.Time
}

// The MAC prevents caller-chosen offsets; the secret is shared through the
// coordinator, so any Aggregator replica can serve and replay a cursor.
func (s snapshot) cursor(offset int) string {
	value := s.ID + "." + strconv.Itoa(offset) + "." + strconv.FormatInt(s.Expires.Unix(), 10)
	mac := hmac.New(sha256.New, s.Secret)
	mac.Write([]byte(value))
	return base64.RawURLEncoding.EncodeToString([]byte(value + "." + hex.EncodeToString(mac.Sum(nil))))
}
func decodeCursor(cursor string) (string, int, error) {
	if len(cursor) > 256 {
		return "", 0, invalid("invalid cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	parts := strings.Split(string(raw), ".")
	if err != nil || len(parts) != 4 {
		return "", 0, invalid("invalid cursor")
	}
	id, err := uuid.Parse(parts[0])
	if err != nil {
		return "", 0, invalid("invalid cursor")
	}
	offset, err := strconv.Atoi(parts[1])
	if err != nil || offset < 1 {
		return "", 0, invalid("invalid cursor")
	}
	expires, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || expires < 1 {
		return "", 0, invalid("invalid cursor")
	}
	if time.Now().Unix() >= expires {
		return "", 0, &Error{400, "cursor_expired", "snapshot expired; start a fresh traversal"}
	}
	return id.String(), offset, nil
}
func (s snapshot) page(q Query, offset int) (Page, error) {
	if !time.Now().Before(s.Expires) {
		return Page{}, &Error{400, "cursor_expired", "snapshot expired; start a fresh traversal"}
	}
	if q.Source != s.Query.Source || q.Since != s.Query.Since {
		return Page{}, invalid("cursor filters differ from snapshot")
	}
	if offset > len(s.Items) || (offset > 0 && !hmac.Equal([]byte(q.Cursor), []byte(s.cursor(offset)))) {
		return Page{}, invalid("invalid cursor")
	}
	end := offset + q.Limit
	if end > len(s.Items) {
		end = len(s.Items)
	}
	page := Page{Items: s.Items[offset:end], Partial: len(s.Missing) > 0, UnavailableUnits: s.Missing}
	if end < len(s.Items) {
		cursor := s.cursor(end)
		page.NextCursor = &cursor
	}
	return page, nil
}

func (s *Store) Query(ctx context.Context, q Query) (Page, error) {
	if err := q.validate(); err != nil {
		return Page{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if q.Cursor != "" {
		id, offset, err := decodeCursor(q.Cursor)
		if err != nil {
			return Page{}, err
		}
		snap, err := s.loadSnapshot(ctx, id)
		if err != nil {
			return Page{}, err
		}
		return snap.page(q, offset)
	}
	events, missing, err := gather(ctx, s, q)
	if err != nil {
		return Page{}, err
	}
	snap := snapshot{ID: uuid.NewString(), Secret: make([]byte, 32), Query: q, Items: selectEvents(events, q), Missing: missing, Expires: time.Now().Add(snapshotTTL)}
	if _, err = rand.Read(snap.Secret); err != nil {
		return Page{}, err
	}
	if len(snap.Items) > q.Limit {
		if err = s.saveSnapshot(ctx, snap); err != nil {
			return Page{}, err
		}
	}
	return snap.page(q, 0)
}
