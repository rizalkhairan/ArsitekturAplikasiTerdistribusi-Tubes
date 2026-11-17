package canonical

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

var schemaOnce sync.Once
var schemaError error

func TestRestrictedApplicationRole(t *testing.T) {
	_, p, a := integration(t)
	u, err := url.Parse(os.Getenv("CANONICAL_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.User("canonical_app")
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{DB: db, Topic: "app-role-test", RequireReplication: os.Getenv("CANONICAL_TEST_REPLICATION") == "1"}
	ctx := context.Background()
	if _, err = s.Put(ctx, p, "SATELIT", uuid.NewString(), mutation(a, 1)); err != nil {
		t.Fatalf("restricted application cannot write: %v", err)
	}
	if _, err = db.Exec(`SELECT rolpassword FROM pg_authid`); err == nil {
		t.Fatal("application can read database passwords")
	}
	if _, err = db.Exec(`ALTER TABLE canonical.events ADD COLUMN forbidden text`); err == nil {
		t.Fatal("application can alter canonical schema")
	}
}

func TestReplicationUnavailable(t *testing.T) {
	if os.Getenv("CANONICAL_TEST_UNAVAILABLE") != "1" {
		t.Skip("isolated standby-loss scenario only")
	}
	db, err := sql.Open("postgres", os.Getenv("CANONICAL_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{DB: db, RequireReplication: true}
	a := Authority{"missing-copy-test", 1}
	p := Principal{Owner: "owner", Assignments: map[string][]string{a.Partition: {"SATELIT"}}}
	_, err = s.Put(context.Background(), p, "SATELIT", "must-not-ack", mutation(a, 1))
	requireCode(t, err, "replication_unavailable")
}

func integration(t *testing.T) (*Store, Principal, Authority) {
	t.Helper()
	dsn := os.Getenv("CANONICAL_TEST_DSN")
	if dsn == "" {
		t.Skip("set CANONICAL_TEST_DSN to an isolated Citus database (schema owner)")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schemaOnce.Do(func() { schemaError = Migrate(context.Background(), db) })
	if schemaError != nil {
		t.Fatal(schemaError)
	}
	id := uuid.NewString()
	s := &Store{DB: db, Topic: "test-" + id, RequireReplication: os.Getenv("CANONICAL_TEST_REPLICATION") == "1"}
	p := Principal{Owner: "owner-" + id, Assignments: map[string][]string{id: {"SATELIT"}}}
	a := Authority{id, 1}
	if err = s.InstallLease(context.Background(), Lease{a, p.Owner, time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	return s, p, a
}
func mutation(a Authority, version int64) Mutation {
	lat, lon := -7.0, 110.0
	return Mutation{version, "upsert", a, "test-correlation", &Input{"WILDFIRE", "AWAS", "same region", &lat, &lon, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), map[string]any{"detection_keys": []any{"verbatim|1.0000"}}}}
}
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}
func TestVersionsTombstonesAndAuthority(t *testing.T) {
	s, p, a := integration(t)
	ctx := context.Background()
	ref := uuid.NewString()
	m := mutation(a, 1)
	first, err := s.Put(ctx, p, "SATELIT", ref, m)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.Get(ctx, first.HazardID)
	if err != nil {
		t.Fatal(err)
	}
	m.CorrelationID = "another-request"
	retry, err := s.Put(ctx, p, "SATELIT", ref, m)
	if err != nil || !retry.Replayed || retry.Created || retry.HazardID != first.HazardID {
		t.Fatalf("retry: %+v %v", retry, err)
	}
	m.Event.AreaName = "changed"
	_, err = s.Put(ctx, p, "SATELIT", ref, m)
	requireCode(t, err, "version_conflict")
	m.Version = 2
	if _, err = s.Put(ctx, p, "SATELIT", ref, m); err != nil {
		t.Fatal(err)
	}
	after, err := s.Get(ctx, first.HazardID)
	if err != nil || before.HazardID != after.HazardID || !before.IngestedAt.Equal(after.IngestedAt) {
		t.Fatalf("identity changed: %+v %v", after, err)
	}
	m.Version = 1
	_, err = s.Put(ctx, p, "SATELIT", ref, m)
	requireCode(t, err, "stale_version")
	m = Mutation{3, "retract", a, "retract", nil}
	if _, err = s.Put(ctx, p, "SATELIT", ref, m); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Get(ctx, first.HazardID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("retracted event visible: %v", err)
	}
	var count int
	if err = s.DB.QueryRow(`SELECT count(*) FROM canonical.outbox WHERE event_key=$1`, Key("SATELIT", ref)).Scan(&count); err != nil || count != 3 {
		t.Fatalf("outbox changes=%d %v", count, err)
	}
	if _, err = s.Put(ctx, p, "SATELIT", ref, mutation(a, 4)); err != nil {
		t.Fatal(err)
	}
	absent := uuid.NewString()
	if _, err = s.Put(ctx, p, "SATELIT", absent, m); err != nil {
		t.Fatal(err)
	}
	_, err = s.Put(ctx, p, "SATELIT", absent, mutation(a, 2))
	requireCode(t, err, "stale_version")
	newAuthority := Authority{a.Partition, 2}
	if err = s.InstallLease(ctx, Lease{newAuthority, "successor", time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	_, err = s.Put(ctx, p, "SATELIT", ref, mutation(a, 4))
	requireCode(t, err, "stale_authority")
	p.Owner = "successor"
	r, err := s.Put(ctx, p, "SATELIT", ref, mutation(newAuthority, 4))
	if err != nil || !r.Replayed {
		t.Fatalf("successor retry: %+v %v", r, err)
	}
	err = s.InstallLease(ctx, Lease{newAuthority, "impostor", time.Now().Add(2 * time.Hour)})
	requireCode(t, err, "stale_authority")
}

func TestConcurrentRetriesAndCrossPartitionRace(t *testing.T) {
	s, p, a := integration(t)
	ctx := context.Background()
	ref := uuid.NewString()
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := s.Put(ctx, p, "SATELIT", ref, mutation(a, 1)); results <- err }()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := s.DB.QueryRow(`SELECT count(*) FROM canonical.outbox WHERE event_key=$1`, Key("SATELIT", ref)).Scan(&count); err != nil || count != 1 {
		t.Fatalf("dedup count=%d %v", count, err)
	}
	b := Authority{uuid.NewString(), 1}
	other := Principal{Owner: "other", Assignments: map[string][]string{b.Partition: {"SATELIT"}}}
	if err := s.InstallLease(ctx, Lease{b, other.Owner, time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	_, err := s.Put(ctx, other, "SATELIT", ref, mutation(b, 99))
	requireCode(t, err, "partition_conflict")
}

func TestFenceInstallationSerializesWithCommit(t *testing.T) {
	s, p, a := integration(t)
	ctx := context.Background()
	tx, err := s.begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = lockAuthority(ctx, tx, p, a); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- s.InstallLease(ctx, Lease{Authority{a.Partition, 2}, "new-owner", time.Now().Add(time.Hour)})
	}()
	select {
	case err = <-done:
		t.Fatalf("new epoch crossed uncommitted write: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	ref := uuid.NewString()
	if _, err = s.mutate(ctx, tx, "SATELIT", ref, mutation(a, 1)); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	_, err = s.Put(ctx, p, "SATELIT", ref, mutation(a, 2))
	requireCode(t, err, "stale_authority")
}

func TestWindowAtomicityAndEmptyReceipt(t *testing.T) {
	s, p, a := integration(t)
	ctx := context.Background()
	w := Window{Authority: a, WindowID: uuid.NewString(), PreviousCursor: "before", NextCursor: "after", Items: []WindowItem{}}
	r, err := s.Seal(ctx, p, w)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := s.Seal(ctx, p, w)
	if err != nil || r != retry {
		t.Fatalf("empty receipt changed: %+v %v", retry, err)
	}
	w.NextCursor = "conflicting"
	_, err = s.Seal(ctx, p, w)
	requireCode(t, err, "window_conflict")
	good, bad := uuid.NewString(), uuid.NewString()
	if _, err = s.Put(ctx, p, "SATELIT", bad, mutation(a, 2)); err != nil {
		t.Fatal(err)
	}
	w.WindowID = uuid.NewString()
	w.Items = []WindowItem{{"SATELIT", good, mutation(a, 1)}, {"SATELIT", bad, mutation(a, 1)}}
	_, err = s.Seal(ctx, p, w)
	requireCode(t, err, "stale_version")
	var count int
	if err = s.DB.QueryRow(`SELECT count(*) FROM canonical.events WHERE event_key=$1`, Key("SATELIT", good)).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed batch leaked %d events: %v", count, err)
	}
	w.WindowID = uuid.NewString()
	w.Items = []WindowItem{{"SATELIT", good, mutation(a, 1)}}
	if _, err = s.Seal(ctx, p, w); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Put(ctx, p, "SATELIT", good, mutation(a, 2)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Seal(ctx, p, w); err != nil {
		t.Fatalf("sealed retry after a newer event: %v", err)
	}
	a.Epoch = 2
	p.Owner = "successor"
	if err = s.InstallLease(ctx, Lease{a, p.Owner, time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	w.Authority = a
	w.Items[0].Mutation.Authority = a
	r, err = s.Seal(ctx, p, w)
	if err != nil || r.Epoch != 2 {
		t.Fatalf("successor must reseal under its own epoch: %+v %v", r, err)
	}
}

func TestExpiredLeaseCannotBeRenewedOrWrite(t *testing.T) {
	s, p, a := integration(t)
	ctx := context.Background()
	if _, err := s.DB.Exec(`UPDATE canonical.authorities SET expires_at=clock_timestamp()-interval '1 second' WHERE partition=$1`, a.Partition); err != nil {
		t.Fatal(err)
	}
	_, err := s.Put(ctx, p, "SATELIT", uuid.NewString(), mutation(a, 1))
	requireCode(t, err, "stale_authority")
	err = s.InstallLease(ctx, Lease{a, p.Owner, time.Now().Add(time.Hour)})
	requireCode(t, err, "stale_authority")
}

func TestOutboxRetryAndVersionOrder(t *testing.T) {
	s, p, a := integration(t)
	ctx := context.Background()
	ref := uuid.NewString()
	for v := int64(1); v <= 3; v++ {
		if _, err := s.Put(ctx, p, "SATELIT", ref, mutation(a, v)); err != nil {
			t.Fatal(err)
		}
	}
	failedID := ""
	err := s.Publish(ctx, func(_ context.Context, e database.OutboxEvent) error {
		failedID = e.EventID
		return errors.New("broker unavailable")
	})
	if err == nil {
		t.Fatal("broker failure should retain pending message")
	}
	for want := int64(1); want <= 3; want++ {
		seen := 0
		err = s.Publish(ctx, func(_ context.Context, e database.OutboxEvent) error {
			seen++
			var payload struct {
				Version int64 `json:"version"`
			}
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				return err
			}
			if payload.Version != want {
				return fmt.Errorf("want version %d got %d", want, payload.Version)
			}
			if want == 1 && e.EventID != failedID {
				return errors.New("retry changed broker identity")
			}
			return nil
		})
		if err != nil || seen != 1 {
			t.Fatalf("publish version %d count %d: %v", want, seen, err)
		}
	}
}

func TestM1ImportPreservesIdentityAndPublicationState(t *testing.T) {
	s, _, _ := integration(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "m1.db")
	legacy, err := database.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	event := database.HazardEvent{Source: "BMKG", SourceRefID: uuid.NewString(), HazardType: "SEISMIC", Severity: "AWAS", AreaName: "area", OccurredAt: time.Now().UTC(), Attributes: map[string]any{}}
	if _, err = database.SaveHazards(ctx, legacy, []database.HazardEvent{event}, nil, s.Topic); err != nil {
		t.Fatal(err)
	}
	old, err := database.ListHazards(ctx, legacy, "", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.ImportM1(ctx, path); err != nil || n != 1 {
		t.Fatalf("import: %d %v", n, err)
	}
	got, err := s.Get(ctx, old[0].HazardID)
	if err != nil || !got.IngestedAt.Equal(old[0].IngestedAt) {
		t.Fatalf("import lost identity/time: %+v %v", got, err)
	}
	if n, err := s.ImportM1(ctx, path); err != nil || n != 0 {
		t.Fatalf("repeat import: %d %v", n, err)
	}
}
