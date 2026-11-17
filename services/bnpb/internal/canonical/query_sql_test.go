package canonical

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestSQLSnapshotConcurrentMutationsAndReplicaRouting(t *testing.T) {
	s, p, a := integration(t)
	ctx := context.Background()
	since := time.Now().UTC().Format(time.RFC3339Nano)
	refs := []string{a.Partition + "a", a.Partition + "b", a.Partition + "c"}
	for _, ref := range refs {
		if _, err := s.Put(ctx, p, "SATELIT", ref, mutation(a, 1)); err != nil {
			t.Fatal(err)
		}
	}
	q := Query{Source: "SATELIT", Since: since, Limit: 1}
	first, err := s.Query(ctx, q)
	if err != nil || first.Partial || first.NextCursor == nil || len(first.Items) != 1 {
		t.Fatalf("first: %+v %v", first, err)
	}
	// Higher versions can change event time and fields, or retract an unseen row.
	m := mutation(a, 2)
	m.Event.OccurredAt = time.Now()
	m.Event.Severity = "NORMAL"
	if _, err = s.Put(ctx, p, "SATELIT", refs[1], m); err != nil {
		t.Fatal(err)
	}
	m = mutation(a, 2)
	m.Operation = "retract"
	m.Event = nil
	if _, err = s.Put(ctx, p, "SATELIT", refs[2], m); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Put(ctx, p, "SATELIT", a.Partition+"new", mutation(a, 1)); err != nil {
		t.Fatal(err)
	}
	// A different Aggregator Store instance shares the snapshot through SQL.
	other := &Store{DB: s.DB}
	q.Cursor = *first.NextCursor
	q.Limit = 100
	last, err := other.Query(ctx, q)
	if err != nil || last.Partial || last.NextCursor != nil || len(last.Items) != 2 {
		t.Fatalf("last: %+v %v", last, err)
	}
	if last.Items[0].Severity != "AWAS" || last.Items[1].SourceRefID != refs[2] {
		t.Fatalf("snapshot changed after update/retract: %+v", last)
	}
	replay, err := s.Query(ctx, q)
	if err != nil || !reflect.DeepEqual(replay, last) {
		t.Fatal("cursor replay changed", err)
	}
	q.Cursor = ""
	fresh, err := s.Query(ctx, q)
	if err != nil || len(fresh.Items) != 3 || fresh.Items[1].Severity != "NORMAL" || fresh.Items[2].SourceRefID != a.Partition+"new" {
		t.Fatalf("fresh traversal: %+v %v", fresh, err)
	}
	q.Cursor = *first.NextCursor
	q.Source = "BMKG"
	_, err = s.Query(ctx, q)
	requireCode(t, err, "invalid_request")
	q.Source = "SATELIT"
	id, _, _ := decodeCursor(q.Cursor)
	if _, err = s.DB.Exec(`DELETE FROM canonical.read_snapshots WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	_, err = s.Query(ctx, q)
	requireCode(t, err, "snapshot_unavailable")
}

func TestSQLShardTimeoutPartialPaginationAndRecovery(t *testing.T) {
	s, p, a := integration(t)
	ctx := context.Background()
	var localShard int64
	if err := s.DB.QueryRow(`SELECT s.shardid FROM pg_dist_shard s JOIN pg_dist_placement p USING(shardid)
	 WHERE s.logicalrelid='canonical.events'::regclass AND p.groupid=0 ORDER BY s.shardid LIMIT 1`).Scan(&localShard); err != nil {
		t.Fatal(err)
	}
	since := time.Now().UTC().Format(time.RFC3339Nano)
	// Ensure one known record lives on the shard whose table lock simulates a
	// slow/unavailable unit, and enough surviving rows require several pages.
	localRef := ""
	for i := 0; i < 10000; i++ {
		ref := fmt.Sprintf("%s-%d", a.Partition, i)
		var shard int64
		if err := s.DB.QueryRow(`SELECT get_shard_id_for_distribution_column('canonical.events',$1::text)`, Key("SATELIT", ref)).Scan(&shard); err != nil {
			t.Fatal(err)
		}
		if i < 8 || shard == localShard {
			if _, err := s.Put(ctx, p, "SATELIT", ref, mutation(a, 1)); err != nil {
				t.Fatal(err)
			}
		}
		if shard == localShard {
			localRef = ref
			break
		}
	}
	if localRef == "" {
		t.Fatal("no key for local shard")
	}
	// Add surviving rows even if the local key was found immediately.
	for i := 0; i < 8; i++ {
		if _, err := s.Put(ctx, p, "SATELIT", fmt.Sprintf("%s-extra-%d", a.Partition, i), mutation(a, 1)); err != nil {
			t.Fatal(err)
		}
	}
	lock, err := s.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err = lock.Exec(`SET LOCAL citus.enable_manual_changes_to_shards=on`); err != nil {
		t.Fatal(err)
	}
	if _, err = lock.Exec(fmt.Sprintf(`LOCK TABLE canonical.events_%d IN ACCESS EXCLUSIVE MODE`, localShard)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	q := Query{Source: "SATELIT", Since: since, Limit: 1}
	page, err := s.Query(ctx, q)
	if err != nil || !page.Partial || page.NextCursor == nil || !contains(page.UnavailableUnits, fmt.Sprintf("shard-%d", localShard)) {
		t.Fatalf("partial: %+v %v", page, err)
	}
	if time.Since(start) > 8*time.Second {
		t.Fatal("read exceeded request bound")
	}
	if _, err = s.List(ctx, "SATELIT", "", since, 100); err == nil {
		t.Fatal("legacy list reported complete coverage")
	}
	if err = lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	// Recovery cannot upgrade a partial cursor or insert previously absent rows.
	missing := page.UnavailableUnits
	seen := map[string]bool{}
	for {
		if !page.Partial || !reflect.DeepEqual(page.UnavailableUnits, missing) {
			t.Fatal("partial coverage changed after recovery")
		}
		for _, e := range page.Items {
			if seen[e.HazardID] || e.SourceRefID == localRef {
				t.Fatal("duplicate or recovered row inserted into cursor")
			}
			seen[e.HazardID] = true
		}
		if page.NextCursor == nil {
			break
		}
		q.Cursor = *page.NextCursor
		page, err = s.Query(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
	}
	q.Cursor = ""
	q.Limit = 100
	page, err = s.Query(ctx, q)
	if err != nil || page.Partial {
		t.Fatalf("recovered: %+v %v", page, err)
	}
	found := false
	for _, e := range page.Items {
		found = found || e.SourceRefID == localRef
	}
	if !found {
		t.Fatal("fresh traversal skipped recovered shard")
	}
}

func TestSQLUnavailableWorkerAndRecovery(t *testing.T) {
	mode := os.Getenv("CANONICAL_TEST_READ_OUTAGE")
	if mode == "" {
		t.Skip("disposable worker outage scenario only")
	}
	db, err := sql.Open("postgres", os.Getenv("CANONICAL_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{DB: db}
	page, err := s.Query(context.Background(), Query{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if mode == "unavailable" {
		if !page.Partial || len(page.UnavailableUnits) != 16 || len(page.Items) == 0 {
			t.Fatalf("outage coverage: %+v", page)
		}
	} else {
		if page.Partial {
			t.Fatalf("recovery still partial: %v", page.UnavailableUnits)
		}
		var count int
		if err = db.QueryRow(`SELECT count(*) FROM canonical.events WHERE operation='upsert'`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if len(page.Items) != count || page.NextCursor != nil {
			t.Fatalf("recovery omitted rows: %d of %d", len(page.Items), count)
		}
	}
}

func contains(values []string, wanted string) bool {
	for _, v := range values {
		if v == wanted {
			return true
		}
	}
	return false
}

func TestSQLReadUsesRestrictedRole(t *testing.T) {
	s, _, _ := integration(t)
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
	app := &Store{DB: db}
	page, err := app.Query(context.Background(), Query{Limit: 1})
	if err != nil || page.Partial || len(page.Items) != 1 || page.NextCursor == nil {
		t.Fatalf("restricted query: %+v %v", page, err)
	}
	_, err = app.Query(context.Background(), Query{Limit: 1000, Cursor: *page.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	// A temporary relation must not shadow the privileged helper's metadata.
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(context.Background(), `CREATE TEMP TABLE pg_dist_shard(shardid bigint,logicalrelid regclass)`); err != nil {
		t.Fatal(err)
	}
	units, err := s.inventory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err = conn.QueryRowContext(context.Background(), `SELECT canonical.read_shard($1,'','')`, units[0].ID).Scan(&raw); err != nil {
		t.Fatal("temporary metadata shadowed helper", err)
	}
	if _, err = db.Exec(`SELECT canonical.read_shard(0,'','')`); err == nil {
		t.Fatal("read helper accepted unknown shard")
	}
	// Keep the owner connection alive until the restricted query completes.
	if err = s.DB.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLOnlineQueryWorkload(t *testing.T) {
	if os.Getenv("CANONICAL_TEST_QUERY_REBALANCE") != "1" {
		t.Skip("disposable online read scenario only")
	}
	s, _, _ := integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	stop := os.Getenv("CANONICAL_TEST_WORKLOAD_STOP")
	if stop == "" {
		t.Fatal("runner must supply stop file")
	}
	complete, partial := 0, 0
	for i := 0; ; i++ {
		if i >= 2 {
			if _, err := os.Stat(stop); err == nil {
				break
			}
		}
		q := Query{Limit: 1000}
		page, err := s.Query(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		missing := page.UnavailableUnits
		seen := map[string]bool{}
		var previous Event
		for {
			if !reflect.DeepEqual(page.UnavailableUnits, missing) || page.Partial != (len(missing) > 0) {
				t.Fatal("coverage changed within traversal")
			}
			for _, event := range page.Items {
				if seen[event.HazardID] || (!previous.IngestedAt.IsZero() && (event.IngestedAt.Before(previous.IngestedAt) || (event.IngestedAt.Equal(previous.IngestedAt) && event.HazardID <= previous.HazardID))) {
					t.Fatal("duplicate or unordered event during online operation")
				}
				seen[event.HazardID] = true
				previous = event
			}
			if page.NextCursor == nil {
				break
			}
			q.Cursor = *page.NextCursor
			page, err = s.Query(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
		}
		if len(missing) == 0 {
			complete++
		} else {
			partial++
		}
		if i == 0 {
			fmt.Println("A09_QUERY_READY")
		}
	}
	if complete == 0 {
		t.Fatal("no complete online traversal")
	}
	fmt.Printf("A09_QUERY_COMPLETED complete=%d partial=%d\n", complete, partial)
}
