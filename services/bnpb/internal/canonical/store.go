package canonical

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

//go:embed schema.sql
var Schema string

// Citus DDL propagation requires one SQL command per protocol message. The
// explicit delimiter also keeps dollar-quoted function bodies intact.
func Migrate(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(70407002)`); err != nil {
		return err
	}
	for _, statement := range strings.Split(Schema, "-- statement\n") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("canonical migration: %w", err)
		}
	}
	return tx.Commit()
}

type Store struct {
	DB    *sql.DB
	Topic string
	// Production enforces the administrator-controlled recovery policy. Tests
	// can exercise SQL alone; those responses say unreplicated_test.
	RequireReplication bool
}

func Open(ctx context.Context, dsn, topic string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err = db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("open canonical PostgreSQL: %w", err)
	}
	return &Store{DB: db, Topic: topic, RequireReplication: true}, nil
}

// The returned policy belongs to this transaction, never mutable Store state.
func (s *Store) begin(ctx context.Context) (*sql.Tx, string, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, "", err
	}
	_, err = tx.ExecContext(ctx, `SET LOCAL synchronous_commit = 'remote_apply'; SET LOCAL statement_timeout = '8s'; SET LOCAL lock_timeout = '3s'`)
	durability := "unreplicated_test"
	if err == nil && s.RequireReplication {
		err = tx.QueryRowContext(ctx, `SELECT durability FROM canonical.recovery WHERE singleton FOR SHARE`).Scan(&durability)
		if err == nil {
			var ready bool
			query := `SELECT canonical.replication_ready()`
			if durability == "single_copy" {
				query = `SELECT canonical.primaries_ready()`
			} else if durability != "replicated" {
				err = fmt.Errorf("unknown canonical durability policy")
			}
			if err == nil {
				err = tx.QueryRowContext(ctx, query).Scan(&ready)
				if err == nil && !ready {
					err = &Error{503, "replication_unavailable", "storage groups are not ready under the recovery policy"}
				}
			}
		}
	}
	if err != nil {
		tx.Rollback()
		return nil, "", err
	}
	return tx, durability, nil
}

func (s *Store) InstallLease(ctx context.Context, lease Lease) error {
	if lease.Partition == "" || lease.Owner == "" || lease.Epoch < 1 || lease.ExpiresAt.IsZero() {
		return invalid("partition, owner, epoch and expires_at required")
	}
	tx, _, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// The database clock governs expiry. Same-epoch renewals cannot transfer an
	// owner, revive an expired lease or shorten its validity.
	result, err := tx.ExecContext(ctx, `INSERT INTO canonical.authorities AS a(partition,epoch,owner,expires_at)
	 SELECT $1,$2,$3,$4 WHERE $4 > clock_timestamp()
	 ON CONFLICT(partition) DO UPDATE SET epoch=excluded.epoch, owner=excluded.owner, expires_at=excluded.expires_at
	 WHERE excluded.epoch > a.epoch OR (excluded.epoch=a.epoch AND excluded.owner=a.owner
	 AND a.expires_at > clock_timestamp() AND excluded.expires_at >= a.expires_at)`, lease.Partition, lease.Epoch, lease.Owner, lease.ExpiresAt)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return conflict("stale_authority")
	}
	return tx.Commit()
}

func lockAuthority(ctx context.Context, tx *sql.Tx, p Principal, a Authority) error {
	var epoch int64
	var owner string
	var expires time.Time
	err := tx.QueryRowContext(ctx, `SELECT epoch,owner,expires_at FROM canonical.authorities WHERE partition=$1 FOR UPDATE`, a.Partition).Scan(&epoch, &owner, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return conflict("stale_authority")
	}
	if err != nil {
		return err
	}
	var valid bool
	if err = tx.QueryRowContext(ctx, `SELECT $1::timestamptz > clock_timestamp()`, expires).Scan(&valid); err != nil {
		return err
	}
	if epoch != a.Epoch || owner != p.Owner || !valid {
		return conflict("stale_authority")
	}
	return nil
}

func (s *Store) Put(ctx context.Context, p Principal, source, ref string, m Mutation) (Result, error) {
	if err := m.validate(p, source, ref); err != nil {
		return Result{}, err
	}
	tx, durability, err := s.begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	if err = lockAuthority(ctx, tx, p, m.Authority); err != nil {
		return Result{}, err
	}
	result, err := s.mutate(ctx, tx, durability, source, ref, m)
	if err != nil {
		return Result{}, err
	}
	if err = tx.Commit(); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (s *Store) mutate(ctx context.Context, tx *sql.Tx, durability, source, ref string, m Mutation) (Result, error) {
	key, digest := Key(source, ref), m.digest()
	// This coordinator lock also covers the first insert when there is no row
	// to lock yet, including competing partition assignments for one identity.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, key); err != nil {
		return Result{}, err
	}
	var event Event
	var oldVersion int64
	var oldDigest string
	var partition string
	var raw []byte
	// Citus rejects SELECT ... FOR UPDATE on a distributed table. The
	// coordinator advisory lock above already serializes writers by event_key.
	err := tx.QueryRowContext(ctx, `SELECT version,digest,event,partition FROM canonical.events WHERE event_key=$1`, key).Scan(&oldVersion, &oldDigest, &raw, &partition)
	created := errors.Is(err, sql.ErrNoRows)
	if err != nil && !created {
		return Result{}, err
	}
	if !created {
		if partition != m.Authority.Partition {
			return Result{}, conflict("partition_conflict")
		}
		if err = json.Unmarshal(raw, &event); err != nil {
			return Result{}, err
		}
		if m.Version < oldVersion {
			return Result{}, conflict("stale_version")
		}
		if m.Version == oldVersion {
			if digest != oldDigest {
				return Result{}, conflict("version_conflict")
			}
			// A timed-out COMMIT may be visible before its standby flush. Force
			// another WAL record on this shard before acknowledging any replay.
			_, err = tx.ExecContext(ctx, `UPDATE canonical.events SET confirmed_at=statement_timestamp() WHERE event_key=$1`, key)
			return Result{event.HazardID, m.Version, m.Operation, true, durability, false}, err
		}
	} else {
		id, err := uuid.NewV7()
		if err != nil {
			return Result{}, err
		}
		var now time.Time
		if err = tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return Result{}, err
		}
		event = Event{HazardID: id.String(), Source: source, SourceRefID: ref, IngestedAt: now.UTC()}
	}
	if m.Event != nil {
		e := m.Event
		event.HazardType, event.Severity, event.AreaName = e.HazardType, e.Severity, e.AreaName
		event.Latitude, event.Longitude = *e.Latitude, *e.Longitude
		event.OccurredAt, event.Attributes = e.OccurredAt, e.Attributes
	}
	raw, err = json.Marshal(event)
	if err != nil {
		return Result{}, err
	}
	// Concurrent claims from different partitions still meet at event_key.
	// A stored identity belongs permanently to its first assigned partition.
	_, err = tx.ExecContext(ctx, `INSERT INTO canonical.events(event_key,hazard_id,source,source_ref_id,version,operation,digest,event,ingested_at,partition)
	 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	 ON CONFLICT(event_key) DO UPDATE SET version=excluded.version,operation=excluded.operation,
	 digest=excluded.digest,event=excluded.event,confirmed_at=excluded.confirmed_at`, key, event.HazardID, source, ref, m.Version, m.Operation, digest, string(raw), event.IngestedAt, m.Authority.Partition)
	if err != nil {
		return Result{}, err
	}
	eventID := hash([]any{key, m.Version})
	payload := map[string]any{}
	if err = json.Unmarshal(raw, &payload); err != nil {
		return Result{}, err
	}
	payload["event_id"], payload["version"], payload["operation"], payload["correlation_id"] = eventID, m.Version, m.Operation, m.CorrelationID
	message, err := json.Marshal(payload)
	if err != nil {
		return Result{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO canonical.outbox(event_key,version,event_id,hazard_id,correlation_id,topic,payload) VALUES($1,$2,$3,$4,$5,$6,$7)`, key, m.Version, eventID, event.HazardID, m.CorrelationID, s.Topic, string(message))
	if err != nil {
		return Result{}, err
	}
	return Result{event.HazardID, m.Version, m.Operation, false, durability, created}, nil
}

type WindowItem struct {
	Source      string   `json:"source"`
	SourceRefID string   `json:"source_ref_id"`
	Mutation    Mutation `json:"mutation"`
}
type Window struct {
	Authority      Authority    `json:"authority"`
	WindowID       string       `json:"window_id"`
	PreviousCursor string       `json:"previous_cursor"`
	NextCursor     string       `json:"next_cursor"`
	Items          []WindowItem `json:"items"`
}
type Receipt struct {
	Receipt    string `json:"receipt"`
	Epoch      int64  `json:"epoch"`
	Durability string `json:"durability"`
}

// Seal atomically accepts a complete window (including empty windows) and its
// receipt. B must CAS its own cursor only after this call succeeds.
func (s *Store) Seal(ctx context.Context, p Principal, w Window) (Receipt, error) {
	if w.WindowID == "" || len(w.WindowID) > 256 || w.Authority.Epoch < 1 || len(w.Items) > 1000 || p.Assignments[w.Authority.Partition] == nil {
		return Receipt{}, invalid("invalid window")
	}
	digests := make([]string, 0, len(w.Items))
	for i := range w.Items {
		item := &w.Items[i]
		if item.Mutation.Authority != w.Authority {
			return Receipt{}, invalid("all mutations must use the window authority")
		}
		if err := item.Mutation.validate(p, item.Source, item.SourceRefID); err != nil {
			return Receipt{}, err
		}
		digests = append(digests, hash([]any{item.Source, item.SourceRefID, item.Mutation.Version, item.Mutation.digest()}))
	}
	digest := hash([]any{w.PreviousCursor, w.NextCursor, digests})
	tx, durability, err := s.begin(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer tx.Rollback()
	if err = lockAuthority(ctx, tx, p, w.Authority); err != nil {
		return Receipt{}, err
	}
	var previous string
	err = tx.QueryRowContext(ctx, `SELECT digest FROM canonical.windows WHERE partition=$1 AND window_id=$2`, w.Authority.Partition, w.WindowID).Scan(&previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, err
	}
	if err == nil && previous != digest {
		return Receipt{}, conflict("window_conflict")
	}
	sealed := err == nil
	// Replaying the event writes also establishes a fresh synchronous barrier.
	for _, item := range w.Items {
		if sealed {
			key := Key(item.Source, item.SourceRefID)
			if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, key); err != nil {
				return Receipt{}, err
			}
			var version int64
			var partition string
			err = tx.QueryRowContext(ctx, `SELECT version,partition FROM canonical.events WHERE event_key=$1`, key).Scan(&version, &partition)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return Receipt{}, err
			}
			if err == nil && partition != w.Authority.Partition {
				return Receipt{}, conflict("partition_conflict")
			}
			if err == nil && version >= item.Mutation.Version {
				if _, err = tx.ExecContext(ctx, `UPDATE canonical.events SET confirmed_at=statement_timestamp() WHERE event_key=$1`, key); err != nil {
					return Receipt{}, err
				}
				continue
			}
		}
		if _, err = s.mutate(ctx, tx, durability, item.Source, item.SourceRefID, item.Mutation); err != nil {
			return Receipt{}, err
		}
	}
	r := Receipt{hash([]any{w.Authority.Partition, w.WindowID, digest, w.Authority.Epoch}), w.Authority.Epoch, durability}
	_, err = tx.ExecContext(ctx, `INSERT INTO canonical.windows(partition,window_id,digest,epoch,receipt) VALUES($1,$2,$3,$4,$5)
	 ON CONFLICT(partition,window_id) DO UPDATE SET epoch=excluded.epoch,receipt=excluded.receipt`, w.Authority.Partition, w.WindowID, digest, w.Authority.Epoch, r.Receipt)
	if err != nil {
		return Receipt{}, err
	}
	if err = tx.Commit(); err != nil {
		return Receipt{}, err
	}
	return r, nil
}
