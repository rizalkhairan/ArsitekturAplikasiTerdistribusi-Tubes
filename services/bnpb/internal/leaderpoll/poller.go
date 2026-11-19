// Leader-elected BMKG/PVMBG pollers using coordinator checkpoints and canonical writes.
package leaderpoll

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/communication"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/coordclient"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/dlqclient"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/health"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/processing"
	"github.com/rizalkhairan/distributed-system/bnpb/internal/storeclient"
	"github.com/rizalkhairan/distributed-system/observability"
)

type Config struct {
	PollInterval    time.Duration
	PVMBGBatchLimit int
	BMKGBatchLimit  int
	MirrorLocal     bool
	KafkaTopic      string
	Holder          string
}

type Poller struct {
	cfg      Config
	sources  *communication.Clients
	coord    *coordclient.Client
	store    *storeclient.Client
	dlq      *dlqclient.Client
	pools    *database.Pools
	registry *health.Registry
	logger   *obs.Logger
}

func New(cfg Config, sources *communication.Clients, coord *coordclient.Client, store *storeclient.Client,
	dlq *dlqclient.Client, pools *database.Pools, registry *health.Registry, logger *obs.Logger) *Poller {
	return &Poller{cfg: cfg, sources: sources, coord: coord, store: store, dlq: dlq, pools: pools,
		registry: registry, logger: logger}
}

func (p *Poller) holder() string {
	if p.cfg.Holder != "" {
		return p.cfg.Holder
	}
	return "unknown"
}

type bmkgState struct {
	Seismic string `json:"seismic"`
	Tsunami string `json:"tsunami"`
	Seq     int64  `json:"seq"`
}

func (p *Poller) RunBMKG(ctx context.Context, lease coordclient.Lease) error {
	state := bmkgState{}
	if checkpoint, err := p.coord.Checkpoint(ctx, "BMKG"); err == nil && checkpoint.Cursor != "" {
		_ = json.Unmarshal([]byte(checkpoint.Cursor), &state)
	}
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if err := p.pollBMKG(ctx, lease, &state); err != nil {
			if errors.Is(err, coordclient.ErrStaleEpoch) || errors.Is(err, storeclient.ErrFenced) {
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			p.registry.RecordFailure("BMKG", err)
			p.logger.Warn(ctx, "bmkg_poll_failed", "error", err.Error(), "epoch", lease.Epoch)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (p *Poller) pollBMKG(ctx context.Context, lease coordclient.Lease, state *bmkgState) error {
	ctx, span := obs.StartSpan(ctx, "poll.bmkg")
	defer span.End(ctx)
	span.Set("epoch", lease.Epoch)
	if err := p.coord.Validate(ctx, "BMKG", p.holder(), lease.Epoch); err != nil {
		return err
	}
	cursors := database.Cursors{database.SeismicFeed: state.Seismic, database.TsunamiFeed: state.Tsunami}
	events, err := p.sources.BMKG.SeismicEvents(ctx, state.Seismic)
	if err != nil {
		return err
	}
	warnings, err := p.sources.BMKG.TsunamiWarnings(ctx, state.Tsunami)
	if err != nil {
		return err
	}
	// Same bounded-prefix rule as PVMBG: the source returns everything since the
	// cursor ordered by occurred_at, and a single transient failure would replay
	// an unbounded batch. Warnings stay whole: they are rare, and slicing them
	// independently could separate a warning from its event.
	if limit := p.cfg.BMKGBatchLimit; limit > 0 && len(events) > limit {
		events = events[:limit]
	}
	lookup := func(ctx context.Context, source, refID string) (database.HazardEvent, error) {
		event, err := p.store.Get(ctx, refID)
		if errors.Is(err, storeclient.ErrNotFound) {
			return database.HazardEvent{}, processing.ErrNotFound
		}
		return event, err
	}
	batch, err := processing.AggregateBMKGWithLookup(ctx, lookup, events, warnings, cursors)
	if err != nil {
		return err
	}
	if err := p.persist(ctx, lease, batch.Hazards, state.Seq+1, "BMKG"); err != nil {
		return err
	}
	state.Seismic = batch.Cursors[database.SeismicFeed]
	state.Tsunami = batch.Cursors[database.TsunamiFeed]
	state.Seq++
	if err := p.commit(ctx, lease, "BMKG", state); err != nil {
		return err
	}
	p.registry.RecordSuccess("BMKG")
	p.logger.Info(ctx, "bmkg_poll_committed", "epoch", lease.Epoch, "stored", len(batch.Hazards), "seq", state.Seq)
	return nil
}

type pvmbgState struct {
	Since string `json:"since"`
	Seq   int64  `json:"seq"`
}

func (p *Poller) RunPVMBG(ctx context.Context, lease coordclient.Lease) error {
	state := pvmbgState{}
	if checkpoint, err := p.coord.Checkpoint(ctx, "PVMBG"); err == nil && checkpoint.Cursor != "" {
		_ = json.Unmarshal([]byte(checkpoint.Cursor), &state)
	}
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if err := p.pollPVMBG(ctx, lease, &state); err != nil {
			if errors.Is(err, coordclient.ErrStaleEpoch) || errors.Is(err, storeclient.ErrFenced) {
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			p.registry.RecordFailure("PVMBG", err)
			p.logger.Warn(ctx, "pvmbg_poll_failed", "error", err.Error(), "epoch", lease.Epoch)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (p *Poller) pollPVMBG(ctx context.Context, lease coordclient.Lease, state *pvmbgState) error {
	ctx, span := obs.StartSpan(ctx, "poll.pvmbg")
	defer span.End(ctx)
	span.Set("epoch", lease.Epoch)
	if err := p.coord.Validate(ctx, "PVMBG", p.holder(), lease.Epoch); err != nil {
		return err
	}
	cursors := database.Cursors{database.VolcanicFeed: state.Since}
	reports, err := p.sources.PVMBG.VolcanicReports(ctx, state.Since)
	if err != nil {
		return err
	}
	// The source returns the whole backlog since the cursor, ordered by
	// reported_at. Persisting all of it in one pass means a single transient
	// write failure replays every already-stored report on the next tick; once
	// the backlog takes longer to replay than the gap between failures, the
	// checkpoint can never advance. Commit a bounded prefix per poll instead,
	// so every tick that stores anything also moves the cursor forward.
	if limit := p.cfg.PVMBGBatchLimit; limit > 0 && len(reports) > limit {
		reports = reports[:limit]
	}
	batch, err := processing.AggregatePVMBG(ctx, p.pools.Read, reports, cursors)
	if err != nil {
		return err
	}
	if len(batch.Failures) > 0 {
		if p.dlq == nil {
			return fmt.Errorf("PVMBG mapping failures require the ingest DLQ")
		}
		for _, failure := range batch.Failures {
			if err := p.dlq.ParkRaw(ctx, "PVMBG", failure.SourceRefID, failure.Raw, failure.Reason, failure.Retryable); err != nil {
				return fmt.Errorf("park PVMBG failure %s: %w", failure.SourceRefID, err)
			}
		}
	}
	if err := p.persist(ctx, lease, batch.Hazards, state.Seq+1, "PVMBG"); err != nil {
		return err
	}
	state.Since = batch.Cursors[database.VolcanicFeed]
	state.Seq++
	if err := p.commit(ctx, lease, "PVMBG", state); err != nil {
		return err
	}
	p.registry.RecordSuccess("PVMBG")
	p.logger.Info(ctx, "pvmbg_poll_committed", "epoch", lease.Epoch, "stored", len(batch.Hazards), "seq", state.Seq)
	return nil
}

func (p *Poller) persist(ctx context.Context, lease coordclient.Lease, hazards []database.HazardEvent, version int64, scope string) error {
	if err := p.coord.Validate(ctx, scope, p.holder(), lease.Epoch); err != nil {
		return err
	}
	for _, hazard := range hazards {
		if _, _, err := p.store.UpsertAuto(ctx, hazard, version, lease.Epoch, scope); err != nil {
			if errors.Is(err, storeclient.ErrFenced) {
				return err
			}
			return fmt.Errorf("store %s %s: %w", hazard.Source, hazard.SourceRefID, err)
		}
	}
	if p.cfg.MirrorLocal && p.pools != nil {
		// Mirror to SQLite only for the local Compose/Kafka demo path.
		cursors := database.Cursors{}
		if _, err := database.SaveHazards(ctx, p.pools.Write, hazards, cursors, p.cfg.KafkaTopic); err != nil {
			return fmt.Errorf("local mirror: %w", err)
		}
	}
	return nil
}

func (p *Poller) commit(ctx context.Context, lease coordclient.Lease, source string, state any) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if _, err := p.coord.CommitCheckpoint(ctx, source, lease.Epoch, string(raw)); err != nil {
		return err
	}
	return nil
}
