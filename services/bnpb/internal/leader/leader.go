// Package leader runs one leader-elected worker per data source.
package leader

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/coordclient"
	"github.com/rizalkhairan/distributed-system/observability"
)

type Metrics struct {
	Holder      *obs.GaugeVec
	Epoch       *obs.GaugeVec
	Transitions *obs.CounterVec
}

func NewMetrics(registry *obs.Registry) *Metrics {
	return &Metrics{
		Holder:      registry.Gauge("lease_holder", "1 when this instance holds the lease", "source", "instance"),
		Epoch:       registry.Gauge("lease_epoch", "Current epoch per source", "source"),
		Transitions: registry.Counter("lease_transitions_total", "Lease transitions", "source", "event"),
	}
}

type Runner struct {
	Source        string
	Holder        string
	Coord         *coordclient.Client
	TTL           time.Duration
	RenewInterval time.Duration
	SafetyMargin  time.Duration
	Metrics       *Metrics
	Logger        *obs.Logger
}

// Run waits for leadership, then runs work until the lease is lost or ctx ends.
// work must return promptly when its context is cancelled.
func (r *Runner) Run(ctx context.Context, work func(context.Context, coordclient.Lease) error) {
	for ctx.Err() == nil {
		lease, err := r.Coord.Acquire(ctx, r.Source, r.Holder, r.TTL)
		if errors.Is(err, coordclient.ErrLeaseHeld) {
			r.sleep(ctx, 2*time.Second)
			continue
		}
		if err != nil {
			if r.Logger != nil {
				r.Logger.Warn(ctx, "lease_acquire_failed", "source", r.Source, "error", err.Error())
			}
			r.sleep(ctx, 2*time.Second)
			continue
		}
		if r.Metrics != nil {
			r.Metrics.Holder.WithLabelValues(r.Source, r.Holder).Set(1)
			r.Metrics.Epoch.WithLabelValues(r.Source).Set(float64(lease.Epoch))
			r.Metrics.Transitions.WithLabelValues(r.Source, "acquired").Inc()
		}
		if r.Logger != nil {
			r.Logger.Info(ctx, "lease_acquired", "source", r.Source, "epoch", lease.Epoch, "instance", r.Holder)
		}

		workCtx, cancel := context.WithCancel(ctx)
		renewDone := make(chan struct{})
		go func() {
			defer close(renewDone)
			r.renewLoop(workCtx, cancel, lease)
		}()
		err = work(workCtx, lease)
		cancel()
		<-renewDone
		if r.Metrics != nil {
			r.Metrics.Holder.WithLabelValues(r.Source, r.Holder).Set(0)
			r.Metrics.Transitions.WithLabelValues(r.Source, "released").Inc()
		}
		if ctx.Err() == nil {
			// Voluntary handover on work failure or lease loss.
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = r.Coord.Release(releaseCtx, r.Source, r.Holder, lease.Epoch)
			releaseCancel()
			if r.Logger != nil && err != nil {
				r.Logger.Warn(ctx, "lease_work_ended", "source", r.Source, "epoch", lease.Epoch, "error", err.Error())
			}
			r.sleep(ctx, time.Second)
		}
	}
}

func (r *Runner) renewLoop(ctx context.Context, cancel context.CancelFunc, lease coordclient.Lease) {
	ticker := time.NewTicker(r.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, stop := context.WithTimeout(ctx, r.RenewInterval)
			renewed, err := r.Coord.Renew(renewCtx, r.Source, r.Holder, lease.Epoch, r.TTL)
			stop()
			if err != nil {
				// Self-fence: stop doing work immediately. The resource will also
				// reject us if we were suspended and the store moved on.
				if r.Metrics != nil {
					r.Metrics.Transitions.WithLabelValues(r.Source, "self_fenced").Inc()
				}
				if r.Logger != nil {
					r.Logger.Warn(ctx, "lease_renew_failed_self_fencing", "source", r.Source, "epoch", lease.Epoch, "error", err.Error())
				}
				cancel()
				return
			}
			if r.Metrics != nil {
				r.Metrics.Epoch.WithLabelValues(r.Source).Set(float64(renewed.Epoch))
			}
			// The store reports remaining TTL using its own mono clock. If we are
			// inside the safety margin, stop before a suspend could make us stale.
			if renewed.ExpiresInMs > 0 && time.Duration(renewed.ExpiresInMs)*time.Millisecond < r.SafetyMargin {
				if r.Metrics != nil {
					r.Metrics.Transitions.WithLabelValues(r.Source, "safety_margin").Inc()
				}
				cancel()
				return
			}
		}
	}
}

func (r *Runner) sleep(ctx context.Context, d time.Duration) {
	// Jitter avoids all replicas waking at the same instant.
	wait := d + time.Duration(rand.Int63n(int64(d)))
	select {
	case <-ctx.Done():
	case <-time.After(wait):
	}
}
