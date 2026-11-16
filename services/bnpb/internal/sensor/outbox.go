package sensor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/coordclient"
	"github.com/rizalkhairan/distributed-system/observability"
)

// Deliverer drains the critical-notification outbox. It is not leader-gated:
// any Aggregator replica may deliver, and claims make the work exclusive enough
// to avoid a delivery storm. Delivery is at-least-once; the receiver dedupes.
type Deliverer struct {
	coord      *coordclient.Client
	notifier   string
	token      string
	instance   string
	metrics    *Metrics
	logger     *obs.Logger
	http       *http.Client
	claimLease time.Duration
}

func NewDeliverer(coord *coordclient.Client, notifierURL, token, instance string, metrics *Metrics, logger *obs.Logger) *Deliverer {
	return &Deliverer{
		coord: coord, notifier: strings.TrimRight(notifierURL, "/"), token: token, instance: instance,
		metrics: metrics, logger: logger, claimLease: 15 * time.Second,
		http: &http.Client{Timeout: 5 * time.Second},
	}
}

func (d *Deliverer) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.drain(ctx)
		}
	}
}

func (d *Deliverer) drain(ctx context.Context) {
	entries, err := d.coord.Claim(ctx, d.instance, 50, d.claimLease)
	if err != nil {
		if ctx.Err() == nil && d.logger != nil {
			d.logger.Warn(ctx, "outbox_claim_failed", "error", err.Error())
		}
		return
	}
	for _, entry := range entries {
		if err := d.deliver(ctx, entry); err != nil {
			retry := time.Duration(1<<min(entry.Attempts, 6)) * time.Second
			_ = d.coord.Nack(ctx, entry.ID, err.Error(), retry)
			if d.metrics != nil {
				d.metrics.OutboxDelivered.WithLabelValues(Source, "retry").Inc()
			}
			if d.logger != nil {
				d.logger.Warn(ctx, "critical_alert_retry", "entry_id", entry.ID, "attempt", entry.Attempts, "error", err.Error())
			}
			continue
		}
		_ = d.coord.Ack(ctx, entry.ID)
		if d.metrics != nil {
			d.metrics.OutboxDelivered.WithLabelValues(Source, "delivered").Inc()
		}
		if d.logger != nil {
			d.logger.Info(ctx, "critical_alert_delivered", "entry_id", entry.ID, "dedupe_key", entry.DedupeKey)
		}
	}
}

func (d *Deliverer) deliver(ctx context.Context, entry coordclient.OutboxEntry) error {
	ctx, span := obs.StartSpan(ctx, "notify.critical")
	defer span.End(ctx)
	span.Set("entry_id", entry.ID)
	span.Set("dedupe_key", entry.DedupeKey)
	raw, err := json.Marshal(entry.Payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.notifier+"/notify", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", entry.DedupeKey)
	if d.token != "" {
		req.Header.Set("Authorization", "Bearer "+d.token)
	}
	obs.InjectHeaders(ctx, req.Header)
	resp, err := d.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("notifier HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// Permanent rejection: retrying cannot fix a malformed payload. Ack it so a
		// poison message does not block the outbox; the metric keeps it visible.
		if d.metrics != nil {
			d.metrics.OutboxDelivered.WithLabelValues(Source, "permanent_reject").Inc()
		}
		return nil
	default:
		return fmt.Errorf("notifier unexpected HTTP %d", resp.StatusCode)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
