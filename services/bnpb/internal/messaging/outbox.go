package messaging

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

type OutboxPublisher struct {
	DB           *sql.DB
	Publisher    Publisher
	RetryLimit   int
	RetryBackoff time.Duration
	PollInterval time.Duration
}

func (p OutboxPublisher) Run(ctx context.Context) {
	ticker := time.NewTicker(p.PollInterval)
	defer ticker.Stop()
	p.publishPending(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.publishPending(ctx)
		}
	}
}

func (p OutboxPublisher) publishPending(ctx context.Context) {
	events, err := database.PendingOutboxEvents(ctx, p.DB, 100)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("outbox query failed: %v", err)
		}
		return
	}
	for _, event := range events {
		if err := p.publishOne(ctx, event); err != nil {
			if ctx.Err() == nil {
				log.Printf("outbox publish failed hazard_id=%s topic=%s outcome=pending correlation_id=%s error=%v",
					event.HazardID, event.Topic, event.CorrelationID, err)
			}
			continue
		}
		log.Printf("outbox publish succeeded hazard_id=%s topic=%s outcome=published correlation_id=%s",
			event.HazardID, event.Topic, event.CorrelationID)
	}
}

func (p OutboxPublisher) publishOne(ctx context.Context, event database.OutboxEvent) error {
	var err error
	for attempt := 1; attempt <= p.RetryLimit; attempt++ {
		err = p.Publisher.Publish(ctx, event)
		if err == nil {
			return database.MarkOutboxEventPublished(ctx, p.DB, event.EventID)
		}
		if attempt < p.RetryLimit {
			timer := time.NewTimer(p.RetryBackoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return err
}
