package messaging

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/database"
)

type Publisher interface {
	Publish(context.Context, database.OutboxEvent) error
	Close()
}

type KafkaPublisher struct {
	client *kgo.Client
}

func NewKafkaPublisher(brokers []string, clientID string) (*KafkaPublisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	return &KafkaPublisher{client: client}, nil
}

func (p *KafkaPublisher) Publish(ctx context.Context, event database.OutboxEvent) error {
	started := time.Now()
	result := p.client.ProduceSync(ctx, &kgo.Record{
		Topic: event.Topic,
		Key:   []byte(event.HazardID),
		Value: event.Payload,
	})
	if err := result.FirstErr(); err != nil {
		log.Printf("kafka publish hazard_id=%s topic=%s outcome=failed correlation_id=%s latency=%s error=%v",
			event.HazardID, event.Topic, event.CorrelationID, time.Since(started), err)
		return fmt.Errorf("publish hazard %s to topic %s: %w", event.HazardID, event.Topic, err)
	}
	log.Printf("kafka publish hazard_id=%s topic=%s outcome=acknowledged correlation_id=%s latency=%s",
		event.HazardID, event.Topic, event.CorrelationID, time.Since(started))
	return nil
}

func (p *KafkaPublisher) Close() {
	p.client.Close()
}
