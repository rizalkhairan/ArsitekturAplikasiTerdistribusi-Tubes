package communication

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	pb "github.com/rizalkhairan/distributed-system/bmkg/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type BMKGClient struct {
	conn *grpc.ClientConn
	api  pb.BMKGClient
}

func newBMKGClient(address, key string, timeout time.Duration) (*BMKGClient, error) {
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any,
			cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			ctx = metadata.AppendToOutgoingContext(ctx, "x-bmkg-key", key)

			// Propagate correlation ID
			corrID := getCorrelationIDFromContext(ctx)
			if corrID == "" {
				corrID = uuid.NewString()
			}
			ctx = metadata.AppendToOutgoingContext(ctx, "x-correlation-id", corrID)

			// Log latency
			start := time.Now()
			err := invoker(ctx, method, req, reply, cc, opts...)
			latency := time.Since(start)
			logStructured(map[string]any{
				"ts":         start.Format(time.RFC3339Nano),
				"corr_id":    corrID,
				"method":     method,
				"status":     200,
				"latency_ms": latency.Milliseconds(),
				"upstream":   "bmkg",
			})
			return err
		}))
	if err != nil {
		return nil, err
	}
	return &BMKGClient{conn: conn, api: pb.NewBMKGClient(conn)}, nil
}

func getCorrelationIDFromContext(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ids := md.Get("x-correlation-id"); len(ids) > 0 {
			return ids[0]
		}
	}
	return ""
}

func logStructured(data map[string]any) {
	b, err := json.Marshal(data)
	if err != nil {
		return
	}
	log.Print(string(b))
}

func (c *BMKGClient) Close() error {
	return c.conn.Close()
}

func (c *BMKGClient) Health(ctx context.Context) error {
	response, err := c.api.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		return err
	}
	if response.GetStatus() != "ok" {
		return fmt.Errorf("BMKG is not healthy")
	}
	return nil
}

func (c *BMKGClient) SeismicEvents(ctx context.Context, since string) ([]*pb.SeismicEvent, error) {
	response, err := c.api.GetSeismicEvents(ctx, &pb.GetSeismicEventsRequest{Since: since})
	if err != nil {
		return nil, err
	}
	return response.Events, nil
}

func (c *BMKGClient) TsunamiWarnings(ctx context.Context, since string) ([]*pb.TsunamiWarning, error) {
	response, err := c.api.GetTsunamiWarnings(ctx, &pb.GetTsunamiWarningsRequest{Since: since})
	if err != nil {
		return nil, err
	}
	return response.Warnings, nil
}
