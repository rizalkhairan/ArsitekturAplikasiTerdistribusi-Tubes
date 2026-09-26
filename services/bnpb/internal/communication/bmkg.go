package communication

import (
	"context"
	"fmt"
	"time"

	pb "github.com/rizalkhairan/distributed-system/bmkg/proto"
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
			return invoker(ctx, method, req, reply, cc, opts...)
		}))
	if err != nil {
		return nil, err
	}
	return &BMKGClient{conn: conn, api: pb.NewBMKGClient(conn)}, nil
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
