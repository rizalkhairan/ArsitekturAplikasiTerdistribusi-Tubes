package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	pb "github.com/rizalkhairan/distributed-system/bmkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedBMKGServer
}

func main() {
	port := os.Getenv("PORT")
	dbPath := os.Getenv("DB_PATH")
	if port == "" {
		port = "8081"
	}
	if dbPath == "" {
		dbPath = "/app/data/bmkg.db"
	}

	var err error
	db, err = openDatabase(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	seedEvents()
	go generateEvents()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(authInterceptor))
	pb.RegisterBMKGServer(grpcServer, &server{})

	log.Printf("BMKG gRPC listening on :%s", port)
	log.Fatal(grpcServer.Serve(listener))
}

func seedEvents() {
	count, err := countEvents()
	if err != nil {
		log.Fatal(err)
	}

	now := time.Now().UTC()
	for i := count + 1; i <= 20; i++ {
		occurredAt := now.Add(-time.Duration(21-i) * time.Hour)
		magnitude := 4 + float64((i-1)%4) + rand.Float64()
		insertEvent(mockEvent(occurredAt, magnitude))
	}
}

func mockEvent(occurredAt time.Time, magnitude float64) *pb.SeismicEvent {
	return &pb.SeismicEvent{
		EventId:          fmt.Sprintf("bmkg-%d", occurredAt.UnixNano()),
		Magnitude:        magnitude,
		DepthKm:          10 + rand.Float64()*50,
		EpicenterLat:     -9.0,
		EpicenterLon:     110.0,
		RegionName:       "Selatan Jawa",
		OccurredAt:       occurredAt.UTC().Format(time.RFC3339),
		PotentialTsunami: magnitude >= 7,
	}
}

func generateEvents() {
	ticker := time.NewTicker(10 * time.Second)
	for now := range ticker.C {
		event := mockEvent(now, 4+rand.Float64()*4)
		insertEvent(event)
		log.Printf("generated %s", event.EventId)
	}
}

func authInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod == "/bmkg.BMKG/Health" {
		return handler(ctx, req)
	}

	start := time.Now()
	md, _ := metadata.FromIncomingContext(ctx)

	// Get or generate correlation ID
	corrID := getCorrelationIDFromMetadata(md)
	if corrID == "" {
		corrID = generateCorrelationID()
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-correlation-id", corrID)

	// Auth check
	keys := md.Get("x-bmkg-key")
	if len(keys) == 0 || keys[0] == "" || keys[0] != os.Getenv("BMKG_API_KEY") {
		latency := time.Since(start)
		logStructured(map[string]any{
			"ts":         start.Format(time.RFC3339Nano),
			"corr_id":    corrID,
			"method":     info.FullMethod,
			"status":     401,
			"latency_ms": latency.Milliseconds(),
			"error":      "unauthorized",
		})
		return nil, status.Error(codes.Unauthenticated, "invalid BMKG key")
	}

	time.Sleep(time.Duration(50+rand.Intn(101)) * time.Millisecond) // Delay sim
	resp, err := handler(ctx, req)

	latency := time.Since(start)
	logStatus := 200
	if err != nil {
		logStatus = 500
	}
	logStructured(map[string]any{
		"ts":         start.Format(time.RFC3339Nano),
		"corr_id":    corrID,
		"method":     info.FullMethod,
		"status":     logStatus,
		"latency_ms": latency.Milliseconds(),
	})

	return resp, err
}

func generateCorrelationID() string {
	return fmt.Sprintf("bmkg-%d", time.Now().UnixNano())
}

func logStructured(data map[string]any) {
	b, err := json.Marshal(data)
	if err != nil {
		return
	}
	log.Print(string(b))
}

func getCorrelationIDFromMetadata(md metadata.MD) string {
	ids := md.Get("x-correlation-id")
	if len(ids) > 0 && ids[0] != "" {
		return ids[0]
	}
	return ""
}

func (s *server) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Status: "ok"}, nil
}

func (s *server) GetSeismicEvents(ctx context.Context, req *pb.GetSeismicEventsRequest) (*pb.GetSeismicEventsResponse, error) {
	events, err := listSeismicEvents(ctx, req.Since)
	if err != nil {
		return nil, status.Error(codes.Internal, "database error")
	}
	return &pb.GetSeismicEventsResponse{Events: events}, nil
}

func (s *server) GetTsunamiWarnings(ctx context.Context, req *pb.GetTsunamiWarningsRequest) (*pb.GetTsunamiWarningsResponse, error) {
	warnings, err := listTsunamiWarnings(ctx, req.Since)
	if err != nil {
		return nil, status.Error(codes.Internal, "database error")
	}
	return &pb.GetTsunamiWarningsResponse{Warnings: warnings}, nil
}
