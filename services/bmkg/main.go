package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"time"

	pb "github.com/rizalkhairan/distributed-system/bmkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	_ "modernc.org/sqlite"
)

var db *sql.DB

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

	os.MkdirAll(filepath.Dir(dbPath), 0755)

	var err error
	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	initDatabase()
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

func initDatabase() {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS seismic_events (
			event_id TEXT PRIMARY KEY,
			magnitude REAL,
			depth_km REAL,
			epicenter_lat REAL,
			epicenter_lon REAL,
			region_name TEXT,
			occurred_at TEXT,
			potential_tsunami INTEGER
		);
		CREATE TABLE IF NOT EXISTS tsunami_warnings (
			warning_id TEXT PRIMARY KEY,
			related_event_id TEXT,
			threat_level TEXT,
			affected_zones TEXT,
			estimated_arrival TEXT
		);
	`)
	if err != nil {
		log.Fatal(err)
	}
}

func seedEvents() {
	var count int
	db.QueryRow("SELECT COUNT(*) FROM seismic_events").Scan(&count)

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

func insertEvent(event *pb.SeismicEvent) {
	_, err := db.Exec(`
		INSERT INTO seismic_events (
			event_id, magnitude, depth_km, epicenter_lat, epicenter_lon,
			region_name, occurred_at, potential_tsunami
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, event.EventId, event.Magnitude, event.DepthKm, event.EpicenterLat,
		event.EpicenterLon, event.RegionName, event.OccurredAt, event.PotentialTsunami)
	if err != nil {
		log.Println(err)
		return
	}

	if event.PotentialTsunami {
		levels := []string{"Waspada", "Siaga", "Awas"}
		zones, _ := json.Marshal([]string{event.RegionName})
		occurredAt, _ := time.Parse(time.RFC3339, event.OccurredAt)

		_, err := db.Exec(`
			INSERT INTO tsunami_warnings (
				warning_id, related_event_id, threat_level, affected_zones, estimated_arrival
			) VALUES (?, ?, ?, ?, ?)
		`, "tsunami-"+event.EventId, event.EventId, levels[rand.Intn(len(levels))],
			string(zones), occurredAt.Add(30*time.Minute).Format(time.RFC3339))
		if err != nil {
			log.Println(err)
		}
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

	md, _ := metadata.FromIncomingContext(ctx)
	keys := md.Get("x-bmkg-key")
	if len(keys) == 0 || keys[0] == "" || keys[0] != os.Getenv("BMKG_API_KEY") {
		return nil, status.Error(codes.Unauthenticated, "invalid BMKG key")
	}

	time.Sleep(time.Duration(50+rand.Intn(101)) * time.Millisecond)		// Delay sim
	return handler(ctx, req)
}

func (s *server) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Status: "ok"}, nil
}

func (s *server) GetSeismicEvents(ctx context.Context, req *pb.GetSeismicEventsRequest) (*pb.GetSeismicEventsResponse, error) {
	rows, err := db.Query(`
		SELECT event_id, magnitude, depth_km, epicenter_lat, epicenter_lon,
			region_name, occurred_at, potential_tsunami
		FROM seismic_events WHERE occurred_at >= ? ORDER BY occurred_at
	`, req.Since)
	if err != nil {
		return nil, status.Error(codes.Internal, "database error")
	}
	defer rows.Close()

	response := &pb.GetSeismicEventsResponse{}
	for rows.Next() {
		event := &pb.SeismicEvent{}
		rows.Scan(&event.EventId, &event.Magnitude, &event.DepthKm,
			&event.EpicenterLat, &event.EpicenterLon, &event.RegionName,
			&event.OccurredAt, &event.PotentialTsunami)
		response.Events = append(response.Events, event)
	}
	return response, rows.Err()
}

func (s *server) GetTsunamiWarnings(ctx context.Context, req *pb.GetTsunamiWarningsRequest) (*pb.GetTsunamiWarningsResponse, error) {
	rows, err := db.Query(`
		SELECT warning_id, related_event_id, threat_level, affected_zones, estimated_arrival
		FROM tsunami_warnings WHERE estimated_arrival >= ? ORDER BY estimated_arrival
	`, req.Since)
	if err != nil {
		return nil, status.Error(codes.Internal, "database error")
	}
	defer rows.Close()

	response := &pb.GetTsunamiWarningsResponse{}
	for rows.Next() {
		warning := &pb.TsunamiWarning{}
		var zones string
		rows.Scan(&warning.WarningId, &warning.RelatedEventId,
			&warning.ThreatLevel, &zones, &warning.EstimatedArrival)
		json.Unmarshal([]byte(zones), &warning.AffectedZones)
		response.Warnings = append(response.Warnings, warning)
	}
	return response, rows.Err()
}
