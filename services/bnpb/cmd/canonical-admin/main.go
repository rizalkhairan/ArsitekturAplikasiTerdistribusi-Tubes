// canonical-admin runs explicit schema initialization and read-only M1 import.
// The normal Aggregator never receives this command's administrator credential.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/canonical"
)

func main() {
	legacyPath := flag.String("sqlite", "", "quiescent M1 database to import read-only")
	flag.Parse()
	if flag.NArg() != 1 || (flag.Arg(0) != "migrate" && flag.Arg(0) != "import-m1") {
		log.Fatal("usage: canonical-admin [-sqlite PATH] migrate|import-m1 (CANONICAL_ADMIN_DSN required)")
	}
	dsn := os.Getenv("CANONICAL_ADMIN_DSN")
	if dsn == "" {
		log.Fatal("CANONICAL_ADMIN_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("invalid PostgreSQL configuration")
	}
	defer db.Close()
	if flag.Arg(0) == "import-m1" {
		if *legacyPath == "" {
			log.Fatal("-sqlite is required")
		}
		store := canonical.Store{DB: db, RequireReplication: true}
		n, err := store.ImportM1(ctx, *legacyPath)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("imported %d M1 events; IDs and outbox state preserved", n)
		return
	}
	err = canonical.Migrate(ctx, db)
	if err != nil {
		log.Fatal(fmt.Errorf("canonical schema migration: %w", err))
	}
	log.Print("canonical schema ready")
}
