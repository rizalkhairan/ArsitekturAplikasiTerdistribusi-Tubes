package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/canonical"
)

func main() {
	ctx := context.Background()
	store, err := canonical.Open(ctx, os.Getenv("CANONICAL_DSN"), "hazard-events")
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(1)
	}
	defer store.DB.Close()
	start := time.Now()
	store.Warm(ctx)
	fmt.Println("warm:", time.Since(start))

	const sql = `SELECT version, operation, event FROM canonical.events WHERE operation='upsert' ORDER BY ingested_at, hazard_id LIMIT 31`
	for i := 0; i < 6; i++ {
		start = time.Now()
		rows, err := store.DB.QueryContext(ctx, sql)
		n := 0
		if err == nil {
			for rows.Next() {
				var version int64
				var operation string
				var raw []byte
				_ = rows.Scan(&version, &operation, &raw)
				n++
			}
			rows.Close()
		}
		fmt.Printf("raw #%d rows=%d err=%v dur=%s\n", i, n, err, time.Since(start))
	}
	for i := 0; i < 6; i++ {
		start = time.Now()
		page, err := store.Query(ctx, canonical.Query{Limit: 30})
		fmt.Printf("query #%d items=%d err=%v dur=%s\n", i, len(page.Items), err, time.Since(start))
	}
}
