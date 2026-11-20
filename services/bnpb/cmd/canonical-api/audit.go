package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/canonical"
)

// auditEvents is the Citus-backed implementation of the Audit API event
// contract. Query already returns ascending ingested_at order, partial
// coverage and opaque next_cursor values.
func (s *server) auditEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	if raw := q.Get("limit"); raw != "" {
		value, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || value < 1 || value > 1000 {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": map[string]string{"code": "invalid_request", "message": "limit must be 1..1000"},
			})
			return
		}
		limit = value
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	page, err := s.store.Query(ctx, canonical.Query{
		Source: q.Get("source"), Since: q.Get("since"), Limit: limit, Cursor: q.Get("cursor"),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	if page.Items == nil {
		page.Items = []canonical.Event{}
	}
	if page.UnavailableUnits == nil {
		page.UnavailableUnits = []string{}
	}
	writeJSON(w, http.StatusOK, page)
}

// auditChecksum traverses the same cursor stream as the Audit API and hashes
// source_ref_id values in byte order, so it cannot disagree with /events.
func (s *server) auditChecksum(w http.ResponseWriter, r *http.Request) {
	source := r.URL.Query().Get("source")
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()

	// Fast path: one distributed merge scan. The cursor traversal below is
	// kept for when a placement is unreachable, where it can mark the exact
	// unavailable units instead of failing the whole checksum.
	if ids, ok := s.checksumIDs(ctx, source); ok {
		writeChecksum(w, source, ids, false, nil)
		return
	}

	ids := []string{}
	units := []string{}
	seenUnits := map[string]bool{}
	cursor := ""
	partial := false
	for {
		page, err := s.store.Query(ctx, canonical.Query{
			Source: source, Limit: 1000, Cursor: cursor,
		})
		if err != nil {
			writeError(w, err)
			return
		}
		for _, event := range page.Items {
			ids = append(ids, event.SourceRefID)
		}
		if page.Partial {
			partial = true
		}
		for _, unit := range page.UnavailableUnits {
			if !seenUnits[unit] {
				seenUnits[unit] = true
				units = append(units, unit)
			}
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}

	writeChecksum(w, source, ids, partial, units)
}

// checksumIDs returns the surviving source_ref_id values in one SQL merge.
// It uses the same identity as the event traversal (source + source_ref_id,
// highest version wins, retractions dropped) and reports ok=false so the
// caller can fall back to the partial-capable cursor path.
func (s *server) checksumIDs(ctx context.Context, source string) ([]string, bool) {
	fastCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	rows, err := s.store.DB.QueryContext(fastCtx, `SELECT source_ref_id FROM (
		SELECT source_ref_id, operation,
		       row_number() OVER (PARTITION BY source, source_ref_id ORDER BY version DESC) AS rn
		FROM canonical.events WHERE ($1 = '' OR source = $1)
	) merged WHERE rn = 1 AND operation = 'upsert' ORDER BY source_ref_id`, source)
	if err != nil {
		log.Printf("canonical checksum fast path unavailable (%v); falling back to cursor traversal", err)
		return nil, false
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			log.Printf("canonical checksum fast path scan failed (%v); falling back to cursor traversal", err)
			return nil, false
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		log.Printf("canonical checksum fast path iteration failed (%v); falling back to cursor traversal", err)
		return nil, false
	}
	return ids, true
}

func writeChecksum(w http.ResponseWriter, source string, ids []string, partial bool, units []string) {
	sort.Strings(ids)
	digest := sha256.New()
	for index, id := range ids {
		if index > 0 {
			_, _ = digest.Write([]byte("\n"))
		}
		_, _ = digest.Write([]byte(id))
	}
	sort.Strings(units)
	if units == nil {
		units = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"source":            source,
		"count":             len(ids),
		"checksum":          hex.EncodeToString(digest.Sum(nil)),
		"algorithm":         "sha256",
		"as_of":             time.Now().UTC().Format(time.RFC3339Nano),
		"partial":           partial,
		"unavailable_units": units,
	})
}

type auditLocationRow struct {
	Node string `json:"node"`
	Role string `json:"role"`
	Unit string `json:"unit"`
}

// auditLocation reports the Citus placements for one hazard. Citus metadata
// names workers by Service; citus-node-map maps those names to Kubernetes node
// names without embedding deployment-specific names in the source.
func (s *server) auditLocation(w http.ResponseWriter, r *http.Request) {
	hazardID := r.URL.Query().Get("hazard_id")
	if hazardID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]string{"code": "invalid_request", "message": "hazard_id is required"},
		})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	var eventKey string
	err := s.store.DB.QueryRowContext(ctx,
		`SELECT event_key FROM canonical.events WHERE hazard_id=$1 AND operation='upsert'`, hazardID).Scan(&eventKey)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": map[string]string{"code": "not_found", "message": "hazard not found"},
		})
		return
	}
	if err != nil {
		log.Printf("audit location event lookup hazard=%s: %v", hazardID, err)
		writeError(w, err)
		return
	}

	var shardID int64
	if err = s.store.DB.QueryRowContext(ctx,
		`SELECT get_shard_id_for_distribution_column('canonical.events', $1::text)`, eventKey).Scan(&shardID); err != nil {
		log.Printf("audit location shard lookup hazard=%s: %v", hazardID, err)
		writeError(w, err)
		return
	}

	rows, err := s.store.DB.QueryContext(ctx, `
		SELECT n.nodename
		FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid)
		WHERE p.shardid=$1 AND p.shardstate=1 AND n.isactive
		ORDER BY n.groupid`, shardID)
	if err != nil {
		log.Printf("audit location placement lookup hazard=%s shard=%d: %v", hazardID, shardID, err)
		writeError(w, err)
		return
	}
	defer rows.Close()

	locations := []auditLocationRow{}
	for rows.Next() {
		var node string
		if err = rows.Scan(&node); err != nil {
			log.Printf("audit location scan hazard=%s shard=%d: %v", hazardID, shardID, err)
			writeError(w, err)
			return
		}
		role := "primary"
		unit := fmt.Sprintf("shard-%d", shardID)
		if len(locations) > 0 {
			role = "replica"
			unit = fmt.Sprintf("shard-%d-r%d", shardID, len(locations))
		}
		locations = append(locations, auditLocationRow{
			Node: mapCitusNode(node), Role: role, Unit: unit,
		})
	}
	if err = rows.Err(); err != nil {
		writeError(w, err)
		return
	}
	if len(locations) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": map[string]string{"code": "storage_unavailable", "message": "no active placement for hazard"},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"hazard_id": hazardID,
		"locations": locations,
		"partial":   false,
	})
}

func mapCitusNode(service string) string {
	key := "CITUS_" + strings.ToUpper(strings.ReplaceAll(strings.TrimPrefix(service, "citus-"), "-", "_"))
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	// Keep the mapping usable even when envFrom is not injected (for example
	// after a manual rollout on an older kubelet). The ConfigMap volume is
	// optional; fallback remains the Citus Service name.
	if raw, err := os.ReadFile("/etc/citus-node-map/" + key); err == nil {
		if value := strings.TrimSpace(string(raw)); value != "" {
			return value
		}
	}
	return service
}
