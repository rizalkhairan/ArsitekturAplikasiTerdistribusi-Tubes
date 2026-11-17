package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rizalkhairan/distributed-system/bnpb/internal/canonical"
)

func TestCanonicalAndLegacyCredentialsAreSeparate(t *testing.T) {
	token := strings.Repeat("w", 32)
	s := NewCanonicalServer(":0", &canonical.Store{}, map[string]canonical.Principal{
		token: {Owner: "producer", Assignments: map[string][]string{"SATELIT": {"SATELIT"}}},
	}, "legacy-read", nil, time.Second, nil)
	for _, c := range []struct {
		method, path, key, bearer string
		status                    int
	}{
		{"GET", "/hazards", "", token, 401},
		{"GET", "/internal/canonical/events", "", token, 401},
		{"HEAD", "/internal/canonical/events", "", token, 401},
		{"GET", "/internal/canonical/events?limit=bad", "legacy-read", "", 400},
		{"GET", "/internal/canonical/events?source=unknown", "legacy-read", "", 400},
		{"GET", "/internal/canonical/events?cursor=bad", "legacy-read", "", 400},
		{"PUT", "/internal/canonical/events/SATELIT/id", "legacy-read", "", 401},
		{"PUT", "/internal/canonical/events/SATELIT/id", "", token, 400},
	} {
		r := httptest.NewRequest(c.method, c.path, strings.NewReader(`{"authority":{"partition":"SATELIT","epoch":1}}`))
		r.Header.Set("X-Internal-Key", c.key)
		if c.bearer != "" {
			r.Header.Set("Authorization", "Bearer "+c.bearer)
		}
		w := httptest.NewRecorder()
		s.server.Handler.ServeHTTP(w, r)
		if w.Code != c.status {
			t.Errorf("%s %s: status=%d want=%d", c.method, c.path, w.Code, c.status)
		}
	}
}

func TestCanonicalQueryHTTPAndHazardsCompatibility(t *testing.T) {
	dsn := os.Getenv("CANONICAL_TEST_DSN")
	if dsn == "" {
		t.Skip("disposable Citus integration only")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewCanonicalServer(":0", &canonical.Store{DB: db}, nil, "read-key", nil, time.Second, nil)
	request := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("X-Internal-Key", "read-key")
		w := httptest.NewRecorder()
		s.server.Handler.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
		return w
	}
	w := request("/internal/canonical/events?limit=1")
	var page canonical.Page
	if err = json.Unmarshal(w.Body.Bytes(), &page); err != nil || page.Partial || len(page.Items) != 1 || page.NextCursor == nil {
		t.Fatalf("page: %+v %v", page, err)
	}
	request("/internal/canonical/events?limit=1000&cursor=" + *page.NextCursor)
	w = request("/hazards?since=2999-01-01T00:00:00Z&limit=1")
	if strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatal("legacy array changed", w.Body.String())
	}
	r := httptest.NewRequest("GET", "/hazards/confirmed-absent-id", nil)
	r.Header.Set("X-Internal-Key", "read-key")
	w = httptest.NewRecorder()
	s.server.Handler.ServeHTTP(w, r.WithContext(context.Background()))
	if w.Code != 404 {
		t.Fatal("confirmed absence status", w.Code, w.Body.String())
	}
}
