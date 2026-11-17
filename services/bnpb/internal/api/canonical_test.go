package api

import (
	"net/http/httptest"
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
