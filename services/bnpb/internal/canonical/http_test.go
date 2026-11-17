package canonical

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type recordingWriter struct{ writes, leases, windows int }

func (s *recordingWriter) Put(_ context.Context, _ Principal, _, _ string, _ Mutation) (Result, error) {
	s.writes++
	return Result{Created: true}, nil
}
func (s *recordingWriter) InstallLease(_ context.Context, _ Lease) error { s.leases++; return nil }
func (s *recordingWriter) Seal(_ context.Context, _ Principal, _ Window) (Receipt, error) {
	s.windows++
	return Receipt{}, nil
}

func TestHTTPAuthenticationAndEnvelope(t *testing.T) {
	producer, authority := strings.Repeat("p", 32), strings.Repeat("a", 32)
	writer := &recordingWriter{}
	h := Handler(writer, map[string]Principal{producer: {Owner: "producer", Assignments: map[string][]string{"SATELIT": {"SATELIT"}}}, authority: {Owner: "authority", Authority: true}})
	cases := []struct {
		method, path, token, body string
		status                    int
	}{
		{"PUT", "/internal/canonical/events/SATELIT/id", "", `{}`, 401},
		{"PUT", "/internal/canonical/events/SATELIT/id", authority, `{}`, 403},
		{"PUT", "/internal/canonical/authority", producer, `{}`, 403},
		{"POST", "/internal/canonical/windows", authority, `{}`, 403},
		{"PUT", "/internal/canonical/events/SATELIT/id", producer, `{"version":1,"extra":1}`, 400},
		{"PUT", "/internal/canonical/events/SATELIT/id", producer, `{} {}`, 400},
		{"PUT", "/internal/canonical/events/SATELIT/id", producer, `{}`, 201},
		{"PUT", "/internal/canonical/authority", authority, `{}`, 200},
	}
	for _, c := range cases {
		r := httptest.NewRequest(c.method, c.path, strings.NewReader(c.body))
		if c.token != "" {
			r.Header.Set("Authorization", "Bearer "+c.token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != c.status {
			t.Errorf("%s %s status %d want %d: %s", c.method, c.path, w.Code, c.status, w.Body)
		}
	}
	if writer.writes != 1 || writer.leases != 1 || writer.windows != 0 {
		t.Fatalf("unauthorized call reached storage: %+v", writer)
	}
}

func TestValidationAndIdentity(t *testing.T) {
	a := Authority{"SATELIT", 1}
	p := Principal{Assignments: map[string][]string{"SATELIT": {"SATELIT"}}}
	if Key("BMKG", "a:b") == Key("BMKG:a", "b") {
		t.Fatal("identity encoding collides")
	}
	for _, bad := range []string{"", "has/slash", string([]byte{0xff}), "has\x00null"} {
		m := mutation(a, 1)
		if err := m.validate(p, "SATELIT", bad); err == nil {
			t.Errorf("accepted reference %q", bad)
		}
	}
	m := mutation(a, 1)
	m.Event.Latitude = nil
	if err := m.validate(p, "SATELIT", "id"); err == nil {
		t.Fatal("accepted missing coordinate")
	}
	m = mutation(a, 1)
	m.Authority.Partition = "BMKG"
	var forbidden *Error
	if err := m.validate(p, "SATELIT", "id"); err == nil {
		t.Fatal("accepted unassigned partition")
	} else {
		forbidden = err.(*Error)
	}
	if forbidden.Status != http.StatusForbidden {
		t.Fatal(forbidden)
	}
}
