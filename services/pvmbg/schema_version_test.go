package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSchemaVersionAdminSwitchesCompatibleAndBreakingModes(t *testing.T) {
	t.Setenv("ADMIN_USERNAME", "admin")
	t.Setenv("ADMIN_PASSWORD", "secret")
	for _, mode := range []string{"baseline", "additive", "rename", "type_change", "missing_required"} {
		r := httptest.NewRequest(http.MethodPost, "/admin/schema-version", strings.NewReader(`{"mode":"`+mode+`"}`))
		r.SetBasicAuth("admin", "secret")
		w := httptest.NewRecorder()
		schemaVersionHandler(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("mode %s returned %d: %s", mode, w.Code, w.Body.String())
		}
		if got := schemaMode.Load().(string); got != mode {
			t.Errorf("active schema mode=%q, want %q", got, mode)
		}
	}
}
