package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthorityValidateRejectsStaleHolder(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Internal-Key") != "token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path != "/v1/leases/BMKG" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		fmt.Fprint(w, `{"holder":"aggregator-a","epoch":7,"active":true,"expires_in_ms":9000}`)
	}))
	defer server.Close()

	authority := newAuthorityClient(server.URL, "token")
	if err := authority.Validate(context.Background(), "BMKG", "aggregator-a", 7); err != nil {
		t.Fatalf("current holder rejected: %v", err)
	}
	for _, tc := range []struct {
		name   string
		holder string
		epoch  int64
	}{
		{"another instance at the same epoch", "aggregator-b", 7},
		{"the old epoch", "aggregator-a", 6},
	} {
		if err := authority.Validate(context.Background(), "BMKG", tc.holder, tc.epoch); err == nil {
			t.Fatalf("%s was accepted", tc.name)
		}
	}
}

func TestAuthorityDisabledWithoutURL(t *testing.T) {
	var authority *authorityClient
	if err := authority.Validate(context.Background(), "BMKG", "anyone", 1); err != nil {
		t.Fatalf("a disabled authority must not block writes: %v", err)
	}
	if newAuthorityClient("", "token") != nil {
		t.Fatal("empty URL must disable fencing")
	}
}

func TestFencedSources(t *testing.T) {
	for _, source := range []string{"BMKG", "PVMBG", "SENSOR_PANTAI"} {
		if !fencedSource(source) {
			t.Fatalf("%s should be fenced", source)
		}
	}
	for _, source := range []string{"SATELIT", "P1_SYNTHETIC"} {
		if fencedSource(source) {
			t.Fatalf("%s must keep its own write path", source)
		}
	}
}
