package canonical

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type Writer interface {
	Put(context.Context, Principal, string, string, Mutation) (Result, error)
	InstallLease(context.Context, Lease) error
	Seal(context.Context, Principal, Window) (Receipt, error)
}

func ParseCredentials(raw string) (map[string]Principal, error) {
	var credentials map[string]Principal
	if err := json.Unmarshal([]byte(raw), &credentials); err != nil {
		return nil, errors.New("CANONICAL_CREDENTIALS must be a JSON object of bearer tokens to principals")
	}
	if len(credentials) == 0 {
		return nil, errors.New("canonical credentials are required")
	}
	for token, p := range credentials {
		if len(token) < 32 || p.Owner == "" || (!p.Authority && len(p.Assignments) == 0) || (p.Authority && len(p.Assignments) > 0) {
			return nil, errors.New("each canonical credential needs a 32+ character token, owner and either authority scope or assignments")
		}
		for partition, sources := range p.Assignments {
			if partition == "" || len(sources) == 0 {
				return nil, errors.New("empty partition assignment")
			}
			for _, source := range sources {
				if !validSource(source) {
					return nil, errors.New("unknown assigned source")
				}
			}
		}
	}
	return credentials, nil
}

func Handler(store Writer, credentials map[string]Principal) http.Handler {
	mux := http.NewServeMux()
	respond := func(w http.ResponseWriter, status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	fail := func(w http.ResponseWriter, err error) {
		var e *Error
		if !errors.As(err, &e) {
			log.Printf("canonical write failed: %v", err)
			e = &Error{503, "storage_unavailable", "write outcome may be unknown; retry the same version/window"}
		}
		respond(w, e.Status, map[string]any{"error": map[string]string{"code": e.Code, "message": e.Message}})
	}
	decode := func(w http.ResponseWriter, r *http.Request, v any) error {
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
		d.DisallowUnknownFields()
		d.UseNumber()
		if err := d.Decode(v); err != nil {
			return invalid("invalid JSON envelope")
		}
		if err := d.Decode(&struct{}{}); err != io.EOF {
			return invalid("expected one JSON envelope")
		}
		return nil
	}
	type principalKey struct{}
	mux.HandleFunc("PUT /internal/canonical/events/{source}/{source_ref_id}", func(w http.ResponseWriter, r *http.Request) {
		var m Mutation
		if err := decode(w, r, &m); err != nil {
			fail(w, err)
			return
		}
		p := r.Context().Value(principalKey{}).(Principal)
		if p.Authority {
			fail(w, &Error{403, "forbidden", "authority credentials cannot write events"})
			return
		}
		result, err := store.Put(r.Context(), p, r.PathValue("source"), r.PathValue("source_ref_id"), m)
		if err != nil {
			fail(w, err)
			return
		}
		log.Printf("canonical mutation hazard_id=%s version=%d operation=%s replayed=%t durability=%s correlation_id=%s", result.HazardID, result.Version, result.Operation, result.Replayed, result.Durability, m.CorrelationID)
		status := http.StatusOK
		if result.Created {
			status = http.StatusCreated
		}
		respond(w, status, result)
	})
	mux.HandleFunc("PUT /internal/canonical/authority", func(w http.ResponseWriter, r *http.Request) {
		if !r.Context().Value(principalKey{}).(Principal).Authority {
			fail(w, &Error{403, "forbidden", "authority scope required"})
			return
		}
		var lease Lease
		if err := decode(w, r, &lease); err != nil {
			fail(w, err)
			return
		}
		if err := store.InstallLease(r.Context(), lease); err != nil {
			fail(w, err)
			return
		}
		respond(w, 200, map[string]any{"partition": lease.Partition, "epoch": lease.Epoch})
	})
	mux.HandleFunc("POST /internal/canonical/windows", func(w http.ResponseWriter, r *http.Request) {
		p := r.Context().Value(principalKey{}).(Principal)
		if p.Authority {
			fail(w, &Error{403, "forbidden", "producer scope required"})
			return
		}
		var window Window
		if err := decode(w, r, &window); err != nil {
			fail(w, err)
			return
		}
		receipt, err := store.Seal(r.Context(), p, window)
		if err != nil {
			fail(w, err)
			return
		}
		respond(w, 200, receipt)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		var principal Principal
		found := false
		if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			for expected, p := range credentials {
				if subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1 {
					principal, found = p, true
				}
			}
		}
		if !found {
			fail(w, &Error{401, "unauthorized", "valid service Bearer credential required"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 9*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(context.WithValue(ctx, principalKey{}, principal)))
	})
}
