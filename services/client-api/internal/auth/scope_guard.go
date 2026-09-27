package auth

import (
	"net/http"
	"strings"
)

// RejectRawFieldRequest checks if a media-scope client is explicitly requesting
// raw (non-summary) fields. Returns true if the request should be rejected.
func RejectRawFieldRequest(r *http.Request, scope string) bool {
	if scope != "summary" {
		return false
	}

	// Check query parameter: ?fields=latitude,longitude or ?raw=true
	fields := r.URL.Query().Get("fields")
	if fields != "" {
		requested := strings.Split(fields, ",")
		for _, f := range requested {
			f = strings.TrimSpace(f)
			if isRawField(f) {
				return true
			}
		}
	}

	// Check header: X-Raw-Fields: true
	if strings.EqualFold(r.Header.Get("X-Raw-Fields"), "true") {
		return true
	}

	// Check query parameter: ?include=attributes,coordinates
	include := r.URL.Query().Get("include")
	if include != "" {
		requested := strings.Split(include, ",")
		for _, f := range requested {
			f = strings.TrimSpace(f)
			if isRawField(f) {
				return true
			}
		}
	}

	return false
}

func isRawField(field string) bool {
	rawFields := []string{
		"latitude", "longitude", "coordinates",
		"attributes", "source_ref_id", "ingested_at",
	}
	for _, raw := range rawFields {
		if strings.EqualFold(field, raw) {
			return true
		}
	}
	return false
}
