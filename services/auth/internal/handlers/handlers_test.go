package handlers

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rizalkhairan/distributed-system/auth/internal/database"
)

func TestFieldCategoryGetsDedicatedWriteScope(t *testing.T) {
	h := NewHandler(nil, "test-secret", time.Minute, time.Hour)
	for _, tc := range []struct{ category, want string }{{"field", "field:write"}, {"media", ""}, {"internal", ""}} {
		raw, err := h.generateAccessToken(database.User{ID: "u", Username: "user", Category: tc.category, Scope: "full"})
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := jwt.Parse(raw, func(token *jwt.Token) (any, error) { return []byte("test-secret"), nil })
		if err != nil {
			t.Fatal(err)
		}
		claims := parsed.Claims.(jwt.MapClaims)
		got, _ := claims["write_scope"].(string)
		if got != tc.want {
			t.Errorf("category %q write_scope=%q, want %q", tc.category, got, tc.want)
		}
	}
}
