package auth

import (
	"errors"
	"strings"
	"time"

	"github.com/casdoor/casdoor-go-sdk/casdoorsdk"
)

var (
	ErrNoToken      = errors.New("no bearer token provided")
	ErrInvalidToken = errors.New("invalid or expired token")
	ErrScopeDenied  = errors.New("insufficient scope for this resource")
	ErrTokenExpired = errors.New("token expired (60s TTL enforced)")
)

// maxTokenAge is the maximum allowed age of a token (60 seconds).
// Configurable via env var for demo purposes.
var maxTokenAge = 60 * time.Second

// SetMaxTokenAge allows configuration of the maximum token age.
func SetMaxTokenAge(d time.Duration) {
	maxTokenAge = d
}

type Claims struct {
	ClientID string
	Scope    string
}

func ValidateToken(authHeader string) (*Claims, error) {
	if authHeader == "" {
		return nil, ErrNoToken
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return nil, ErrNoToken
	}

	token := strings.TrimSpace(parts[1])
	if token == "" {
		return nil, ErrNoToken
	}

	parsed, err := casdoorsdk.ParseJwtToken(token)
	if err != nil {
		return nil, ErrInvalidToken
	}

	// Enforce 60s TTL at the API layer (Casdoor's minimum granularity is 1 hour)
	iat := parsed.IssuedAt
	if iat != nil {
		age := time.Since(iat.Time)
		if age > maxTokenAge {
			return nil, ErrTokenExpired
		}
	}

	scope := parsed.Properties["scope"]
	if scope == "" {
		scope = "summary"
	}

	return &Claims{
		ClientID: parsed.Name,
		Scope:    scope,
	}, nil
}

func (c *Claims) CanAccessFull() bool {
	return c.Scope == "full"
}
