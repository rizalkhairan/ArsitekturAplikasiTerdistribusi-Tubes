package auth

import (
	"errors"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrNoToken      = errors.New("no bearer token provided")
	ErrInvalidToken = errors.New("invalid or expired token")
	ErrTokenExpired = errors.New("token expired")
)

// maxTokenAge is the maximum allowed age of a token (60 seconds).
var maxTokenAge = 60 * time.Second

// SetMaxTokenAge allows configuration of the maximum token age.
func SetMaxTokenAge(d time.Duration) {
	maxTokenAge = d
}

type Claims struct {
	ClientID string
	Scope    string
}

var jwtSecret []byte

// InitAuth initializes the auth package with the JWT secret.
func InitAuth(authServiceURL, secret string) {
	jwtSecret = []byte(secret)
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

	parsed, err := jwt.Parse(token, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return jwtSecret, nil
	})
	if err != nil {
		return nil, ErrInvalidToken
	}

	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok || !parsed.Valid {
		return nil, ErrInvalidToken
	}

	// Check token age
	if iat, ok := claims["iat"].(float64); ok {
		age := time.Since(time.Unix(int64(iat), 0))
		if age > maxTokenAge {
			return nil, ErrTokenExpired
		}
	}

	scope, _ := claims["scope"].(string)
	if scope == "" {
		scope = "summary"
	}

	name, _ := claims["name"].(string)

	return &Claims{
		ClientID: name,
		Scope:    scope,
	}, nil
}


