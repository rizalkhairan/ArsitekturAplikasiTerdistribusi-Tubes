package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestValidateTokenCarriesWriteScopeSeparatelyFromReadScope(t *testing.T) {
	InitAuth("", "test-secret")
	SetMaxTokenAge(time.Minute)
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"name": "field-user", "scope": "full", "write_scope": "field:write", "iat": time.Now().Unix(), "exp": time.Now().Add(time.Minute).Unix()})
	raw, err := token.SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	claims, err := ValidateToken("Bearer " + raw)
	if err != nil {
		t.Fatal(err)
	}
	if claims.Scope != "full" || claims.WriteScope != "field:write" {
		t.Fatalf("claims = %+v", claims)
	}
}
