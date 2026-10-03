package handlers

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/rizalkhairan/distributed-system/auth/internal/database"
)

type Handler struct {
	db          *sql.DB
	jwtSecret   []byte
	accessTTL   time.Duration
	refreshTTL  time.Duration
}

func NewHandler(db *sql.DB, jwtSecret string, accessTTL, refreshTTL time.Duration) *Handler {
	return &Handler{
		db:         db,
		jwtSecret:  []byte(jwtSecret),
		accessTTL:  accessTTL,
		refreshTTL: refreshTTL,
	}
}

func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *Handler) HandleLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	user, err := database.FindUserByUsername(h.db, req.Username)
	if err != nil || user.Password != database.HashPassword(req.Password) {
		writeError(w, http.StatusUnauthorized, "invalid username or password")
		return
	}

	accessToken, err := h.generateAccessToken(user)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate token")
		return
	}

	refreshToken, err := h.generateRefreshToken(user)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate refresh token")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"data": map[string]any{
			"accessToken":  accessToken,
			"refreshToken": refreshToken,
		},
	})
}

func (h *Handler) HandleRefresh(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Look up the refresh token
	var tokenID, userID string
	var expiresAt string
	err := h.db.QueryRow(`
		SELECT token_id, user_id, expires_at FROM refresh_tokens WHERE token = ?
	`, req.RefreshToken).Scan(&tokenID, &userID, &expiresAt)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid refresh token")
		return
	}

	exp, err := time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil || time.Now().After(exp) {
		writeError(w, http.StatusUnauthorized, "refresh token expired")
		return
	}

	// Atomically revoke the old refresh token (rotation). The
	// revoked=0 predicate guarantees only one concurrent refresh wins.
	res, err := h.db.Exec(`UPDATE refresh_tokens SET revoked = 1 WHERE token_id = ? AND revoked = 0`, tokenID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to revoke old token")
		return
	}
	affected, err := res.RowsAffected()
	if err != nil || affected == 0 {
		writeError(w, http.StatusUnauthorized, "refresh token already used")
		return
	}

	// Get user
	user, err := database.FindUserByID(h.db, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "user not found")
		return
	}

	// Generate new tokens
	accessToken, err := h.generateAccessToken(user)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate token")
		return
	}

	refreshToken, err := h.generateRefreshToken(user)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate refresh token")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"data": map[string]any{
			"accessToken":  accessToken,
			"refreshToken": refreshToken,
		},
	})
}

func (h *Handler) generateAccessToken(user database.User) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":  user.ID,
		"name": user.Username,
		"scope": user.Scope,
		"iat":  now.Unix(),
		"exp":  now.Add(h.accessTTL).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(h.jwtSecret)
}

func (h *Handler) generateRefreshToken(user database.User) (string, error) {
	tokenID := uuid.NewString()
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}
	token := hex.EncodeToString(tokenBytes)

	expiresAt := time.Now().Add(h.refreshTTL)
	_, err := h.db.Exec(`
		INSERT INTO refresh_tokens (token_id, user_id, token, expires_at, revoked)
		VALUES (?, ?, ?, ?, 0)
	`, tokenID, user.ID, token, expiresAt.Format(time.RFC3339Nano))
	if err != nil {
		return "", err
	}
	return token, nil
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
