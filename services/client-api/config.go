package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/casdoor/casdoor-go-sdk/casdoorsdk"
	"github.com/rizalkhairan/distributed-system/client-api/internal/auth"
)

type config struct {
	Port              string
	BNPBURL           string
	UpstreamTimeout   time.Duration
	MaxConcurrent      int
	BNPBInternalKey   string
	MaxLimit          int
	CasdoorEndpoint   string
	CasdoorClientID   string
	CasdoorClientSecret string
}

func loadConfig() config {
	// Configure token TTL (default 60s, configurable for demo)
	if v := os.Getenv("TOKEN_TTL_SECONDS"); v != "" {
		if secs, err := time.ParseDuration(v + "s"); err == nil && secs > 0 {
			auth.SetMaxTokenAge(secs)
		}
	}

	return config{
		Port:                env("PORT", "8080"),
		BNPBURL:             env("BNPB_API_URL", "http://bnpb:8085"),
		UpstreamTimeout:     envDuration("UPSTREAM_TIMEOUT", 5*time.Second),
		MaxConcurrent:       envInt("MAX_CONCURRENT_REQUESTS", 100),
		BNPBInternalKey:     os.Getenv("BNPB_INTERNAL_KEY"),
		MaxLimit:            envInt("MAX_LIMIT", 100),
		CasdoorEndpoint:     env("CASDOOR_ENDPOINT", "http://casdoor:8000"),
		CasdoorClientID:     os.Getenv("CASDOOR_CLIENT_ID"),
		CasdoorClientSecret: os.Getenv("CASDOOR_CLIENT_SECRET"),
	}
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return fallback
}

func initCasdoor() error {
	endpoint := env("CASDOOR_ENDPOINT", "http://casdoor:8000")
	clientID := os.Getenv("CASDOOR_CLIENT_ID")
	clientSecret := os.Getenv("CASDOOR_CLIENT_SECRET")
	orgName := env("CASDOOR_ORG_NAME", "bnpb")
	appName := env("CASDOOR_APP_NAME", "bnpb-client-api")

	if clientID == "" || clientSecret == "" {
		return fmt.Errorf("CASDOOR_CLIENT_ID and CASDOOR_CLIENT_SECRET are required")
	}

	// Fetch certificate from Casdoor admin API at startup (with retry)
	var certificate string
	var err error
	for i := 0; i < 30; i++ {
		certificate, err = fetchCertificate(endpoint, clientID, clientSecret, orgName, appName)
		if err == nil {
			break
		}
		log.Printf("Waiting for Casdoor to be ready... (%d/30): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return fmt.Errorf("failed to fetch certificate after retries: %w", err)
	}

	casdoorsdk.InitConfig(endpoint, clientID, clientSecret, certificate, orgName, appName)
	return nil
}

func fetchCertificate(endpoint, clientID, clientSecret, orgName, appName string) (string, error) {
	// Login as admin to get access token
	adminUser := os.Getenv("CASDOOR_ADMIN_USER")
	adminPass := os.Getenv("CASDOOR_ADMIN_PASSWORD")
	if adminUser == "" {
		adminUser = "admin"
	}
	if adminPass == "" {
		adminPass = "123"
	}

	loginBody, _ := json.Marshal(map[string]string{
		"username": adminUser,
		"password": adminPass,
		"clientId": "app-built-in",
	})
	resp, err := http.Post(endpoint+"/api/login", "application/json", bytes.NewBuffer(loginBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var loginResult struct {
		Status string `json:"status"`
		Data   struct {
			AccessToken string `json:"accessToken"`
		} `json:"data"`
	}
	json.NewDecoder(resp.Body).Decode(&loginResult)
	if loginResult.Status != "ok" {
		return "", fmt.Errorf("admin login failed: %s", loginResult.Status)
	}

	// Get application details
	req, _ := http.NewRequest("GET", endpoint+"/api/get-application?name="+appName, nil)
	req.Header.Set("Authorization", "Bearer "+loginResult.Data.AccessToken)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var appResult struct {
		Status string `json:"status"`
		Data   struct {
			Cert string `json:"cert"`
		} `json:"data"`
	}
	json.NewDecoder(resp.Body).Decode(&appResult)
	if appResult.Status != "ok" {
		return "", fmt.Errorf("get application failed: %s", appResult.Status)
	}

	return appResult.Data.Cert, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}
