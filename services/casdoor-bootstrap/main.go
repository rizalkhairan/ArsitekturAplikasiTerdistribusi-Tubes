package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

// User passwords are stored in Casdoor's users database, not in env vars.
// These are the seed passwords for the three user categories.
var seedUsers = []struct {
	username string
	password string
	category string // media, field, internal
}{
	{"media-user", "media-pass-2026", "media"},
	{"field-user", "field-pass-2026", "field"},
	{"internal-user", "internal-pass-2026", "internal"},
}

func main() {
	endpoint := getEnv("CASDOOR_ENDPOINT", "http://casdoor:8000")

	waitForCasdoor(endpoint)

	// Login as admin to get access token
	adminToken := adminLogin(endpoint)

	// Create organization
	ensureOrganization(endpoint, adminToken, "bnpb", "BNPB Disaster Coordination")

	// Create application with 1-hour token expiry (Casdoor minimum)
	// Client-Facing API enforces 60s TTL at the API layer
	ensureApplication(endpoint, adminToken, "bnpb", "bnpb-client-api", "BNPB Client-Facing API")

	// Seed users only if SEED_USERS is enabled (default: true)
	if strings.EqualFold(getEnv("SEED_USERS", "true"), "true") {
		log.Println("Seeding users (SEED_USERS=true)...")
		for _, u := range seedUsers {
			ensureUser(endpoint, adminToken, "bnpb", u.username, u.password, u.category)
		}

		// Verify all users can authenticate
		for _, u := range seedUsers {
			if err := verifyUser(endpoint, u.username, u.password, u.category); err != nil {
				log.Fatalf("User '%s' verification failed: %v", u.username, err)
			}
			log.Printf("User '%s' (category=%s) verified successfully", u.username, u.category)
		}
		log.Println("User seeding complete: 3 users configured and verified")
	} else {
		log.Println("User seeding skipped (SEED_USERS=false)");
	}

	log.Println("Casdoor bootstrap complete")
}

func waitForCasdoor(endpoint string) {
	for i := 0; i < 90; i++ {
		resp, err := http.Get(endpoint + "/api/health")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			log.Println("Casdoor is ready")
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		log.Printf("Waiting for Casdoor... (%d/90)", i+1)
		time.Sleep(2 * time.Second)
	}
	log.Fatal("Casdoor did not become ready in time")
}

func adminLogin(endpoint string) string {
	username := getEnv("CASDOOR_ADMIN_USER", "admin")
	password := getEnv("CASDOOR_ADMIN_PASSWORD", "123")

	// Use built-in application for admin login (always exists)
	body, _ := json.Marshal(map[string]string{
		"username": username,
		"password": password,
		"clientId": "app-built-in",
	})

	resp, err := http.Post(endpoint+"/api/login", "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	var result struct {
		Status string `json:"status"`
		Data   struct {
			AccessToken string `json:"accessToken"`
		} `json:"data"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	if result.Status != "ok" {
		log.Fatalf("Admin login failed: %+v", result)
	}
	log.Println("Admin login successful")
	return result.Data.AccessToken
}

func ensureOrganization(endpoint, token, name, displayName string) {
	req, _ := http.NewRequest("GET", endpoint+"/api/get-organization?name="+name, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err == nil && resp.StatusCode == 200 {
		resp.Body.Close()
		log.Printf("Organization '%s' already exists", name)
		return
	}
	if resp != nil {
		resp.Body.Close()
	}

	payload := map[string]string{
		"owner":       "admin",
		"name":        name,
		"displayName": displayName,
	}
	body, _ := json.Marshal(payload)
	req, _ = http.NewRequest("POST", endpoint+"/api/create-organization", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	log.Printf("Organization '%s': %v", name, result["status"])
}

func ensureApplication(endpoint, token, org, name, displayName string) {
	req, _ := http.NewRequest("GET", endpoint+"/api/get-application?name="+name, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err == nil && resp.StatusCode == 200 {
		resp.Body.Close()
		log.Printf("Application '%s' already exists", name)
		return
	}
	if resp != nil {
		resp.Body.Close()
	}

	payload := map[string]any{
		"owner":               "admin",
		"name":                name,
		"createdTime":         time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
		"displayName":         displayName,
		"description":         "OAuth2 application for BNPB Client-Facing API",
		"redirectUris":        []string{"http://localhost:8080/callback"},
		"tokenFormat":         "JWT",
		"expireInHours":       1,
		"refreshExpireInHours": 24,
		"clientId":            "bnpb-client-id",
		"clientSecret":        "bnpb-client-secret-2026",
	}
	body, _ := json.Marshal(payload)
	req, _ = http.NewRequest("POST", endpoint+"/api/create-application", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	log.Printf("Application '%s': %v", name, result["status"])
}

func ensureUser(endpoint, token, org, username, password, category string) {
	// Check if existing
	req, _ := http.NewRequest("GET", endpoint+"/api/get-user?name="+username, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err == nil && resp.StatusCode == 200 {
		resp.Body.Close()
		log.Printf("User '%s' already exists", username)
		return
	}
	if resp != nil {
		resp.Body.Close()
	}

	// Map category to scope
	scope := "summary"
	if category == "field" || category == "internal" {
		scope = "full"
	}

	// Create user with category in properties
	payload := map[string]any{
		"owner":       org,
		"name":        username,
		"createdTime": time.Now().UTC().Format("2006-01-02T15:04:05Z07:00"),
		"displayName": username,
		"password":    password,
		"properties": map[string]string{
			"category": category,
			"scope":    scope,
		},
	}
	body, _ := json.Marshal(payload)
	req, _ = http.NewRequest("POST", endpoint+"/api/create-user", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	log.Printf("User '%s' (category=%s, scope=%s): %v", username, category, scope, result["status"])
}

func verifyUser(endpoint, username, password, category string) error {
	payload := map[string]string{
		"grant_type": "password",
		"username":   username,
		"password":   password,
	}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(endpoint+"/api/login/oauth/access_token", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	var result struct {
		Status string `json:"status"`
		Data   struct {
			AccessToken string `json:"accessToken"`
		} `json:"data"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	if result.Status != "ok" {
		return fmt.Errorf("login failed: %s", result.Status)
	}
	return nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
