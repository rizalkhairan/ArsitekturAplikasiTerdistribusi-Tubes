package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type loginResp struct {
	Data struct {
		AccessToken string `json:"accessToken"`
	} `json:"data"`
}

func main() {
	base := flag.String("base", "http://client-api:8080", "base url")
	path := flag.String("path", "/hazards?limit=100", "request path")
	conc := flag.Int("concurrency", 50, "concurrent workers")
	dur := flag.Duration("duration", 60*time.Second, "duration")
	user := flag.String("user", "field-user", "username")
	pass := flag.String("pass", "field-pass-2026", "password")
	timeout := flag.Duration("timeout", 10*time.Second, "request timeout")
	label := flag.String("label", "run", "label")
	flag.Parse()

	client := &http.Client{Timeout: *timeout}
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = *conc + 10
	var mu sync.Mutex
	token := login(client, *base, *user, *pass)
	relogin := func() string {
		mu.Lock()
		defer mu.Unlock()
		token = login(client, *base, *user, *pass)
		return token
	}

	var total, ok, errs, throttled atomic.Int64
	var latMu sync.Mutex
	lats := make([]float64, 0, 100000)
	var statuses sync.Map

	deadline := time.Now().Add(*dur)
	var wg sync.WaitGroup
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				mu.Lock()
				t := token
				mu.Unlock()
				req, _ := http.NewRequest("GET", *base+*path, nil)
				req.Header.Set("Authorization", "Bearer "+t)
				start := time.Now()
				resp, err := client.Do(req)
				lat := time.Since(start).Seconds() * 1000
				total.Add(1)
				if err != nil {
					errs.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				c, _ := statuses.LoadOrStore(resp.StatusCode, new(int64))
				atomic.AddInt64(c.(*int64), 1)
				switch {
				case resp.StatusCode == 401:
					relogin()
					errs.Add(1)
				case resp.StatusCode == 429:
					throttled.Add(1)
				case resp.StatusCode >= 500:
					errs.Add(1)
				case resp.StatusCode == 200:
					ok.Add(1)
					latMu.Lock()
					lats = append(lats, lat)
					latMu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	sort.Float64s(lats)
	pct := func(p float64) float64 {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(p/100*float64(len(lats)-1))]
	}
	fmt.Printf("[%s] path=%s conc=%d dur=%s total=%d ok=%d throttled429=%d errors=%d rps=%.1f p50=%.1fms p95=%.1fms p99=%.1fms\n",
		*label, *path, *conc, *dur, total.Load(), ok.Load(), throttled.Load(), errs.Load(),
		float64(total.Load())/dur.Seconds(), pct(50), pct(95), pct(99))
	statuses.Range(func(k, v any) bool {
		fmt.Printf("  status %v: %d\n", k, *(v.(*int64)))
		return true
	})
}

func login(c *http.Client, base, user, pass string) string {
	b, _ := json.Marshal(map[string]string{"username": user, "password": pass})
	resp, err := c.Post(base+"/auth/login", "application/json", bytes.NewReader(b))
	if err != nil {
		fmt.Println("login err:", err)
		return ""
	}
	defer resp.Body.Close()
	var lr loginResp
	json.NewDecoder(resp.Body).Decode(&lr)
	return lr.Data.AccessToken
}
