package main

import (
	"bufio"
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rizalkhairan/distributed-system/shared/telemetry"
)

type config struct {
	baseURL, token, statePath, addr, metricsAddr, canonicalURL, canonicalToken, partition, centroidsPath string
	startOffset, epoch                                                                                   int64
	batchSize                                                                                            int
	allowedLateness, reconnectMin, reconnectMax                                                          time.Duration
	testMode                                                                                             bool
	testToken                                                                                            string
}
type app struct {
	cfg                                                            config
	db                                                             *store
	httpClient                                                     *http.Client
	telemetry                                                      *telemetry.Runtime
	connected                                                      atomic.Bool
	ready                                                          atomic.Bool
	sourceLatest                                                   atomic.Int64
	reconnectCount                                                 atomic.Int64
	feedEvents, feedReconnects, canonicalWrites, canonicalFailures prometheus.Counter
	lagGauge, lateGauge, singletonGauge, p95Gauge                  prometheus.Gauge
	writeLatency                                                   prometheus.Histogram
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		resp, err := http.Get("http://127.0.0.1:8080/health")
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		_ = resp.Body.Close()
		return
	}
	cfg := loadConfig()
	if cfg.token == "" || cfg.canonicalURL == "" || cfg.canonicalToken == "" {
		log.Fatal("SATELLITE_TOKEN, CANONICAL_API_URL and CANONICAL_WRITE_TOKEN are required")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.statePath), 0750); err != nil {
		log.Fatal(err)
	}
	ps, err := os.ReadFile(cfg.centroidsPath)
	if err != nil {
		log.Fatalf("load province reference file: %v", err)
	}
	var provinces []province
	if err = json.Unmarshal(ps, &provinces); err != nil || len(provinces) == 0 {
		log.Fatal("province reference file must contain a non-empty JSON array")
	}
	for _, p := range provinces {
		if p.Name == "" || p.Latitude < -90 || p.Latitude > 90 || p.Longitude < -180 || p.Longitude > 180 {
			log.Fatal("province reference contains an invalid row")
		}
	}
	provinceData = ps
	db, err := openStore(cfg.statePath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.db.Close()
	if err := db.initializeStartOffset(context.Background(), cfg.startOffset); err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	tr, err := telemetry.FromEnv(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tr.Shutdown(c)
	}()
	a := newApp(cfg, db, tr)
	go a.feedLoop(ctx)
	go a.processLoop(ctx)
	go a.publishLoop(ctx)
	go a.refreshSession(ctx)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.health)
	mux.HandleFunc("GET /status", a.status)
	mux.HandleFunc("GET /dashboard", a.dashboard)
	if cfg.testMode {
		mux.HandleFunc("POST /__test/reset", a.testReset)
		mux.HandleFunc("GET /__test/state", a.testState)
	}
	server := &http.Server{Addr: cfg.addr, Handler: correlationMiddleware(tr, mux), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", tr.MetricsHandler())
	metricsServer := &http.Server{Addr: cfg.metricsAddr, Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server error: %v", err)
			stop()
		}
	}()
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(c)
		_ = metricsServer.Shutdown(c)
	}()
	log.Printf("satellite processor listening addr=%s", cfg.addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func loadConfig() config {
	return config{baseURL: strings.TrimRight(env("SATELLITE_BASE_URL", "https://satellite.naufarrel.dev"), "/"), token: os.Getenv("SATELLITE_TOKEN"), statePath: env("SATELLITE_STATE_PATH", "./data/satellite.db"), addr: env("SATELLITE_HTTP_ADDR", ":8080"), metricsAddr: env("METRICS_ADDR", ":9090"), canonicalURL: strings.TrimRight(os.Getenv("CANONICAL_API_URL"), "/"), canonicalToken: os.Getenv("CANONICAL_WRITE_TOKEN"), partition: env("CANONICAL_PARTITION", "SATELIT"), epoch: envInt64("CANONICAL_EPOCH", 1), startOffset: envInt64("SATELLITE_START_OFFSET", 0), batchSize: envInt("SATELLITE_BATCH_SIZE", 128), allowedLateness: envDuration("SATELLITE_ALLOWED_LATENESS", 6*time.Hour), reconnectMin: envDuration("SATELLITE_RECONNECT_MIN", time.Second), reconnectMax: envDuration("SATELLITE_RECONNECT_MAX", 30*time.Second), centroidsPath: env("PROVINCE_CENTROIDS_PATH", "./data/province-centroids.json"), testMode: os.Getenv("P4_TEST_MODE") == "true", testToken: os.Getenv("P4_TEST_TOKEN")}
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	v, e := strconv.Atoi(os.Getenv(k))
	if e != nil || v < 1 {
		return d
	}
	return v
}
func envInt64(k string, d int64) int64 {
	v, e := strconv.ParseInt(os.Getenv(k), 10, 64)
	if e != nil || v < 0 {
		return d
	}
	return v
}
func envDuration(k string, d time.Duration) time.Duration {
	v, e := time.ParseDuration(os.Getenv(k))
	if e != nil || v < 0 {
		return d
	}
	return v
}

func newApp(cfg config, db *store, tr *telemetry.Runtime) *app {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 5 * time.Second
	a := &app{cfg: cfg, db: db, httpClient: &http.Client{Transport: transport}, telemetry: tr}
	const svc = "satellite_processor"
	a.feedEvents = prometheus.NewCounter(prometheus.CounterOpts{Name: svc + "_feed_events_total", Help: "SSE feed events durably received."})
	a.feedReconnects = prometheus.NewCounter(prometheus.CounterOpts{Name: svc + "_feed_reconnects_total", Help: "SSE reconnect attempts."})
	a.canonicalWrites = prometheus.NewCounter(prometheus.CounterOpts{Name: svc + "_canonical_mutations_total", Help: "Canonical mutations acknowledged."})
	a.canonicalFailures = prometheus.NewCounter(prometheus.CounterOpts{Name: svc + "_canonical_write_failures_total", Help: "Canonical mutation attempts that failed."})
	a.lagGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: svc + "_processing_offset_lag", Help: "Durably ingested offsets not yet processed."})
	a.lateGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: svc + "_late_detections_total", Help: "Detections arriving behind the configured watermark."})
	a.singletonGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: svc + "_singleton_detections", Help: "Current singleton connected components."})
	a.p95Gauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: svc + "_input_to_canonical_p95_seconds", Help: "Rolling p95 from feed receipt to canonical acknowledgement."})
	a.writeLatency = prometheus.NewHistogram(prometheus.HistogramOpts{Name: svc + "_input_to_canonical_seconds", Help: "Feed receipt to canonical acknowledgement latency.", Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300}})
	tr.Registry.MustRegister(a.feedEvents, a.feedReconnects, a.canonicalWrites, a.canonicalFailures, a.lagGauge, a.lateGauge, a.singletonGauge, a.p95Gauge, a.writeLatency)
	return a
}

func (a *app) feedLoop(ctx context.Context) {
	backoff := a.cfg.reconnectMin
	for ctx.Err() == nil {
		last, err := a.db.sourceOffset(ctx)
		if err != nil {
			log.Printf("read durable satellite offset failed: %v", err)
			a.db.markError(ctx, err)
			sleep(ctx, backoff)
			continue
		}
		endpoint := a.cfg.baseURL + "/v1/stream?from=" + strconv.FormatInt(a.cfg.startOffset, 10)
		log.Printf("connecting to satellite stream after_offset=%d", last)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			a.db.markError(ctx, err)
			return
		}
		req.Header.Set("Authorization", "Bearer "+a.cfg.token)
		if last >= a.cfg.startOffset {
			req.Header.Set("Last-Event-ID", strconv.FormatInt(last, 10))
		}
		a.feedReconnects.Inc()
		a.reconnectCount.Add(1)
		resp, err := a.httpClient.Do(req)
		if err != nil {
			log.Printf("satellite stream connection failed: %v", err)
			a.connected.Store(false)
			a.db.markConnected(ctx, false, err.Error())
			sleep(ctx, backoff)
			backoff = nextBackoff(backoff, a.cfg.reconnectMax)
			continue
		}
		log.Printf("satellite stream response status=%d", resp.StatusCode)
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			err = fmt.Errorf("feed returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			log.Printf("satellite stream rejected: %v", err)
			a.connected.Store(false)
			a.db.markConnected(ctx, false, err.Error())
			sleep(ctx, backoff)
			backoff = nextBackoff(backoff, a.cfg.reconnectMax)
			continue
		}
		a.connected.Store(true)
		a.db.markConnected(ctx, true, "")
		backoff = a.cfg.reconnectMin
		pending := make([]feedEvent, 0, 32)
		flushPending := func() error {
			if len(pending) == 0 {
				return nil
			}
			batch := pending
			pending = make([]feedEvent, 0, 32)
			if err := a.db.persistFeedBatch(ctx, batch); err != nil {
				return err
			}
			a.feedEvents.Add(float64(len(batch)))
			return nil
		}
		err = readSSE(ctx, resp.Body, func(ev feedEvent) error {
			if ev.Flush {
				return flushPending()
			}
			if !ev.HasOffset || ev.Offset < 0 {
				return fmt.Errorf("SSE event has no non-negative integer id")
			}
			if ev.ReceivedAt.IsZero() {
				ev.ReceivedAt = time.Now().UTC()
			}
			pending = append(pending, ev)
			if len(pending) >= cap(pending) || strings.EqualFold(ev.Type, "heartbeat") {
				return flushPending()
			}
			return nil
		})
		if flushErr := flushPending(); flushErr != nil && err == nil {
			err = flushErr
		}
		resp.Body.Close()
		log.Printf("satellite stream closed: %s", errorString(err))
		a.connected.Store(false)
		a.db.markConnected(ctx, false, errorString(err))
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			a.db.markError(ctx, err)
		}
		sleep(ctx, backoff)
		backoff = nextBackoff(backoff, a.cfg.reconnectMax)
	}
}

func readSSE(ctx context.Context, r io.Reader, handle func(feedEvent) error) error {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 4096), 8*1024*1024)
	ev := feedEvent{Offset: -1}
	var data strings.Builder
	flush := func() error {
		if ev.Offset < 0 && data.Len() == 0 && ev.Type == "" {
			return nil
		}
		ev.Data = json.RawMessage(strings.TrimSuffix(data.String(), "\n"))
		if len(ev.Data) == 0 && ev.Type != "heartbeat" {
			return fmt.Errorf("empty SSE data at offset %d", ev.Offset)
		}
		if err := handle(ev); err != nil {
			return err
		}
		ev = feedEvent{Offset: -1}
		data.Reset()
		return nil
	}
	for s.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := s.Text()
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			if err := handle(feedEvent{Flush: true}); err != nil {
				return err
			}
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return fmt.Errorf("invalid SSE id %q", value)
			}
			ev.Offset = n
			ev.HasOffset = true
		case "event":
			ev.Type = value
		case "data":
			data.WriteString(value)
			data.WriteByte('\n')
		}
	}
	if err := s.Err(); err != nil {
		return err
	}
	if ev.Offset >= 0 || data.Len() > 0 || ev.Type != "" {
		return flush()
	}
	return io.EOF
}
func errorString(err error) string {
	if err == nil || errors.Is(err, io.EOF) {
		return ""
	}
	return err.Error()
}
func nextBackoff(d, max time.Duration) time.Duration {
	d *= 2
	if d > max {
		return max
	}
	return d
}
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (a *app) processLoop(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := a.db.processBatch(ctx, a.cfg.batchSize, a.cfg.allowedLateness)
			if err != nil {
				a.db.markError(ctx, err)
				a.ready.Store(false)
				continue
			}
			if n > 0 {
				a.ready.Store(true)
				a.db.markError(ctx, nil)
			}
			a.updateMetrics(ctx)
		}
	}
}
func (a *app) publishLoop(ctx context.Context) {
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		rec, err := a.db.nextOutbox(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			sleep(ctx, 100*time.Millisecond)
			continue
		}
		if err != nil {
			a.db.markError(ctx, err)
			sleep(ctx, backoff)
			continue
		}
		start := time.Now()
		err = a.publishMutation(ctx, rec)
		if err == nil {
			if e := a.db.markOutbox(ctx, rec.ID, ""); e != nil {
				err = e
			} else {
				latency := time.Since(rec.ReceivedAt)
				if rec.ReceivedAt.IsZero() || latency < 0 {
					latency = time.Since(start)
				}
				a.canonicalWrites.Inc()
				_ = a.db.addLatency(ctx, latency)
				a.writeLatency.Observe(latency.Seconds())
				backoff = 250 * time.Millisecond
			}
		}
		if err != nil {
			a.canonicalFailures.Inc()
			_ = a.db.markOutbox(ctx, rec.ID, err.Error())
			a.db.markError(ctx, err)
			sleep(ctx, backoff)
			backoff = nextBackoff(backoff, 10*time.Second)
		}
	}
}
func (a *app) publishMutation(ctx context.Context, rec outboxRecord) error {
	path := a.cfg.canonicalURL + "/internal/canonical/events/" + url.PathEscape(a.cfg.partition) + "/" + url.PathEscape(rec.Ref)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, path, strings.NewReader(string(rec.Body)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+a.cfg.canonicalToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("canonical API returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
func (a *app) refreshSession(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.baseURL+"/v1/session", nil)
			if err != nil {
				continue
			}
			req.Header.Set("Authorization", "Bearer "+a.cfg.token)
			sessionCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			req = req.WithContext(sessionCtx)
			resp, err := a.httpClient.Do(req)
			if err != nil {
				cancel()
				continue
			}
			var session struct {
				LatestOffset int64 `json:"latest_offset"`
			}
			if resp.StatusCode == http.StatusOK {
				_ = json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&session)
				a.sourceLatest.Store(session.LatestOffset)
				_, _ = a.db.db.ExecContext(ctx, `UPDATE feed_state SET latest_offset=MAX(latest_offset,?) WHERE id=1`, session.LatestOffset)
			}
			resp.Body.Close()
			cancel()
			a.updateMetrics(ctx)
		}
	}
}
func (a *app) updateMetrics(ctx context.Context) {
	status, err := a.db.status(ctx)
	if err != nil {
		return
	}
	a.lagGauge.Set(toFloat(status["processing_lag"]))
	a.lateGauge.Set(toFloat(status["late_detections"]))
	a.singletonGauge.Set(toFloat(status["singleton_detections"]))
	a.p95Gauge.Set(toFloat(status["p95_input_to_store_seconds"]))
}
func toFloat(v any) float64 {
	switch x := v.(type) {
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case float64:
		return x
	}
	return 0
}

func (a *app) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if _, err := a.db.status(ctx); err != nil {
		writeJSON(w, 503, map[string]any{"status": "unhealthy", "error": "state_store_unavailable"})
		return
	}
	writeJSON(w, 200, map[string]any{"status": "ok", "feed_connected": a.connected.Load()})
}
func (a *app) status(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	v, err := a.db.status(ctx)
	if err != nil {
		writeJSON(w, 503, map[string]any{"error": err.Error()})
		return
	}
	v["ready"] = a.ready.Load()
	v["reconnect_attempts"] = a.reconnectCount.Load()
	writeJSON(w, 200, v)
}

func (a *app) testAuth(r *http.Request) bool {
	if !a.cfg.testMode || a.cfg.testToken == "" {
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	return subtle.ConstantTimeCompare([]byte(got), []byte(a.cfg.testToken)) == 1
}

func (a *app) testReset(w http.ResponseWriter, r *http.Request) {
	if !a.testAuth(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	if err := a.db.resetForTest(r.Context(), a.cfg.startOffset); err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	a.ready.Store(false)
	writeJSON(w, 200, map[string]any{"reset": true, "source_offset": a.cfg.startOffset - 1})
}

func (a *app) testState(w http.ResponseWriter, r *http.Request) {
	if !a.testAuth(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	state, err := a.db.testState(r.Context())
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, state)
}

func (a *app) dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Satellite processor</title><style>body{font:16px system-ui;background:#101827;color:#edf2f7;margin:2rem}h1{font-size:1.6rem}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:1rem}.card{background:#1d2a3d;border-radius:12px;padding:1rem}.label{color:#a9bad0;font-size:.85rem}.value{font-size:1.6rem;margin-top:.4rem;overflow-wrap:anywhere}#error{color:#ff9d9d}</style></head><body><h1>Satellite feed processing</h1><p>Offset lag, event-time watermark, late arrivals, and durable output acknowledgements.</p><div id="grid" class="grid"></div><p id="error"></p><script>const labels={source_offset:'Durably received offset',processed_offset:'Processed offset',latest_offset:'Feed latest offset',ingest_lag:'Ingest lag',processing_lag:'Processing lag',watermark:'Event-time watermark',late_detections:'Late detections',singleton_detections:'Singleton components',active_fires:'Active fires',pending_canonical_mutations:'Pending canonical writes',p95_input_to_store_seconds:'p95 receipt to store (s)',feed_connected:'Feed connected',ready:'Processor ready'};async function tick(){try{const r=await fetch('/status',{cache:'no-store'});const d=await r.json();document.querySelector('#error').textContent=d.last_error||'';document.querySelector('#grid').innerHTML=Object.entries(labels).map(([k,l])=>'<div class="card"><div class="label">'+l+'</div><div class="value">'+(d[k]??'—')+'</div></div>').join('')}catch(e){document.querySelector('#error').textContent=String(e)}}tick();setInterval(tick,2000)</script></body></html>`)
}
func correlationMiddleware(tr *telemetry.Runtime, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(telemetry.CorrelationHeader)
		ctx := telemetry.WithCorrelation(r.Context(), id)
		w.Header().Set(telemetry.CorrelationHeader, telemetry.CorrelationID(ctx))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (a *app) unused() {}
