package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/segmentio/kafka-go"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Ring keeps the last N raw JSON gateway events.
type Ring struct {
	mu   sync.RWMutex
	buf  []json.RawMessage
	size int
	next int
	full bool
}

func NewRing(size int) *Ring {
	return &Ring{buf: make([]json.RawMessage, size), size: size}
}

func (r *Ring) Add(b []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]byte, len(b))
	copy(cp, b)
	r.buf[r.next] = json.RawMessage(cp)
	r.next = (r.next + 1) % r.size
	if r.next == 0 {
		r.full = true
	}
}

func (r *Ring) Snapshot() []json.RawMessage {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []json.RawMessage
	if !r.full {
		out = make([]json.RawMessage, r.next)
		copy(out, r.buf[:r.next])
		return out
	}
	out = make([]json.RawMessage, 0, r.size)
	for i := 0; i < r.size; i++ {
		idx := (r.next + i) % r.size
		out = append(out, r.buf[idx])
	}
	return out
}

// OutMsg is one SSE frame.
type OutMsg struct {
	Event string
	Data  []byte
}

// Hub fans out typed events to SSE subscribers.
type Hub struct {
	mu      sync.Mutex
	clients map[chan OutMsg]struct{}
}

func NewHub() *Hub {
	return &Hub{clients: make(map[chan OutMsg]struct{})}
}

func (h *Hub) Subscribe() chan OutMsg {
	ch := make(chan OutMsg, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *Hub) Unsubscribe(ch chan OutMsg) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
	close(ch)
}

func (h *Hub) Broadcast(msg OutMsg) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
			// drop if client is slow
		}
	}
}

// HostMetrics is a single CPU/RAM sample for Admin.
type HostMetrics struct {
	Type           string  `json:"type"`
	Time           string  `json:"time"`
	CPUPercent     float64 `json:"cpuPercent"`
	MemTotalMb     uint64  `json:"memTotalMb"`
	MemUsedMb      uint64  `json:"memUsedMb"`
	MemAvailableMb uint64  `json:"memAvailableMb"`
	MemUsedPercent float64 `json:"memUsedPercent"`
	SwapTotalMb    uint64  `json:"swapTotalMb"`
	SwapUsedMb     uint64  `json:"swapUsedMb"`
	Load1          float64 `json:"load1"`
	Load5          float64 `json:"load5"`
	Load15         float64 `json:"load15"`
}

type cpuSample struct {
	idle  uint64
	total uint64
}

func readCPUSample() (cpuSample, error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuSample{}, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		return cpuSample{}, fmt.Errorf("empty /proc/stat")
	}
	// cpu user nice system idle iowait irq softirq steal ...
	fields := strings.Fields(sc.Text())
	if len(fields) < 5 || fields[0] != "cpu" {
		return cpuSample{}, fmt.Errorf("bad cpu line")
	}
	var vals []uint64
	var sum uint64
	for _, f := range fields[1:] {
		v, err := strconv.ParseUint(f, 10, 64)
		if err != nil {
			break
		}
		vals = append(vals, v)
		sum += v
	}
	if len(vals) < 4 {
		return cpuSample{}, fmt.Errorf("short cpu line")
	}
	idle := vals[3]
	if len(vals) > 4 {
		idle += vals[4] // iowait
	}
	return cpuSample{idle: idle, total: sum}, nil
}

func readMemInfo() (total, available, swapTotal, swapFree uint64, err error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		kb, e := strconv.ParseUint(parts[1], 10, 64)
		if e != nil {
			continue
		}
		switch parts[0] {
		case "MemTotal:":
			total = kb
		case "MemAvailable:":
			available = kb
		case "SwapTotal:":
			swapTotal = kb
		case "SwapFree:":
			swapFree = kb
		}
	}
	return total, available, swapTotal, swapFree, sc.Err()
}

func readLoadAvg() (float64, float64, float64) {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, 0, 0
	}
	fields := strings.Fields(string(b))
	if len(fields) < 3 {
		return 0, 0, 0
	}
	a, _ := strconv.ParseFloat(fields[0], 64)
	b1, _ := strconv.ParseFloat(fields[1], 64)
	c, _ := strconv.ParseFloat(fields[2], 64)
	return a, b1, c
}

func sampleHostMetrics(prev *cpuSample) (HostMetrics, cpuSample) {
	nowCPU, err := readCPUSample()
	cpuPct := 0.0
	if err == nil && prev != nil && nowCPU.total > prev.total {
		totalDelta := float64(nowCPU.total - prev.total)
		idleDelta := float64(nowCPU.idle - prev.idle)
		if totalDelta > 0 {
			cpuPct = (1.0 - idleDelta/totalDelta) * 100.0
			if cpuPct < 0 {
				cpuPct = 0
			}
			if cpuPct > 100 {
				cpuPct = 100
			}
		}
	}
	totalKb, availKb, swapTotKb, swapFreeKb, _ := readMemInfo()
	usedKb := uint64(0)
	if totalKb > availKb {
		usedKb = totalKb - availKb
	}
	swapUsed := uint64(0)
	if swapTotKb > swapFreeKb {
		swapUsed = swapTotKb - swapFreeKb
	}
	memPct := 0.0
	if totalKb > 0 {
		memPct = float64(usedKb) / float64(totalKb) * 100.0
	}
	l1, l5, l15 := readLoadAvg()
	m := HostMetrics{
		Type:           "host.metrics",
		Time:           time.Now().UTC().Format(time.RFC3339Nano),
		CPUPercent:     float64(int(cpuPct*10)) / 10, // 1 decimal
		MemTotalMb:     totalKb / 1024,
		MemUsedMb:      usedKb / 1024,
		MemAvailableMb: availKb / 1024,
		MemUsedPercent: float64(int(memPct*10)) / 10,
		SwapTotalMb:    swapTotKb / 1024,
		SwapUsedMb:     swapUsed / 1024,
		Load1:          l1,
		Load5:          l5,
		Load15:         l15,
	}
	return m, nowCPU
}

func main() {
	port := env("PORT", "8097")
	bootstrap := env("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
	topic := env("KAFKA_TOPIC", "byz.gateway.access")
	group := env("KAFKA_GROUP", "admin-gateway-sse")
	jwksURL := env("IAM_JWKS_URL", "http://127.0.0.1:8082/.well-known/jwks.json")
	corsOrigins := env("CORS_ORIGINS", "http://localhost:4200,https://admin.byzantineapp.dev,https://sys.byzantineapp.dev")
	metricsEvery := env("HOST_METRICS_INTERVAL", "2s")
	interval, err := time.ParseDuration(metricsEvery)
	if err != nil || interval < time.Second {
		interval = 2 * time.Second
	}

	ring := NewRing(50)
	hub := NewHub()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	k, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURL})
	if err != nil {
		log.Fatalf("jwks: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(bootstrap, ","),
		Topic:          topic,
		GroupID:        group,
		StartOffset:    kafka.LastOffset,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		MaxWait:        time.Second,
	})
	defer reader.Close()

	go func() {
		log.Printf("kafka consumer group=%s topic=%s brokers=%s", group, topic, bootstrap)
		for {
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("kafka read: %v", err)
				time.Sleep(time.Second)
				continue
			}
			if len(m.Value) == 0 || !json.Valid(m.Value) {
				continue
			}
			ring.Add(m.Value)
			hub.Broadcast(OutMsg{Event: "gateway.request.completed", Data: append([]byte(nil), m.Value...)})
		}
	}()

	// Host CPU/RAM ticker for SSE
	go func() {
		prev, _ := readCPUSample()
		t := time.NewTicker(interval)
		defer t.Stop()
		// first sample after one interval so CPU % is meaningful
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m, now := sampleHostMetrics(&prev)
				prev = now
				b, err := json.Marshal(m)
				if err != nil {
					continue
				}
				hub.Broadcast(OutMsg{Event: "host.metrics", Data: b})
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	})
	mux.HandleFunc("/api/v1/gateway-access/recent", withCORS(corsOrigins, withJWT(k, handleRecent(ring))))
	mux.HandleFunc("/api/v1/gateway-access/stream", withCORS(corsOrigins, withJWT(k, handleStream(hub, ring))))
	mux.HandleFunc("/api/v1/host-metrics", withCORS(corsOrigins, withJWT(k, handleHostMetrics())))

	addr := "127.0.0.1:" + port
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Printf("admin-gateway-sse listening on http://%s (host metrics every %s)", addr, interval)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	shutdown, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	_ = srv.Shutdown(shutdown)
	log.Printf("shutdown")
}

func withCORS(origins string, next http.HandlerFunc) http.HandlerFunc {
	allowed := map[string]bool{}
	for _, o := range strings.Split(origins, ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			allowed[o] = true
		}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && allowed[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func withJWT(k keyfunc.Keyfunc, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, "Bearer ") {
			http.Error(w, `{"title":"Unauthorized","detail":"missing bearer token"}`, http.StatusUnauthorized)
			return
		}
		tokenStr := strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
		tok, err := jwt.Parse(tokenStr, k.Keyfunc,
			jwt.WithValidMethods([]string{"RS256"}),
			jwt.WithExpirationRequired(),
		)
		if err != nil || !tok.Valid {
			http.Error(w, `{"title":"Unauthorized","detail":"invalid token"}`, http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func handleRecent(ring *Ring) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		snap := ring.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"count":  len(snap),
			"events": snap,
		})
	}
}

func handleHostMetrics() http.HandlerFunc {
	var prev cpuSample
	var once sync.Once
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		once.Do(func() {
			prev, _ = readCPUSample()
			time.Sleep(200 * time.Millisecond)
		})
		m, now := sampleHostMetrics(&prev)
		prev = now
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m)
	}
}

func handleStream(hub *Hub, ring *Ring) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")

		// gateway request snapshot
		for _, ev := range ring.Snapshot() {
			_, _ = w.Write([]byte("event: snapshot\ndata: "))
			_, _ = w.Write(ev)
			_, _ = w.Write([]byte("\n\n"))
		}
		// immediate host metrics sample
		prev, _ := readCPUSample()
		time.Sleep(150 * time.Millisecond)
		m, _ := sampleHostMetrics(&prev)
		if b, err := json.Marshal(m); err == nil {
			_, _ = w.Write([]byte("event: host.metrics\ndata: "))
			_, _ = w.Write(b)
			_, _ = w.Write([]byte("\n\n"))
		}
		flusher.Flush()

		ch := hub.Subscribe()
		defer hub.Unsubscribe(ch)

		ctx := r.Context()
		ping := time.NewTicker(15 * time.Second)
		defer ping.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ping.C:
				_, _ = w.Write([]byte(": ping\n\n"))
				flusher.Flush()
			case msg, ok := <-ch:
				if !ok {
					return
				}
				_, _ = w.Write([]byte("event: " + msg.Event + "\ndata: "))
				_, _ = w.Write(msg.Data)
				_, _ = w.Write([]byte("\n\n"))
				flusher.Flush()
			}
		}
	}
}
