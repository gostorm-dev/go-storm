package storm

import (
	"bufio"
	"context"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SystemStats is a snapshot of generator system metrics.
type SystemStats struct {
	Timestamp  time.Time
	CPUUsage   float64
	MemoryMB   float64
	HeapMB     float64
	Goroutines int
	GCCycles   uint32
	GCPauseNS  uint64
	FDCount    int
}

// Monitor samples system stats in the background.
type Monitor struct {
	mu         sync.RWMutex
	current    SystemStats
	samples    []SystemStats
	maxSamples int
	interval   time.Duration

	prevUTicks uint64
	prevSTicks uint64
	prevTime   time.Time
	numCPU     float64

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// NewMonitor creates a system monitor.
func NewMonitor(interval time.Duration, maxSamples int) *Monitor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Monitor{
		interval:   interval,
		maxSamples: maxSamples,
		samples:    make([]SystemStats, 0, maxSamples),
		numCPU:     float64(runtime.NumCPU()),
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
}

// Start begins background sampling.
func (m *Monitor) Start() {
	go m.loop()
}

// Stop halts sampling and waits for the goroutine to exit.
func (m *Monitor) Stop() {
	m.cancel()
	<-m.done
}

// Stats returns the most recent system snapshot.
func (m *Monitor) Stats() SystemStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

// Samples returns a copy of all collected samples.
func (m *Monitor) Samples() []SystemStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SystemStats, len(m.samples))
	copy(out, m.samples)
	return out
}

func (m *Monitor) loop() {
	defer close(m.done)

	m.sample()

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.sample()
		}
	}
}

func (m *Monitor) sample() {
	now := time.Now()

	stats := SystemStats{Timestamp: now}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	stats.MemoryMB = float64(mem.Sys) / 1024 / 1024
	stats.HeapMB = float64(mem.HeapInuse) / 1024 / 1024
	stats.GCCycles = mem.NumGC
	stats.GCPauseNS = mem.PauseTotalNs

	stats.Goroutines = runtime.NumGoroutine()

	stats.CPUUsage = m.sampleCPU(now)

	stats.FDCount = sampleFDs()

	m.mu.Lock()
	m.current = stats
	m.samples = append(m.samples, stats)
	if len(m.samples) > m.maxSamples {
		m.samples = m.samples[1:]
	}
	m.mu.Unlock()
}

func (m *Monitor) sampleCPU(now time.Time) float64 {
	utime, stime, ok := readProcStat()
	if !ok {
		return 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.prevTime.IsZero() {
		m.prevUTicks = utime
		m.prevSTicks = stime
		m.prevTime = now
		return 0
	}

	ticksDelta := (utime - m.prevUTicks) + (stime - m.prevSTicks)
	timeDelta := now.Sub(m.prevTime).Seconds()

	m.prevUTicks = utime
	m.prevSTicks = stime
	m.prevTime = now

	if timeDelta <= 0 {
		return 0
	}

	cpuPercent := (float64(ticksDelta) / timeDelta / 100.0) / m.numCPU * 100.0

	if cpuPercent > 100 {
		cpuPercent = 100
	}
	if cpuPercent < 0 {
		cpuPercent = 0
	}
	return cpuPercent
}

func readProcStat() (utime, stime uint64, ok bool) {
	f, err := openProcStat()
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Scan()
	line := scanner.Text()

	idx := strings.LastIndex(line, ")")
	if idx < 0 {
		return 0, 0, false
	}
	fields := strings.Fields(line[idx+1:])
	if len(fields) < 12 {
		return 0, 0, false
	}

	u, err1 := strconv.ParseUint(fields[11], 10, 64)
	s, err2 := strconv.ParseUint(fields[12], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return u, s, true
}
