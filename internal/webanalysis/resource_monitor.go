package webanalysis

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	targetCPULoadPct        = 70.0
	targetMemoryUsagePct    = 80.0
	targetBandwidthUsagePct = 90.0
	maxParallelWorkersCap   = 48
	minParallelWorkers      = 1
)

type resourceSnapshot struct {
	CPUUsagePct       float64
	MemoryUsagePct    float64
	BandwidthUsagePct float64
	UpdatedAt         time.Time
	HasCPU            bool
	HasMemory         bool
	HasBandwidth      bool
}

type resourceMonitor struct {
	mu           sync.RWMutex
	snapshot     resourceSnapshot
	prevCPUIdle  uint64
	prevCPUTotal uint64
	prevNetBytes map[string]uint64
	prevNetAt    time.Time
	stopCh       chan struct{}
	stoppedCh    chan struct{}
}

func newResourceMonitor() *resourceMonitor {
	m := &resourceMonitor{
		prevNetBytes: make(map[string]uint64),
		stopCh:       make(chan struct{}),
		stoppedCh:    make(chan struct{}),
	}
	m.collect()
	go m.loop()
	return m
}

func (m *resourceMonitor) loop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	defer close(m.stoppedCh)

	for {
		select {
		case <-ticker.C:
			m.collect()
		case <-m.stopCh:
			return
		}
	}
}

func (m *resourceMonitor) stop() {
	close(m.stopCh)
	<-m.stoppedCh
}

func (m *resourceMonitor) snapshotNow() resourceSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snapshot
}

func (m *resourceMonitor) recommendedConcurrency(maxConcurrency int) (int, resourceSnapshot) {
	if maxConcurrency < minParallelWorkers {
		maxConcurrency = minParallelWorkers
	}
	snap := m.snapshotNow()

	headroom := 1.0
	hasAnyMetric := false

	if snap.HasCPU {
		hasAnyMetric = true
		headroom = minFloat(headroom, remainingRatio(targetCPULoadPct, snap.CPUUsagePct))
	}
	if snap.HasMemory {
		hasAnyMetric = true
		headroom = minFloat(headroom, remainingRatio(targetMemoryUsagePct, snap.MemoryUsagePct))
	}
	if snap.HasBandwidth {
		hasAnyMetric = true
		headroom = minFloat(headroom, remainingRatio(targetBandwidthUsagePct, snap.BandwidthUsagePct))
	}

	if !hasAnyMetric {
		return minInt(maxConcurrency, fallbackParallelWorkers()), snap
	}

	if headroom <= 0 {
		return minParallelWorkers, snap
	}

	target := int(float64(maxConcurrency) * headroom)
	if target < minParallelWorkers {
		target = minParallelWorkers
	}
	if target > maxConcurrency {
		target = maxConcurrency
	}
	return target, snap
}

func fallbackParallelWorkers() int {
	cpu := runtime.NumCPU()
	if cpu <= 4 {
		return 2
	}
	if cpu <= 16 {
		return 4
	}
	workers := cpu / 4
	if workers > maxParallelWorkersCap {
		return maxParallelWorkersCap
	}
	if workers < 4 {
		return 4
	}
	return workers
}

func calculateMaxParallelWorkers(fileCount int) int {
	if fileCount <= 1 {
		return 1
	}
	cpuBased := runtime.NumCPU() / 4
	if cpuBased < 4 {
		cpuBased = 4
	}
	if cpuBased > maxParallelWorkersCap {
		cpuBased = maxParallelWorkersCap
	}
	if fileCount < cpuBased {
		return fileCount
	}
	return cpuBased
}

func (m *resourceMonitor) collect() {
	snap := resourceSnapshot{
		UpdatedAt: time.Now(),
	}

	if cpu, ok := m.readCPUUsage(); ok {
		snap.CPUUsagePct = cpu
		snap.HasCPU = true
	}
	if mem, ok := readMemoryUsage(); ok {
		snap.MemoryUsagePct = mem
		snap.HasMemory = true
	}
	if bw, ok := m.readBandwidthUsage(); ok {
		snap.BandwidthUsagePct = bw
		snap.HasBandwidth = true
	}

	m.mu.Lock()
	m.snapshot = snap
	m.mu.Unlock()
}

func (m *resourceMonitor) readCPUUsage() (float64, bool) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, false
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) == 0 {
		return 0, false
	}
	fields := strings.Fields(lines[0])
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, false
	}

	var total uint64
	values := make([]uint64, 0, len(fields)-1)
	for _, field := range fields[1:] {
		v, parseErr := strconv.ParseUint(field, 10, 64)
		if parseErr != nil {
			return 0, false
		}
		values = append(values, v)
		total += v
	}
	idle := values[3]
	if len(values) > 4 {
		idle += values[4]
	}

	if m.prevCPUTotal == 0 {
		m.prevCPUTotal = total
		m.prevCPUIdle = idle
		return 0, false
	}

	totalDelta := total - m.prevCPUTotal
	idleDelta := idle - m.prevCPUIdle
	m.prevCPUTotal = total
	m.prevCPUIdle = idle
	if totalDelta == 0 {
		return 0, false
	}
	usage := (1 - float64(idleDelta)/float64(totalDelta)) * 100
	return clampFloat(usage, 0, 100), true
}

func readMemoryUsage() (float64, bool) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer file.Close()

	var memTotal uint64
	var memAvailable uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			memTotal, _ = strconv.ParseUint(fields[1], 10, 64)
		case "MemAvailable:":
			memAvailable, _ = strconv.ParseUint(fields[1], 10, 64)
		}
	}
	if memTotal == 0 || memAvailable > memTotal {
		return 0, false
	}
	usage := (1 - float64(memAvailable)/float64(memTotal)) * 100
	return clampFloat(usage, 0, 100), true
}

func (m *resourceMonitor) readBandwidthUsage() (float64, bool) {
	file, err := os.Open("/proc/net/dev")
	if err != nil {
		return 0, false
	}
	defer file.Close()

	now := time.Now()
	scanner := bufio.NewScanner(file)
	current := make(map[string]uint64)
	maxUsage := 0.0
	valid := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "Inter-") || strings.HasPrefix(line, "face") {
			continue
		}
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			continue
		}
		iface := strings.TrimSpace(parts[0])
		if iface == "lo" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 16 {
			continue
		}
		rxBytes, err1 := strconv.ParseUint(fields[0], 10, 64)
		txBytes, err2 := strconv.ParseUint(fields[8], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		currentTotal := rxBytes + txBytes
		current[iface] = currentTotal

		prevTotal, ok := m.prevNetBytes[iface]
		if !ok || m.prevNetAt.IsZero() {
			continue
		}
		speedMbps, speedOK := readInterfaceSpeedMbps(iface)
		if !speedOK || speedMbps <= 0 {
			continue
		}
		seconds := now.Sub(m.prevNetAt).Seconds()
		if seconds <= 0 {
			continue
		}
		deltaBytes := currentTotal - prevTotal
		bps := float64(deltaBytes*8) / seconds
		usage := (bps / (float64(speedMbps) * 1000 * 1000)) * 100
		if usage > maxUsage {
			maxUsage = usage
		}
		valid = true
	}

	m.prevNetBytes = current
	m.prevNetAt = now
	if !valid {
		return 0, false
	}
	return clampFloat(maxUsage, 0, 100), true
}

func readInterfaceSpeedMbps(iface string) (int64, bool) {
	path := filepath.Join("/sys/class/net", iface, "speed")
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	value, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || value <= 0 {
		return 0, false
	}
	return value, true
}

func remainingRatio(limit, usage float64) float64 {
	if limit <= 0 {
		return 0
	}
	return clampFloat((limit-usage)/limit, 0, 1)
}

func clampFloat(value, minValue, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
