package webanalysis

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"bin2sql/pkg/analyzer"
	"bin2sql/pkg/seeker"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	defaultReadTimeout = 120 * time.Second
	defaultHeartbeat   = 60 * time.Second
	maxDMLEvents       = 100000
	maxBigTransactions = 5000
)

var (
	errTimeOver = fmt.Errorf("binlog time is over end time")
	logPosRegex = regexp.MustCompile(`LogPos:0x([0-9a-fA-F]+)`)
)

type Config struct {
	Host                 string
	Port                 int
	User                 string
	Password             string
	StartFile            string
	StartTime            time.Time
	EndTime              time.Time
	BigTxnThreshold      int
	BigTxnMode           string
	BigTxnBytesThreshold uint64
}

type Progress struct {
	Phase           string    `json:"phase"`
	Message         string    `json:"message"`
	CurrentFile     string    `json:"currentFile"`
	FilesTotal      int       `json:"filesTotal"`
	FilesCompleted  int       `json:"filesCompleted"`
	EventsProcessed int64     `json:"eventsProcessed"`
	LastEventTime   time.Time `json:"lastEventTime,omitempty"`
}

type Summary struct {
	StartFile            string    `json:"startFile"`
	StartTime            time.Time `json:"startTime"`
	EndTime              time.Time `json:"endTime"`
	FilesAnalyzed        int       `json:"filesAnalyzed"`
	InsertRows           int       `json:"insertRows"`
	UpdateRows           int       `json:"updateRows"`
	DeleteRows           int       `json:"deleteRows"`
	TotalRows            int       `json:"totalRows"`
	EventCount           int       `json:"eventCount"`
	BigTxnCount          int       `json:"bigTxnCount"`
	BigTxnMode           string    `json:"bigTxnMode"`
	BigTxnThreshold      int       `json:"bigTxnThreshold"`
	BigTxnBytesThreshold uint64    `json:"bigTxnBytesThreshold"`
	BucketSeconds        int       `json:"bucketSeconds"`
	DMLTruncated         bool      `json:"dmlTruncated"`
	DMLStoredEvents      int       `json:"dmlStoredEvents"`
}

type TimeBucket struct {
	Start          time.Time `json:"start"`
	End            time.Time `json:"end"`
	InsertRows     int       `json:"insertRows"`
	UpdateRows     int       `json:"updateRows"`
	DeleteRows     int       `json:"deleteRows"`
	TotalRows      int       `json:"totalRows"`
	EventCount     int       `json:"eventCount"`
	BigTxnCount    int       `json:"bigTxnCount"`
	SumBigTxnBytes uint64    `json:"sumBigTxnBytes"`
	MaxBigTxnRows  int       `json:"maxBigTxnRows"`
	MaxBigTxnBytes uint64    `json:"maxBigTxnBytes"`
}

type DMLEvent struct {
	Time              time.Time `json:"time"`
	Schema            string    `json:"schema"`
	Table             string    `json:"table"`
	Type              string    `json:"type"`
	RowCount          int       `json:"rowCount"`
	TransactionLength uint64    `json:"transactionLength"`
	BinlogFile        string    `json:"binlogFile"`
	GTID              string    `json:"gtid,omitempty"`
}

type TableSummary struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	InsertRows int    `json:"insertRows"`
	UpdateRows int    `json:"updateRows"`
	DeleteRows int    `json:"deleteRows"`
	TotalRows  int    `json:"totalRows"`
}

type BigTransaction struct {
	StartTime         time.Time `json:"startTime"`
	EndTime           time.Time `json:"endTime"`
	RowCount          int       `json:"rowCount"`
	TransactionLength uint64    `json:"transactionLength"`
	BinlogFile        string    `json:"binlogFile"`
	GTID              string    `json:"gtid,omitempty"`
	Tables            []string  `json:"tables"`
}

type Result struct {
	Summary         Summary          `json:"summary"`
	Buckets         []TimeBucket     `json:"buckets"`
	DMLEvents       []DMLEvent       `json:"dmlEvents"`
	Tables          []TableSummary   `json:"tables"`
	BigTransactions []BigTransaction `json:"bigTransactions"`
	AnalyzedFiles   []string         `json:"analyzedFiles"`
}

type analyzerState struct {
	cfg             Config
	result          *Result
	bucketSize      time.Duration
	filesCompleted  int
	analyzedFiles   []string
	analyzedFileSet map[string]struct{}
	tableStats      map[string]*TableSummary
	progress        Progress
	emitProgress    func(Progress)
	lastEmitAt      time.Time
	currentGTID     string
	inTxn           bool
	txnStart        time.Time
	txnRows         int
	txnBytes        uint64
	txnTables       map[string]struct{}
}

func Analyze(ctx context.Context, cfg Config, emitProgress func(Progress)) (*Result, error) {
	if cfg.Port == 0 {
		cfg.Port = 3306
	}
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.StartTime.IsZero() || cfg.EndTime.IsZero() {
		return nil, fmt.Errorf("开始时间和结束时间不能为空")
	}
	if cfg.EndTime.Before(cfg.StartTime) {
		return nil, fmt.Errorf("结束时间不能早于开始时间")
	}

	startFile, files, followRotates, err := resolveBinlogFiles(cfg)
	if err != nil {
		return nil, err
	}
	cfg.StartFile = startFile

	bucketSize := chooseBucketSize(cfg.EndTime.Sub(cfg.StartTime))
	result := &Result{
		Summary: Summary{
			StartFile:            startFile,
			StartTime:            cfg.StartTime,
			EndTime:              cfg.EndTime,
			BigTxnMode:           normalizeBigTxnMode(cfg.BigTxnMode),
			BigTxnThreshold:      cfg.BigTxnThreshold,
			BigTxnBytesThreshold: cfg.BigTxnBytesThreshold,
			BucketSeconds:        int(bucketSize.Seconds()),
		},
		Buckets: makeBuckets(cfg.StartTime, cfg.EndTime, bucketSize),
	}

	state := &analyzerState{
		cfg:             cfg,
		result:          result,
		bucketSize:      bucketSize,
		analyzedFileSet: make(map[string]struct{}),
		tableStats:      make(map[string]*TableSummary),
		emitProgress:    emitProgress,
		txnTables:       make(map[string]struct{}),
		progress: Progress{
			Phase:      "preparing",
			Message:    "准备分析任务",
			FilesTotal: len(files),
		},
	}
	state.emit(true)

	if followRotates {
		if err := state.processStream(ctx, startFile, true); err != nil {
			if err == errTimeOver {
				state.finalize()
				return result, nil
			}
			return nil, err
		}
	} else if len(files) > 1 {
		if err := state.processFilesParallel(ctx, files); err != nil {
			if err == errTimeOver {
				state.finalize()
				return result, nil
			}
			return nil, err
		}
	} else {
		for _, file := range files {
			if err := state.processStream(ctx, file, false); err != nil {
				if err == errTimeOver {
					state.finalize()
					return result, nil
				}
				return nil, err
			}
			state.filesCompleted++
			state.progress.FilesCompleted = state.filesCompleted
			state.progress.Message = fmt.Sprintf("已完成 %d/%d 个 Binlog 文件", state.filesCompleted, state.progress.FilesTotal)
			state.emit(true)
		}
	}

	state.finalize()
	return result, nil
}

func resolveBinlogFiles(cfg Config) (string, []string, bool, error) {
	seekerCfg := seeker.Config{
		Host:     cfg.Host,
		Port:     cfg.Port,
		User:     cfg.User,
		Password: cfg.Password,
	}

	startFile := cfg.StartFile
	if startFile == "" {
		var err error
		startFile, err = seeker.FindStartBinlog(seekerCfg, cfg.StartTime)
		if err != nil {
			return "", nil, false, fmt.Errorf("查找起始 binlog 失败: %v", err)
		}
	}

	files, err := seeker.GetBinlogsAfter(seekerCfg, startFile)
	if err != nil {
		return startFile, []string{startFile}, true, nil
	}
	return startFile, files, false, nil
}

type parallelFileResult struct {
	filename string
	state    *analyzerState
	err      error
}

func (s *analyzerState) processFilesParallel(ctx context.Context, files []string) error {
	maxConcurrency := calculateMaxParallelWorkers(len(files))
	monitor := newResourceMonitor()
	defer monitor.stop()

	resultCh := make(chan parallelFileResult, maxConcurrency)
	var stopScheduling bool
	nextIndex := 0
	active := 0

	updateProgress := func(message string, snap resourceSnapshot) {
		s.progress.Phase = "running"
		s.progress.FilesCompleted = s.filesCompleted
		s.progress.FilesTotal = len(files)
		s.progress.Message = message
		if snap.HasCPU || snap.HasMemory || snap.HasBandwidth {
			s.progress.Message = fmt.Sprintf("%s | %s", message, formatResourceSnapshot(snap))
		}
		s.emit(true)
	}

	for nextIndex < len(files) || active > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		targetConcurrency, snap := monitor.recommendedConcurrency(maxConcurrency)
		if !stopScheduling {
			for active < targetConcurrency && nextIndex < len(files) {
				filename := files[nextIndex]
				nextIndex++
				active++

				localState := newPartialAnalyzerState(s.cfg, s.bucketSize)
				updateProgress(fmt.Sprintf("并行分析中，活跃任务 %d/%d，启动 %s", active, targetConcurrency, filename), snap)

				go func(file string, local *analyzerState) {
					err := local.processStream(ctx, file, false)
					local.finalize()
					resultCh <- parallelFileResult{
						filename: file,
						state:    local,
						err:      err,
					}
				}(filename, localState)
			}
		}

		if active == 0 {
			continue
		}

		select {
		case fileResult := <-resultCh:
			active--
			if fileResult.err == errTimeOver {
				stopScheduling = true
			} else if fileResult.err != nil {
				return fileResult.err
			}

			s.mergePartialState(fileResult.state)
			s.filesCompleted++
			s.progress.EventsProcessed += fileResult.state.progress.EventsProcessed

			targetConcurrency, snap = monitor.recommendedConcurrency(maxConcurrency)
			updateProgress(fmt.Sprintf("并行分析中，已完成 %d/%d，活跃任务 %d/%d，最近完成 %s", s.filesCompleted, len(files), active, targetConcurrency, fileResult.filename), snap)
		case <-time.After(1200 * time.Millisecond):
			updateProgress(fmt.Sprintf("并行分析中，已完成 %d/%d，活跃任务 %d/%d", s.filesCompleted, len(files), active, targetConcurrency), snap)
		}
	}

	return nil
}

func newPartialAnalyzerState(cfg Config, bucketSize time.Duration) *analyzerState {
	result := &Result{
		Summary: Summary{
			StartFile:            cfg.StartFile,
			StartTime:            cfg.StartTime,
			EndTime:              cfg.EndTime,
			BigTxnMode:           normalizeBigTxnMode(cfg.BigTxnMode),
			BigTxnThreshold:      cfg.BigTxnThreshold,
			BigTxnBytesThreshold: cfg.BigTxnBytesThreshold,
			BucketSeconds:        int(bucketSize.Seconds()),
		},
		Buckets: makeBuckets(cfg.StartTime, cfg.EndTime, bucketSize),
	}
	return &analyzerState{
		cfg:             cfg,
		result:          result,
		bucketSize:      bucketSize,
		analyzedFileSet: make(map[string]struct{}),
		tableStats:      make(map[string]*TableSummary),
		txnTables:       make(map[string]struct{}),
	}
}

func (s *analyzerState) mergePartialState(partial *analyzerState) {
	if partial == nil || partial.result == nil {
		return
	}

	for _, file := range partial.analyzedFiles {
		s.addAnalyzedFile(file)
	}

	s.result.Summary.InsertRows += partial.result.Summary.InsertRows
	s.result.Summary.UpdateRows += partial.result.Summary.UpdateRows
	s.result.Summary.DeleteRows += partial.result.Summary.DeleteRows
	s.result.Summary.TotalRows += partial.result.Summary.TotalRows
	s.result.Summary.EventCount += partial.result.Summary.EventCount
	s.result.Summary.DMLTruncated = s.result.Summary.DMLTruncated || partial.result.Summary.DMLTruncated

	for i := range s.result.Buckets {
		if i >= len(partial.result.Buckets) {
			break
		}
		dst := &s.result.Buckets[i]
		src := partial.result.Buckets[i]
		dst.InsertRows += src.InsertRows
		dst.UpdateRows += src.UpdateRows
		dst.DeleteRows += src.DeleteRows
		dst.TotalRows += src.TotalRows
		dst.EventCount += src.EventCount
		dst.BigTxnCount += src.BigTxnCount
		dst.SumBigTxnBytes += src.SumBigTxnBytes
		if dst.MaxBigTxnRows < src.MaxBigTxnRows {
			dst.MaxBigTxnRows = src.MaxBigTxnRows
		}
		if dst.MaxBigTxnBytes < src.MaxBigTxnBytes {
			dst.MaxBigTxnBytes = src.MaxBigTxnBytes
		}
	}

	if len(s.result.DMLEvents) < maxDMLEvents {
		remaining := maxDMLEvents - len(s.result.DMLEvents)
		if len(partial.result.DMLEvents) > remaining {
			s.result.DMLEvents = append(s.result.DMLEvents, partial.result.DMLEvents[:remaining]...)
			s.result.Summary.DMLTruncated = true
		} else {
			s.result.DMLEvents = append(s.result.DMLEvents, partial.result.DMLEvents...)
		}
	} else if len(partial.result.DMLEvents) > 0 {
		s.result.Summary.DMLTruncated = true
	}

	if len(s.result.BigTransactions) < maxBigTransactions {
		remaining := maxBigTransactions - len(s.result.BigTransactions)
		if len(partial.result.BigTransactions) > remaining {
			s.result.BigTransactions = append(s.result.BigTransactions, partial.result.BigTransactions[:remaining]...)
		} else {
			s.result.BigTransactions = append(s.result.BigTransactions, partial.result.BigTransactions...)
		}
	}

	for key, src := range partial.tableStats {
		dst := s.tableStats[key]
		if dst == nil {
			dst = &TableSummary{Schema: src.Schema, Table: src.Table}
			s.tableStats[key] = dst
		}
		dst.InsertRows += src.InsertRows
		dst.UpdateRows += src.UpdateRows
		dst.DeleteRows += src.DeleteRows
		dst.TotalRows += src.TotalRows
	}
}

func formatResourceSnapshot(snap resourceSnapshot) string {
	parts := make([]string, 0, 3)
	if snap.HasCPU {
		parts = append(parts, fmt.Sprintf("CPU %.1f%%", snap.CPUUsagePct))
	}
	if snap.HasMemory {
		parts = append(parts, fmt.Sprintf("MEM %.1f%%", snap.MemoryUsagePct))
	}
	if snap.HasBandwidth {
		parts = append(parts, fmt.Sprintf("NET %.1f%%", snap.BandwidthUsagePct))
	}
	if len(parts) == 0 {
		return "资源采样不可用"
	}
	return strings.Join(parts, " | ")
}

func (s *analyzerState) processStream(ctx context.Context, filename string, followRotates bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			err = fmt.Errorf("分析 %s 时发生 panic: %v\n%s", filename, r, string(buf))
		}
	}()

	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(rand.Intn(1000000) + 1000 + int(time.Now().UnixNano()%1000)),
		Flavor:          "mysql",
		Host:            s.cfg.Host,
		Port:            uint16(s.cfg.Port),
		User:            s.cfg.User,
		Password:        s.cfg.Password,
		ReadTimeout:     defaultReadTimeout,
		HeartbeatPeriod: defaultHeartbeat,
		ParseTime:       false,
		UseDecimal:      false,
	}

	pos := uint32(4)
	firstEventChecked := false
	currentFile := filename
	tableMap := make(map[uint64][2]string)
	s.addAnalyzedFile(currentFile)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		s.progress.Phase = "running"
		s.progress.CurrentFile = currentFile
		s.progress.Message = fmt.Sprintf("正在分析 %s", currentFile)
		s.emit(true)

		syncer := replication.NewBinlogSyncer(syncerCfg)
		streamer, err := syncer.StartSync(mysql.Position{Name: currentFile, Pos: pos})
		if err != nil {
			syncer.Close()
			return fmt.Errorf("启动 binlog 同步失败: %v", err)
		}

	readLoop:
		for {
			if err := ctx.Err(); err != nil {
				syncer.Close()
				return err
			}

			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				errStr := err.Error()
				if strings.Contains(errStr, "parse rows event panic") || strings.Contains(errStr, "Expect odd item") {
					matches := logPosRegex.FindStringSubmatch(errStr)
					if len(matches) > 1 {
						nextPos, parseErr := strconv.ParseUint(matches[1], 16, 32)
						if parseErr == nil && uint32(nextPos) > pos {
							pos = uint32(nextPos)
							syncer.Close()
							break readLoop
						}
					}
				}
				syncer.Close()
				return err
			}

			if ev.Header.LogPos > 0 {
				pos = ev.Header.LogPos
			}

			if ev.Header.Timestamp > 0 {
				ts := time.Unix(int64(ev.Header.Timestamp), 0)
				s.progress.LastEventTime = ts
				if !firstEventChecked {
					firstEventChecked = true
					if ts.After(s.cfg.EndTime) {
						syncer.Close()
						return errTimeOver
					}
				}
				if ts.After(s.cfg.EndTime) {
					s.finishOpenTransaction(ts, currentFile)
					syncer.Close()
					return nil
				}
			}

			s.progress.EventsProcessed++
			s.emit(false)

			switch e := ev.Event.(type) {
			case *replication.GTIDEvent:
				s.finishOpenTransaction(time.Unix(int64(ev.Header.Timestamp), 0), currentFile)
				s.startTransaction(time.Time{})
				u := e.SID
				s.currentGTID = fmt.Sprintf("%x-%x-%x-%x-%x:%d", u[0:4], u[4:6], u[6:8], u[8:10], u[10:], e.GNO)
				s.txnBytes = e.TransactionLength

			case *replication.MariadbGTIDEvent:
				s.finishOpenTransaction(time.Unix(int64(ev.Header.Timestamp), 0), currentFile)
				s.startTransaction(time.Time{})
				s.currentGTID = e.GTID.String()

			case *replication.RotateEvent:
				nextLog := string(e.NextLogName)
				if followRotates && nextLog != "" && nextLog != currentFile {
					currentFile = nextLog
					s.addAnalyzedFile(currentFile)
					s.progress.CurrentFile = currentFile
					s.progress.Message = fmt.Sprintf("切换到 %s", currentFile)
					s.emit(true)
					continue
				}
				if !followRotates && nextLog != "" && nextLog != filename {
					s.finishOpenTransaction(time.Unix(int64(ev.Header.Timestamp), 0), currentFile)
					syncer.Close()
					return nil
				}

			case *replication.TableMapEvent:
				tableMap[e.TableID] = [2]string{string(e.Schema), string(e.Table)}

			case *replication.RowsEvent:
				ts := time.Unix(int64(ev.Header.Timestamp), 0)
				if ts.Before(s.cfg.StartTime) {
					continue
				}
				info, ok := tableMap[e.TableID]
				if !ok {
					continue
				}

				eventType, ok := classifyDML(ev.Header.EventType)
				if !ok {
					continue
				}

				rowCount := len(e.Rows)
				if rowCount == 0 {
					continue
				}

				if !s.inTxn {
					s.startTransaction(ts)
				} else if s.txnStart.IsZero() {
					s.txnStart = ts
				}
				s.txnRows += rowCount
				s.txnTables[info[0]+"."+info[1]] = struct{}{}

				s.recordDMLEvent(ts, currentFile, info[0], info[1], eventType, rowCount, s.txnBytes)

			case *replication.QueryEvent:
				ts := time.Unix(int64(ev.Header.Timestamp), 0)
				query := strings.ToUpper(strings.TrimSpace(string(e.Query)))
				if query == "BEGIN" {
					s.startTransaction(ts)
					continue
				}
				if query == "COMMIT" || query == "ROLLBACK" {
					s.finishOpenTransaction(ts, currentFile)
				}

			case *replication.XIDEvent:
				s.finishOpenTransaction(time.Unix(int64(ev.Header.Timestamp), 0), currentFile)
			}
		}
	}
}

func (s *analyzerState) startTransaction(start time.Time) {
	s.inTxn = true
	s.txnStart = start
	s.txnRows = 0
	s.txnBytes = 0
	s.txnTables = make(map[string]struct{})
}

func (s *analyzerState) finishOpenTransaction(end time.Time, binlogFile string) {
	if !s.inTxn {
		return
	}
	if isBigTransaction(s.cfg, s.txnRows, s.txnBytes) {
		if len(s.result.BigTransactions) < maxBigTransactions {
			tables := make([]string, 0, len(s.txnTables))
			for table := range s.txnTables {
				tables = append(tables, table)
			}
			sort.Strings(tables)
			start := s.txnStart
			if start.IsZero() {
				start = end
			}
			s.result.BigTransactions = append(s.result.BigTransactions, BigTransaction{
				StartTime:         start,
				EndTime:           end,
				RowCount:          s.txnRows,
				TransactionLength: s.txnBytes,
				BinlogFile:        binlogFile,
				GTID:              s.currentGTID,
				Tables:            tables,
			})
		}
		if idx := bucketIndex(s.cfg.StartTime, end, s.bucketSize, len(s.result.Buckets)); idx >= 0 {
			s.result.Buckets[idx].BigTxnCount++
			s.result.Buckets[idx].SumBigTxnBytes += s.txnBytes
			if s.result.Buckets[idx].MaxBigTxnRows < s.txnRows {
				s.result.Buckets[idx].MaxBigTxnRows = s.txnRows
			}
			if s.result.Buckets[idx].MaxBigTxnBytes < s.txnBytes {
				s.result.Buckets[idx].MaxBigTxnBytes = s.txnBytes
			}
		}
	}
	s.inTxn = false
	s.txnStart = time.Time{}
	s.txnRows = 0
	s.txnBytes = 0
	s.txnTables = make(map[string]struct{})
	s.currentGTID = ""
}

func (s *analyzerState) recordDMLEvent(ts time.Time, binlogFile, schema, table, eventType string, rowCount int, txnBytes uint64) {
	idx := bucketIndex(s.cfg.StartTime, ts, s.bucketSize, len(s.result.Buckets))
	if idx >= 0 {
		bucket := &s.result.Buckets[idx]
		bucket.EventCount++
		bucket.TotalRows += rowCount
		switch eventType {
		case "INSERT":
			bucket.InsertRows += rowCount
		case "UPDATE":
			bucket.UpdateRows += rowCount
		case "DELETE":
			bucket.DeleteRows += rowCount
		}
	}

	s.result.Summary.EventCount++
	s.result.Summary.TotalRows += rowCount
	switch eventType {
	case "INSERT":
		s.result.Summary.InsertRows += rowCount
	case "UPDATE":
		s.result.Summary.UpdateRows += rowCount
	case "DELETE":
		s.result.Summary.DeleteRows += rowCount
	}

	key := schema + "." + table
	stats := s.tableStats[key]
	if stats == nil {
		stats = &TableSummary{Schema: schema, Table: table}
		s.tableStats[key] = stats
	}
	switch eventType {
	case "INSERT":
		stats.InsertRows += rowCount
	case "UPDATE":
		stats.UpdateRows += rowCount
	case "DELETE":
		stats.DeleteRows += rowCount
	}
	stats.TotalRows += rowCount

	if len(s.result.DMLEvents) < maxDMLEvents {
		s.result.DMLEvents = append(s.result.DMLEvents, DMLEvent{
			Time:              ts,
			Schema:            schema,
			Table:             table,
			Type:              eventType,
			RowCount:          rowCount,
			TransactionLength: txnBytes,
			BinlogFile:        binlogFile,
			GTID:              s.currentGTID,
		})
	} else {
		s.result.Summary.DMLTruncated = true
	}
}

func (s *analyzerState) addAnalyzedFile(filename string) {
	if filename == "" {
		return
	}
	if _, exists := s.analyzedFileSet[filename]; exists {
		return
	}
	s.analyzedFileSet[filename] = struct{}{}
	s.analyzedFiles = append(s.analyzedFiles, filename)
}

func (s *analyzerState) finalize() {
	sort.Strings(s.analyzedFiles)
	s.result.AnalyzedFiles = append([]string(nil), s.analyzedFiles...)
	s.result.Summary.FilesAnalyzed = len(s.analyzedFiles)
	s.result.Summary.BigTxnCount = len(s.result.BigTransactions)
	s.result.Summary.DMLStoredEvents = len(s.result.DMLEvents)

	tableList := make([]TableSummary, 0, len(s.tableStats))
	for _, stats := range s.tableStats {
		tableList = append(tableList, *stats)
	}
	sort.Slice(tableList, func(i, j int) bool {
		if tableList[i].TotalRows == tableList[j].TotalRows {
			if tableList[i].Schema == tableList[j].Schema {
				return tableList[i].Table < tableList[j].Table
			}
			return tableList[i].Schema < tableList[j].Schema
		}
		return tableList[i].TotalRows > tableList[j].TotalRows
	})
	s.result.Tables = tableList

	sort.Slice(s.result.DMLEvents, func(i, j int) bool {
		return s.result.DMLEvents[i].Time.Before(s.result.DMLEvents[j].Time)
	})
	sort.Slice(s.result.BigTransactions, func(i, j int) bool {
		return s.result.BigTransactions[i].StartTime.Before(s.result.BigTransactions[j].StartTime)
	})

	s.progress.Phase = "completed"
	s.progress.FilesCompleted = s.filesCompleted
	if s.progress.FilesCompleted == 0 {
		s.progress.FilesCompleted = len(s.analyzedFiles)
	}
	s.progress.Message = "分析完成"
	s.emit(true)
}

func (s *analyzerState) emit(force bool) {
	if s.emitProgress == nil {
		return
	}
	now := time.Now()
	if !force && now.Sub(s.lastEmitAt) < time.Second {
		return
	}
	s.lastEmitAt = now
	progressCopy := s.progress
	s.emitProgress(progressCopy)
}

func chooseBucketSize(span time.Duration) time.Duration {
	switch {
	case span <= 2*time.Hour:
		return time.Minute
	case span <= 24*time.Hour:
		return 5 * time.Minute
	case span <= 72*time.Hour:
		return 15 * time.Minute
	case span <= 7*24*time.Hour:
		return time.Hour
	default:
		return 2 * time.Hour
	}
}

func makeBuckets(start, end time.Time, size time.Duration) []TimeBucket {
	if !end.After(start) {
		return []TimeBucket{{Start: start, End: end}}
	}
	count := int(end.Sub(start) / size)
	if end.Sub(start)%size != 0 {
		count++
	}
	if count <= 0 {
		count = 1
	}

	buckets := make([]TimeBucket, 0, count)
	cursor := start
	for cursor.Before(end) {
		next := cursor.Add(size)
		if next.After(end) {
			next = end
		}
		buckets = append(buckets, TimeBucket{Start: cursor, End: next})
		cursor = next
	}
	if len(buckets) == 0 {
		buckets = append(buckets, TimeBucket{Start: start, End: end})
	}
	return buckets
}

func bucketIndex(start, target time.Time, size time.Duration, total int) int {
	if target.Before(start) || total == 0 {
		return -1
	}
	idx := int(target.Sub(start) / size)
	if idx >= total {
		return total - 1
	}
	if idx < 0 {
		return -1
	}
	return idx
}

func normalizeBigTxnMode(mode string) string {
	if mode == analyzer.BigTxnModeBytes {
		return analyzer.BigTxnModeBytes
	}
	return analyzer.BigTxnModeRows
}

func isBigTransaction(cfg Config, rows int, txnBytes uint64) bool {
	if normalizeBigTxnMode(cfg.BigTxnMode) == analyzer.BigTxnModeBytes {
		return cfg.BigTxnBytesThreshold > 0 && txnBytes >= cfg.BigTxnBytesThreshold
	}
	return cfg.BigTxnThreshold > 0 && rows >= cfg.BigTxnThreshold
}

func classifyDML(eventType replication.EventType) (string, bool) {
	switch eventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return "INSERT", true
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return "UPDATE", true
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return "DELETE", true
	default:
		return "", false
	}
}
