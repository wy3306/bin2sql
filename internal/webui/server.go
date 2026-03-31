package webui

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"bin2sql/internal/webanalysis"
)

//go:embed assets/*
var assets embed.FS

type Server struct {
	mux        *http.ServeMux
	tasks      *TaskManager
	onShutdown func()
}

func NewServer(onShutdown func()) (*Server, error) {
	staticFS, err := fs.Sub(assets, "assets")
	if err != nil {
		return nil, err
	}

	server := &Server{
		mux:        http.NewServeMux(),
		tasks:      NewTaskManager(),
		onShutdown: onShutdown,
	}

	server.mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))
	server.mux.HandleFunc("/", server.handleIndex)
	server.mux.HandleFunc("/api/cleanup", server.handleCleanup)
	server.mux.HandleFunc("/api/shutdown", server.handleShutdown)
	server.mux.HandleFunc("/api/tasks", server.handleTasks)
	server.mux.HandleFunc("/api/tasks/", server.handleTaskRoutes)

	return server, nil
}

func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message": "程序正在关闭",
	})
	if s.onShutdown != nil {
		go s.onShutdown()
	}
}

func (s *Server) handleCleanup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	patterns := []string{
		"big_transactions_*.txt",
		"dml_ddl_stats_*.txt",
		filepath.Join("bin", "big_transactions_*.txt"),
		filepath.Join("bin", "dml_ddl_stats_*.txt"),
		filepath.Join("logs", "*.log"),
		filepath.Join("bin", "binlog-web.log"),
	}

	seen := make(map[string]struct{})
	var deleted []string
	var freedBytes int64

	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("清理失败: %v", err))
			return
		}
		for _, match := range matches {
			if _, ok := seen[match]; ok {
				continue
			}
			seen[match] = struct{}{}

			info, err := os.Stat(match)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("读取文件失败: %v", err))
				return
			}
			if info.IsDir() {
				continue
			}
			if err := os.Remove(match); err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("删除文件失败: %v", err))
				return
			}
			deleted = append(deleted, match)
			freedBytes += info.Size()
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"deletedFiles": deleted,
		"deletedCount": len(deleted),
		"freedBytes":   freedBytes,
	})
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	data, err := assets.ReadFile("assets/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"tasks": s.tasks.List(),
		})
	case http.MethodPost:
		var req AnalyzeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("请求体解析失败: %v", err))
			return
		}
		task, err := s.tasks.Create(req)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, task)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTaskRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	path = strings.Trim(path, "/")
	if path == "" {
		http.NotFound(w, r)
		return
	}

	parts := strings.Split(path, "/")
	taskID := parts[0]

	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		task, ok := s.tasks.Get(taskID)
		if !ok {
			writeError(w, http.StatusNotFound, "任务不存在")
			return
		}
		writeJSON(w, http.StatusOK, task)
		return
	}

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	switch parts[1] {
	case "timeline":
		s.handleTimeline(w, r, taskID)
	case "dml":
		s.handleDML(w, r, taskID)
	case "tables":
		s.handleTables(w, r, taskID)
	case "result":
		s.handleFullResult(w, r, taskID)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleFullResult(w http.ResponseWriter, _ *http.Request, taskID string) {
	result, err := s.tasks.Result(taskID)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"summary":         result.Summary,
		"buckets":         result.Buckets,
		"bigTransactions": result.BigTransactions,
		"analyzedFiles":   result.AnalyzedFiles,
	})
}

func (s *Server) handleTimeline(w http.ResponseWriter, r *http.Request, taskID string) {
	result, err := s.tasks.Result(taskID)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	from, to, err := parseTimeRange(r, result.Summary.StartTime, result.Summary.EndTime)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	buckets := make([]webanalysis.TimeBucket, 0, len(result.Buckets))
	for _, bucket := range result.Buckets {
		if bucket.End.Before(from) || bucket.Start.After(to) {
			continue
		}
		buckets = append(buckets, bucket)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"summary": result.Summary,
		"from":    from,
		"to":      to,
		"buckets": buckets,
	})
}

func (s *Server) handleDML(w http.ResponseWriter, r *http.Request, taskID string) {
	result, err := s.tasks.Result(taskID)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	from, to, err := parseTimeRange(r, result.Summary.StartTime, result.Summary.EndTime)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	schema := strings.TrimSpace(r.URL.Query().Get("schema"))
	table := strings.TrimSpace(r.URL.Query().Get("table"))
	dmlType := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("type")))
	rowCount := parseIntDefault(r.URL.Query().Get("row_count"), -1)
	transactionBytes := parseUint64Default(r.URL.Query().Get("transaction_bytes"), 0)
	binlog := strings.TrimSpace(r.URL.Query().Get("binlog"))
	gtid := strings.TrimSpace(r.URL.Query().Get("gtid"))
	limit := clampInt(parseIntDefault(r.URL.Query().Get("limit"), 200), 1, 1000)
	offset := maxInt(parseIntDefault(r.URL.Query().Get("offset"), 0), 0)

	filtered := make([]webanalysis.DMLEvent, 0, limit)
	total := 0
	for _, event := range result.DMLEvents {
		if event.Time.Before(from) || event.Time.After(to) {
			continue
		}
		if schema != "" && event.Schema != schema {
			continue
		}
		if table != "" && event.Table != table {
			continue
		}
		if dmlType != "" && event.Type != dmlType {
			continue
		}
		if rowCount >= 0 && event.RowCount != rowCount {
			continue
		}
		if transactionBytes > 0 && event.TransactionLength < transactionBytes {
			continue
		}
		if binlog != "" && !strings.Contains(event.BinlogFile, binlog) {
			continue
		}
		if gtid != "" && !strings.Contains(event.GTID, gtid) {
			continue
		}
		if total >= offset && len(filtered) < limit {
			filtered = append(filtered, event)
		}
		total++
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items":  filtered,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (s *Server) handleTables(w http.ResponseWriter, r *http.Request, taskID string) {
	result, err := s.tasks.Result(taskID)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	from, to, err := parseTimeRange(r, result.Summary.StartTime, result.Summary.EndTime)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	stats := make(map[string]*webanalysis.TableSummary)
	for _, event := range result.DMLEvents {
		if event.Time.Before(from) || event.Time.After(to) {
			continue
		}
		key := event.Schema + "." + event.Table
		item := stats[key]
		if item == nil {
			item = &webanalysis.TableSummary{
				Schema: event.Schema,
				Table:  event.Table,
			}
			stats[key] = item
		}
		switch event.Type {
		case "INSERT":
			item.InsertRows += event.RowCount
		case "UPDATE":
			item.UpdateRows += event.RowCount
		case "DELETE":
			item.DeleteRows += event.RowCount
		}
		item.TotalRows += event.RowCount
	}

	items := make([]webanalysis.TableSummary, 0, len(stats))
	for _, item := range stats {
		items = append(items, *item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].TotalRows == items[j].TotalRows {
			if items[i].Schema == items[j].Schema {
				return items[i].Table < items[j].Table
			}
			return items[i].Schema < items[j].Schema
		}
		return items[i].TotalRows > items[j].TotalRows
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
	})
}

func parseTimeRange(r *http.Request, defaultFrom, defaultTo time.Time) (time.Time, time.Time, error) {
	query := r.URL.Query()
	from := defaultFrom
	to := defaultTo

	if raw := strings.TrimSpace(query.Get("from")); raw != "" {
		parsed, err := parseInputTime(raw)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("from 参数格式错误: %v", err)
		}
		from = parsed
	}
	if raw := strings.TrimSpace(query.Get("to")); raw != "" {
		parsed, err := parseInputTime(raw)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("to 参数格式错误: %v", err)
		}
		to = parsed
	}
	if to.Before(from) {
		return time.Time{}, time.Time{}, fmt.Errorf("to 不能早于 from")
	}
	return from, to, nil
}

func parseIntDefault(raw string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return value
}

func parseUint64Default(raw string, fallback uint64) uint64 {
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return fallback
	}
	return value
}

func clampInt(value, minValue, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{
		"error": message,
	})
}
