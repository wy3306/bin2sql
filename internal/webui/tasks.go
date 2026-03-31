package webui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"bin2sql/internal/webanalysis"
	"bin2sql/pkg/analyzer"
)

const timeLayout = "2006-01-02 15:04:05"

type AnalyzeRequest struct {
	Host                 string `json:"host"`
	Port                 int    `json:"port"`
	User                 string `json:"user"`
	Password             string `json:"password"`
	StartTime            string `json:"startTime"`
	EndTime              string `json:"endTime"`
	StartFile            string `json:"startFile"`
	BigTxnThreshold      int    `json:"bigTxnThreshold"`
	BigTxnMode           string `json:"bigTxnMode"`
	BigTxnBytesThreshold uint64 `json:"bigTxnBytesThreshold"`
}

type RequestView struct {
	Host                 string `json:"host"`
	Port                 int    `json:"port"`
	User                 string `json:"user"`
	StartTime            string `json:"startTime"`
	EndTime              string `json:"endTime"`
	StartFile            string `json:"startFile"`
	BigTxnThreshold      int    `json:"bigTxnThreshold"`
	BigTxnMode           string `json:"bigTxnMode"`
	BigTxnBytesThreshold uint64 `json:"bigTxnBytesThreshold"`
}

type TaskView struct {
	ID         string               `json:"id"`
	Status     string               `json:"status"`
	Error      string               `json:"error,omitempty"`
	CreatedAt  time.Time            `json:"createdAt"`
	StartedAt  time.Time            `json:"startedAt,omitempty"`
	FinishedAt time.Time            `json:"finishedAt,omitempty"`
	Request    RequestView          `json:"request"`
	Progress   webanalysis.Progress `json:"progress"`
	Summary    *webanalysis.Summary `json:"summary,omitempty"`
}

type taskRecord struct {
	task   TaskView
	result *webanalysis.Result
}

type TaskManager struct {
	mu    sync.RWMutex
	tasks map[string]*taskRecord
}

func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks: make(map[string]*taskRecord),
	}
}

func (m *TaskManager) Create(req AnalyzeRequest) (TaskView, error) {
	cfg, view, err := buildAnalyzeConfig(req)
	if err != nil {
		return TaskView{}, err
	}

	taskID := fmt.Sprintf("task-%d", time.Now().UnixNano())
	record := &taskRecord{
		task: TaskView{
			ID:        taskID,
			Status:    "queued",
			CreatedAt: time.Now(),
			Request:   view,
			Progress: webanalysis.Progress{
				Phase:   "queued",
				Message: "任务已创建，等待执行",
			},
		},
	}

	m.mu.Lock()
	m.tasks[taskID] = record
	m.mu.Unlock()

	go m.runTask(taskID, cfg)

	return record.task, nil
}

func (m *TaskManager) runTask(taskID string, cfg webanalysis.Config) {
	m.mu.Lock()
	record := m.tasks[taskID]
	record.task.Status = "running"
	record.task.StartedAt = time.Now()
	record.task.Progress = webanalysis.Progress{
		Phase:   "starting",
		Message: "任务启动中",
	}
	m.mu.Unlock()

	ctx := context.Background()
	result, err := webanalysis.Analyze(ctx, cfg, func(progress webanalysis.Progress) {
		m.mu.Lock()
		defer m.mu.Unlock()
		rec := m.tasks[taskID]
		if rec == nil {
			return
		}
		rec.task.Progress = progress
	})

	m.mu.Lock()
	defer m.mu.Unlock()

	record = m.tasks[taskID]
	if record == nil {
		return
	}

	record.task.FinishedAt = time.Now()
	if err != nil {
		record.task.Status = "failed"
		record.task.Error = err.Error()
		record.task.Progress.Phase = "failed"
		record.task.Progress.Message = err.Error()
		return
	}

	record.result = result
	record.task.Status = "completed"
	record.task.Summary = &result.Summary
	record.task.Progress.Phase = "completed"
	record.task.Progress.Message = "分析完成"
}

func (m *TaskManager) List() []TaskView {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tasks := make([]TaskView, 0, len(m.tasks))
	for _, record := range m.tasks {
		tasks = append(tasks, record.task)
	}
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].CreatedAt.After(tasks[j].CreatedAt)
	})
	return tasks
}

func (m *TaskManager) Get(id string) (TaskView, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	record, ok := m.tasks[id]
	if !ok {
		return TaskView{}, false
	}
	return record.task, true
}

func (m *TaskManager) Result(id string) (*webanalysis.Result, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	record, ok := m.tasks[id]
	if !ok {
		return nil, fmt.Errorf("任务不存在")
	}
	if record.task.Status != "completed" || record.result == nil {
		return nil, fmt.Errorf("任务尚未完成")
	}
	return record.result, nil
}

func buildAnalyzeConfig(req AnalyzeRequest) (webanalysis.Config, RequestView, error) {
	host := strings.TrimSpace(req.Host)
	if host == "" {
		host = "127.0.0.1"
	}
	port := req.Port
	if port == 0 {
		port = 3306
	}
	user := strings.TrimSpace(req.User)
	if user == "" {
		user = "root"
	}

	startTime, err := parseInputTime(req.StartTime)
	if err != nil {
		return webanalysis.Config{}, RequestView{}, fmt.Errorf("开始时间格式错误: %v", err)
	}
	endTime, err := parseInputTime(req.EndTime)
	if err != nil {
		return webanalysis.Config{}, RequestView{}, fmt.Errorf("结束时间格式错误: %v", err)
	}
	if endTime.Before(startTime) {
		return webanalysis.Config{}, RequestView{}, fmt.Errorf("结束时间不能早于开始时间")
	}

	cfg := webanalysis.Config{
		Host:                 host,
		Port:                 port,
		User:                 user,
		Password:             req.Password,
		StartTime:            startTime,
		EndTime:              endTime,
		StartFile:            strings.TrimSpace(req.StartFile),
		BigTxnThreshold:      req.BigTxnThreshold,
		BigTxnMode:           normalizeBigTxnMode(req.BigTxnMode),
		BigTxnBytesThreshold: req.BigTxnBytesThreshold,
	}

	view := RequestView{
		Host:                 host,
		Port:                 port,
		User:                 user,
		StartTime:            startTime.Format(timeLayout),
		EndTime:              endTime.Format(timeLayout),
		StartFile:            strings.TrimSpace(req.StartFile),
		BigTxnThreshold:      req.BigTxnThreshold,
		BigTxnMode:           normalizeBigTxnMode(req.BigTxnMode),
		BigTxnBytesThreshold: req.BigTxnBytesThreshold,
	}

	return cfg, view, nil
}

func normalizeBigTxnMode(mode string) string {
	if mode == analyzer.BigTxnModeBytes {
		return analyzer.BigTxnModeBytes
	}
	return analyzer.BigTxnModeRows
}

func parseInputTime(raw string) (time.Time, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return time.Time{}, fmt.Errorf("不能为空")
	}
	text = strings.ReplaceAll(text, "T", " ")
	if len(text) == len("2006-01-02 15:04") {
		text += ":00"
	}
	return time.ParseInLocation(timeLayout, text, time.Local)
}
