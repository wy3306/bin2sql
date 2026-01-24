package analyzer

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// 定义特定错误，用于通知调度器停止分发
var ErrTimeOver = fmt.Errorf("binlog time is over end time")

// Config 定义分析器的配置参数
type Config struct {
	Host            string    // MySQL 主机地址
	Port            int       // MySQL 端口
	User            string    // MySQL 用户名
	Password        string    // MySQL 密码
	StartFile       string    // 起始 Binlog 文件名
	BinlogFiles     []string  // 需要分析的 Binlog 文件列表 (用于多线程)
	StartTime       time.Time // 分析的起始时间（包含）
	EndTime         time.Time // 分析的结束时间（包含）
	BigTxnThreshold int       // 大事务行数阈值，0 表示不开启
}

// TableStats 用于存储单个表的统计信息
type TableStats struct {
	Insert int // 插入操作计数
	Update int // 更新操作计数
	Delete int // 删除操作计数
	DDL    int // DDL 操作（如 ALTER, CREATE 等）计数
}

// BigTxnInfo 存储大事务信息
type BigTxnInfo struct {
	Filename  string    // Binlog 文件名
	StartTime time.Time // 事务开始时间
	EndTime   time.Time // 事务结束时间
	Gtid      string    // GTID (如果有)
	Rows      int       // 影响行数
	Tables    []string  // 涉及的表
}

// Analyzer 是 Binlog 分析器的核心结构体
type Analyzer struct {
	cfg           Config                            // 配置信息
	stats         map[string]map[string]*TableStats // 统计数据存储结构：库名 -> 表名 -> 统计对象
	mu            sync.Mutex                        // 保护 stats 的互斥锁
	totalEvents   int64                             // 全局事件计数器 (原子操作)
	analyzedFiles []string                          // 已实际分析的文件列表
	fileMu        sync.Mutex                        // 保护 analyzedFiles 的互斥锁
	bigTxns       []BigTxnInfo                      // 发现的大事务列表
	bigTxnMu      sync.Mutex                        // 保护 bigTxns 的互斥锁
}

// New 创建一个新的分析器实例
// cfg: 分析器的配置参数
func New(cfg Config) *Analyzer {
	return &Analyzer{
		cfg:   cfg,
		stats: make(map[string]map[string]*TableStats),
	}
}

// Run 启动 Binlog 分析流程
// 如果配置了 BinlogFiles 且数量大于 1，将自动启用多文件并行分析
func (a *Analyzer) Run() error {
	// 如果文件列表为空，但指定了 StartFile，则将其加入列表（单文件模式）
	if len(a.cfg.BinlogFiles) == 0 && a.cfg.StartFile != "" {
		a.cfg.BinlogFiles = []string{a.cfg.StartFile}
	}

	if len(a.cfg.BinlogFiles) == 0 {
		return fmt.Errorf("未指定要分析的 Binlog 文件")
	}

	// 启动全局进度打印协程
	done := make(chan struct{})
	go a.printProgress(done)
	defer close(done)

	// 如果只有一个文件，使用单线程模式（减少开销）
	if len(a.cfg.BinlogFiles) == 1 {
		a.addAnalyzedFile(a.cfg.BinlogFiles[0]) // 单文件直接记录
		return a.processBinlogFile(a.cfg.BinlogFiles[0], uint32(rand.Intn(1000000)+1000))
	}

	// 多文件并行模式
	first := a.cfg.BinlogFiles[0]
	last := a.cfg.BinlogFiles[len(a.cfg.BinlogFiles)-1]
	fmt.Printf("检测到 %d 个 Binlog 文件 (%s ... %s)，启用多线程并行分析...\n",
		len(a.cfg.BinlogFiles), first, last)
	return a.runParallel()
}

// addAnalyzedFile 线程安全地添加已分析的文件
func (a *Analyzer) addAnalyzedFile(filename string) {
	a.fileMu.Lock()
	defer a.fileMu.Unlock()
	a.analyzedFiles = append(a.analyzedFiles, filename)
}

// printProgress 打印全局进度
func (a *Analyzer) printProgress(done chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			count := atomic.LoadInt64(&a.totalEvents)
			fmt.Printf("全局进度: 已处理 %d 个事件...\n", count)
		case <-done:
			return
		}
	}
}

// runParallel 并行分析多个 Binlog 文件
func (a *Analyzer) runParallel() error {
	// 限制并发数为 CPU 核心数或固定值 (如 8)
	concurrency := 8
	if len(a.cfg.BinlogFiles) < concurrency {
		concurrency = len(a.cfg.BinlogFiles)
	}

	// 创建 worker 池
	// sem 用于控制并发数
	sem := make(chan struct{}, concurrency)
	// errChan 用于收集错误
	errChan := make(chan error, len(a.cfg.BinlogFiles))
	// wg 用于等待所有任务完成
	var wg sync.WaitGroup

	// 使用原子标志位来控制是否停止调度
	var stopScheduling int32 = 0

	// 遍历所有文件分配任务
	for _, filename := range a.cfg.BinlogFiles {
		// 检查是否收到停止信号
		if atomic.LoadInt32(&stopScheduling) == 1 {
			// fmt.Printf("跳过文件 %s (已超过结束时间)\n", filename)
			continue
		}

		wg.Add(1)
		sem <- struct{}{} // 获取令牌

		// 双重检查
		if atomic.LoadInt32(&stopScheduling) == 1 {
			wg.Done()
			<-sem
			continue
		}

		// 每个 Worker 需要独立的 ServerID
		serverID := uint32(rand.Intn(1000000) + 1000 + int(time.Now().UnixNano()%1000))

		go func(fname string, sid uint32) {
			defer wg.Done()
			defer func() { <-sem }() // 释放令牌

			// 执行分析
			// 注意：processBinlogFile 内部会更新 a.stats，需要确保线程安全
			if err := a.processBinlogFile(fname, sid); err != nil {
				if err == ErrTimeOver {
					// 如果当前文件已经超过结束时间，设置停止标志
					// 意味着后续的文件也不需要分析了（因为文件名是有序的）
					atomic.StoreInt32(&stopScheduling, 1)
					// ErrTimeOver 不是真正的错误，而是正常的提前结束信号
				} else {
					errChan <- fmt.Errorf("文件 %s 分析失败: %v", fname, err)
				}
			}
		}(filename, serverID)
	}

	// 等待所有 Worker 完成
	wg.Wait()
	close(errChan)

	// 检查是否有错误
	// 这里只返回第一个错误，实际生产中可能需要聚合错误
	if len(errChan) > 0 {
		return <-errChan
	}

	return nil
}

// addBigTxn 线程安全地记录大事务
func (a *Analyzer) addBigTxn(info BigTxnInfo) {
	a.bigTxnMu.Lock()
	defer a.bigTxnMu.Unlock()
	a.bigTxns = append(a.bigTxns, info)
}

// processBinlogFile 分析单个 Binlog 文件
// 在多线程模式下，此函数会被并发调用
func (a *Analyzer) processBinlogFile(filename string, serverID uint32) error {
	// 1. 配置 Binlog Syncer
	syncerCfg := replication.BinlogSyncerConfig{
		ServerID: serverID,
		Flavor:   "mysql",
		Host:     a.cfg.Host,
		Port:     uint16(a.cfg.Port),
		User:     a.cfg.User,
		Password: a.cfg.Password,
	}
	syncer := replication.NewBinlogSyncer(syncerCfg)
	defer syncer.Close()

	// 2. 开始同步
	// 始终从文件头部 (Pos: 4) 开始
	// fmt.Printf("[%s] 开始分析...\n", filename)
	streamer, err := syncer.StartSync(mysql.Position{Name: filename, Pos: 4})
	if err != nil {
		return err
	}

	// 3. 启动事件获取协程（Producer）
	eventChan := make(chan *replication.BinlogEvent, 10000)
	errChan := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		defer close(eventChan)
		defer close(errChan)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				errChan <- err
				return
			}

			select {
			case eventChan <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()

	// 局部统计数据，避免频繁锁竞争
	localStats := make(map[string]map[string]*TableStats)
	// 辅助函数：更新局部统计
	updateLocalStats := func(schema, table string, statsType string) {
		if localStats[schema] == nil {
			localStats[schema] = make(map[string]*TableStats)
		}
		if localStats[schema][table] == nil {
			localStats[schema][table] = &TableStats{}
		}
		s := localStats[schema][table]
		switch statsType {
		case "INSERT":
			s.Insert++
		case "UPDATE":
			s.Update++
		case "DELETE":
			s.Delete++
		case "DDL":
			s.DDL++
		}
	}

	tableMap := make(map[uint64][2]string)

	// 单文件模式下不再打印单独进度，统一由全局进度打印
	// showProgress := len(a.cfg.BinlogFiles) <= 1

	// 4. 事件处理循环（Consumer）
	firstEventChecked := false

	// 事务状态跟踪
	var (
		currTxnGtid      string
		currTxnRows      int
		currTxnStartTime time.Time
		currTxnTables    = make(map[string]bool)
		inTxn            bool
	)

	// 辅助函数：重置事务状态
	resetTxn := func() {
		currTxnGtid = ""
		currTxnRows = 0
		currTxnStartTime = time.Time{}
		currTxnTables = make(map[string]bool)
		inTxn = false
	}

	// 辅助函数：检查并记录大事务
	checkBigTxn := func(endTime time.Time) {
		if a.cfg.BigTxnThreshold > 0 && currTxnRows >= a.cfg.BigTxnThreshold {
			tables := make([]string, 0, len(currTxnTables))
			for t := range currTxnTables {
				tables = append(tables, t)
			}
			sort.Strings(tables)

			// 如果开始时间未设置，使用结束时间作为近似
			startTime := currTxnStartTime
			if startTime.IsZero() {
				startTime = endTime
			}

			a.addBigTxn(BigTxnInfo{
				Filename:  filename,
				StartTime: startTime,
				EndTime:   endTime,
				Gtid:      currTxnGtid,
				Rows:      currTxnRows,
				Tables:    tables,
			})
		}
	}

	// 性能优化：只有在开启大事务分析时才跟踪事务状态
	trackBigTxn := a.cfg.BigTxnThreshold > 0

	for {
		select {
		case err := <-errChan:
			// 合并结果前返回错误
			a.mergeStats(localStats)
			return err
		default:
		}

		var ev *replication.BinlogEvent
		var ok bool

		select {
		case ev, ok = <-eventChan:
			if !ok {
				// Channel Closed
				select {
				case err := <-errChan:
					a.mergeStats(localStats)
					return err
				default:
					a.mergeStats(localStats)
					return nil
				}
			}
		case err := <-errChan:
			a.mergeStats(localStats)
			return err
		}

		atomic.AddInt64(&a.totalEvents, 1)

		// 5. 检查事件时间
		ts := time.Unix(int64(ev.Header.Timestamp), 0)

		// 快速失败检查：如果这是第一个有效时间戳事件，且时间已经超过结束时间
		// 说明整个文件都在时间范围之外，可以直接跳过并通知调度器
		if !firstEventChecked && ev.Header.Timestamp > 0 {
			firstEventChecked = true
			if ts.After(a.cfg.EndTime) {
				// 不需要合并统计，因为没有有效数据
				return ErrTimeOver
			}
			// 只有通过了快速失败检查的文件，才算作真正开始分析
			// (注意：这里在多线程下会并发调用，需要锁)
			// 但如果在单文件模式下已经在 Run() 中添加了，这里再添加会重复吗？
			// Run() 中只在 len==1 时添加。runParallel 中调用 processBinlogFile 时，len > 1。
			// 所以这里只需判断 len > 1 时添加。
			if len(a.cfg.BinlogFiles) > 1 {
				a.addAnalyzedFile(filename)
			}
		}

		// 如果当前事件时间已超过结束时间，停止分析该文件
		if ts.After(a.cfg.EndTime) {
			// fmt.Printf("[%s] 达到结束时间，停止。\n", filename)
			a.mergeStats(localStats)
			return nil
		}

		isBefore := ts.Before(a.cfg.StartTime)

		// 6. 根据事件类型进行处理
		switch e := ev.Event.(type) {
		case *replication.GTIDEvent:
			if trackBigTxn {
				if inTxn {
					checkBigTxn(ts)
				}
				resetTxn()
				inTxn = true
				currTxnStartTime = ts
				// 格式化 GTID: SID:GNO
				u := e.SID
				currTxnGtid = fmt.Sprintf("%x-%x-%x-%x-%x:%d",
					u[0:4], u[4:6], u[6:8], u[8:10], u[10:], e.GNO)
			}

		case *replication.MariadbGTIDEvent:
			if trackBigTxn {
				if inTxn {
					checkBigTxn(ts)
				}
				resetTxn()
				inTxn = true
				currTxnStartTime = ts
				currTxnGtid = e.GTID.String()
			}

		case *replication.XIDEvent:
			if trackBigTxn {
				checkBigTxn(ts)
				resetTxn()
			}

		case *replication.RotateEvent:
			// 如果发生 Rotate，说明该文件结束了（通常意味着流式读取会切换到下一个文件）
			// 但我们在多文件模式下，只分析当前指定的文件。
			// 不过，StartSync 会自动跟随 Rotate。
			// 既然我们手动控制文件列表，最好是读到 RotateEvent 就停止？
			// 不，StartSync(file) 如果不指定 StopOnRotate，它会自动切换到下一个。
			// 这会导致重复分析！
			// **关键点**：我们需要在遇到 RotateEvent 且 NextLogName != CurrentFile 时停止吗？
			// 或者，我们可以依赖 EndTime。
			// 但如果 EndTime 很远，它会一直读下去。
			// **修正**：我们应该检查 NextLogName。
			nextLog := string(e.NextLogName)
			if nextLog != filename && len(a.cfg.BinlogFiles) > 1 {
				// 切换到了下一个文件，而我们是按文件并行的，所以当前 Worker 应该结束
				// fmt.Printf("[%s] 遇到 Rotate 到 %s，当前文件任务结束。\n", filename, nextLog)
				if trackBigTxn {
					checkBigTxn(ts) // 结算可能未完成的事务
				}
				a.mergeStats(localStats)
				return nil
			}
			// if showProgress {
			// 	fmt.Printf("切换到日志文件 %s\n", nextLog)
			// }

		case *replication.TableMapEvent:
			tableMap[e.TableID] = [2]string{string(e.Schema), string(e.Table)}

		case *replication.RowsEvent:
			if isBefore {
				continue
			}
			info, ok := tableMap[e.TableID]
			if !ok {
				continue
			}
			schema, table := info[0], info[1]

			// 大事务统计：累加行数
			if trackBigTxn {
				if !inTxn {
					inTxn = true
					currTxnStartTime = ts
				}
				currTxnRows += len(e.Rows)
				currTxnTables[schema+"."+table] = true
			}

			// 记录 DML
			switch ev.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				updateLocalStats(schema, table, "INSERT")
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				updateLocalStats(schema, table, "UPDATE")
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				updateLocalStats(schema, table, "DELETE")
			}

		case *replication.QueryEvent:
			if isBefore {
				continue
			}
			query := string(e.Query)

			// 事务边界检测
			if trackBigTxn {
				if query == "BEGIN" {
					// 如果已经在事务中且有 GTID，则 BEGIN 只是事务的一部分，不重置
					if !inTxn || currTxnGtid == "" {
						if inTxn {
							checkBigTxn(ts)
						}
						resetTxn()
						inTxn = true
						currTxnStartTime = ts
					}
				} else if query == "COMMIT" || query == "ROLLBACK" {
					checkBigTxn(ts)
					resetTxn()
				}
			}

			if isDDL(query) {
				schema := string(e.Schema)
				table := extractTableFromDDL(query)
				if table == "" {
					table = "UNKNOWN"
				}
				updateLocalStats(schema, table, "DDL")
			}
		}
	}
}

// mergeStats 将局部统计数据合并到全局统计中 (线程安全)
func (a *Analyzer) mergeStats(localStats map[string]map[string]*TableStats) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for schema, tables := range localStats {
		if a.stats[schema] == nil {
			a.stats[schema] = make(map[string]*TableStats)
		}
		for table, s := range tables {
			if a.stats[schema][table] == nil {
				a.stats[schema][table] = &TableStats{}
			}
			globalS := a.stats[schema][table]
			globalS.Insert += s.Insert
			globalS.Update += s.Update
			globalS.Delete += s.Delete
			globalS.DDL += s.DDL
		}
	}
}

// recordDML 记录 DML (Data Manipulation Language) 操作统计
// 注意：此方法仅在单线程模式或 mergeStats 中使用，直接调用不再安全
// 为了兼容性保留，但不再直接使用
func (a *Analyzer) recordDML(schema, table string, eventType replication.EventType) {
	// Deprecated implementation
}

// recordDDL 记录 DDL (Data Definition Language) 操作统计
func (a *Analyzer) recordDDL(schema, table string) {
	// Deprecated implementation
}

// getStats 获取指定表的统计对象，如果不存在则初始化
func (a *Analyzer) getStats(schema, table string) *TableStats {
	// Unsafe for concurrent use
	if a.stats[schema] == nil {
		a.stats[schema] = make(map[string]*TableStats)
	}
	if a.stats[schema][table] == nil {
		a.stats[schema][table] = &TableStats{}
	}
	return a.stats[schema][table]
}

// PrintStats 打印最终的统计结果到控制台
func (a *Analyzer) PrintStats() {
	// 打印已分析的文件列表
	a.fileMu.Lock()
	sort.Strings(a.analyzedFiles)
	files := a.analyzedFiles
	a.fileMu.Unlock()

	fmt.Println("\n=== 分析结果 ===")

	if len(files) > 0 {
		fmt.Printf("已有效分析的 Binlog 文件 (共 %d 个):\n", len(files))
		// 如果文件太多，折叠显示
		if len(files) > 20 {
			for i := 0; i < 10; i++ {
				fmt.Printf("  - %s\n", files[i])
			}
			fmt.Printf("  ... (中间省略 %d 个文件) ...\n", len(files)-20)
			for i := len(files) - 10; i < len(files); i++ {
				fmt.Printf("  - %s\n", files[i])
			}
		} else {
			for _, f := range files {
				fmt.Printf("  - %s\n", f)
			}
		}
		fmt.Println("--------------------------------")
	}

	// 打印大事务信息
	a.bigTxnMu.Lock()
	if len(a.bigTxns) > 0 {
		fmt.Printf("\n=== 大事务分析报告 (阈值 > %d 行) ===\n", a.cfg.BigTxnThreshold)
		fmt.Printf("共发现 %d 个大事务。\n", len(a.bigTxns))

		// 按行数降序排序
		sort.Slice(a.bigTxns, func(i, j int) bool {
			return a.bigTxns[i].Rows > a.bigTxns[j].Rows
		})

		// 生成独立报告文件
		reportFilename := fmt.Sprintf("big_transactions_%s.txt", time.Now().Format("20060102_150405"))
		file, err := os.Create(reportFilename)
		if err != nil {
			fmt.Printf("警告: 无法创建大事务报告文件: %v\n", err)
		} else {
			defer file.Close()
			fmt.Fprintf(file, "=== 大事务分析报告 ===\n")
			fmt.Fprintf(file, "生成时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))
			fmt.Fprintf(file, "行数阈值: %d\n", a.cfg.BigTxnThreshold)
			fmt.Fprintf(file, "总数: %d\n\n", len(a.bigTxns))

			for i, txn := range a.bigTxns {
				duration := txn.EndTime.Sub(txn.StartTime)
				gtidStr := txn.Gtid
				if gtidStr == "" {
					gtidStr = "(无 GTID)"
				}
				fmt.Fprintf(file, "[%d]\n", i+1)
				fmt.Fprintf(file, "  Binlog文件: %s\n", txn.Filename)
				fmt.Fprintf(file, "  开始时间:   %s\n", txn.StartTime.Format("2006-01-02 15:04:05"))
				fmt.Fprintf(file, "  结束时间:   %s\n", txn.EndTime.Format("2006-01-02 15:04:05"))
				fmt.Fprintf(file, "  持续时间:   %v\n", duration)
				fmt.Fprintf(file, "  影响行数:   %d\n", txn.Rows)
				fmt.Fprintf(file, "  GTID:       %s\n", gtidStr)
				if len(txn.Tables) > 0 {
					fmt.Fprintf(file, "  涉及表:     %v\n", txn.Tables)
				}
				fmt.Fprintf(file, "\n")
			}
			fmt.Printf("详细大事务报告已保存至独立文件: %s\n", reportFilename)
		}

		// 终端显示摘要 (前 20 个)
		limit := 20
		fmt.Printf("\nTop %d 大事务预览:\n", limit)
		if len(a.bigTxns) < limit {
			limit = len(a.bigTxns)
		}

		for i := 0; i < limit; i++ {
			txn := a.bigTxns[i]
			duration := txn.EndTime.Sub(txn.StartTime)
			gtidStr := txn.Gtid
			if gtidStr == "" {
				gtidStr = "(无 GTID)"
			}
			// 优化显示格式
			fmt.Printf("[%d] %s | 行数: %d | 耗时: %v | 开始: %s\n",
				i+1, txn.Filename, txn.Rows, duration, txn.StartTime.Format("2006-01-02 15:04:05"))
		}
		if len(a.bigTxns) > limit {
			fmt.Printf("... 更多 %d 个大事务请查看报告文件 ...\n", len(a.bigTxns)-limit)
		}
		fmt.Println("--------------------------------")
	}
	a.bigTxnMu.Unlock()

	for schema, tables := range a.stats {
		fmt.Printf("库名: %s\n", schema)
		for table, s := range tables {
			// 如果该表没有任何操作，则跳过不显示
			if s.Insert == 0 && s.Update == 0 && s.Delete == 0 && s.DDL == 0 {
				continue
			}
			fmt.Printf("  表名: %s -> 插入: %d, 更新: %d, 删除: %d, DDL: %d\n",
				table, s.Insert, s.Update, s.Delete, s.DDL)
		}
	}
}

// isDDL 简单判断 SQL 语句是否为 DDL
func isDDL(query string) bool {
	q := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(q, "CREATE") ||
		strings.HasPrefix(q, "ALTER") ||
		strings.HasPrefix(q, "DROP") ||
		strings.HasPrefix(q, "TRUNCATE") ||
		strings.HasPrefix(q, "RENAME")
}

// extractTableFromDDL 从 DDL 语句中提取表名
// 注意：这是一个简化的实现，使用正则表达式匹配。
// 局限性：
// 1. 如果 SQL 复杂（如包含注释、子查询等），可能提取失败。
// 2. 如果没有使用 `USE db`，且 SQL 中使用 `db.table` 格式，这里尝试进行了简单处理。
func extractTableFromDDL(query string) string {
	// 预处理：将换行符等多余空白替换为单个空格
	q := strings.Join(strings.Fields(query), " ")

	// 正则表达式说明：
	// (?i)             : 开启不区分大小写模式
	// (?:TABLE|VIEW)   : 匹配 TABLE 或 VIEW 关键字（非捕获组）
	// \s+              : 匹配至少一个空格
	// (?:IF\s+(?:NOT\s+)?EXISTS\s+)? : 可选匹配 IF EXISTS 或 IF NOT EXISTS
	// (?:`?)           : 可选匹配起始反引号
	// ([\w\.]+)        : 捕获组1：匹配表名（包含字母、数字、下划线、点号）
	re := regexp.MustCompile(`(?i)(?:TABLE|VIEW)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?(?:` + "`" + `?)([\w\.]+)`)
	matches := re.FindStringSubmatch(q)
	if len(matches) > 1 {
		// matches[1] 是捕获到的表名部分
		// 处理 db.table 的情况，只返回 table 部分
		parts := strings.Split(matches[1], ".")
		if len(parts) > 1 {
			return parts[1] // 返回点号后面的部分作为表名
		}
		return matches[1]
	}
	return ""
}
