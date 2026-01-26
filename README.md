# Binlog 分析工具 (Binlog Analyzer)

这是一个高性能的 MySQL Binlog 分析工具，专为统计数据库操作（DML/DDL）和大事务分析而设计。

## 📅 版本更新日志 (2026-01-26)

相比最初版本，修复了关键 Bug 并引入了多项重磅功能：

### ✅ 修复的问题
1.  **时区解析错误**：修复了输入时间被误解析为 UTC 而导致分析范围偏差的问题。现在严格按照本地时区（CST/UTC+8）处理命令行时间参数。
2.  **连接超时重试**：解决了网络抖动导致的 `i/o timeout` 连接失败。新增了自动重试机制（指数退避策略，最多重试 3 次）。
3.  **多文件重复分析**：修复了在多线程并行分析模式下，文件切换（Rotate Event）可能导致同一文件被重复统计的 Bug。
4.  **兼容性增强**：解决了部分环境（如阿里云 RDS）不支持 `SHOW BINARY LOGS` 命令导致无法启动的问题，通过 `-start-file` 参数可手动绕过。
5.  **异常事件容错**：针对底层库解析特定字符集（如 `Expect odd item in DefaultCharset`）的 Panic 问题，增加了自动跳过机制，确保分析任务不中断。
6.  **统计报告导出**：分析完成后，除了在终端显示外，会自动将 DML/DDL 统计结果写入到 `dml_ddl_stats_YYYYMMDD_HHMMSS.txt` 文件中，方便后续查看和记录。

### 🚀 新增功能
1.  **多线程并行分析**：
    *   引入 Worker Pool 模式，自动并发分析多个 Binlog 文件。
    *   在处理大量 Binlog 时，性能提升显著（取决于 CPU 核心数）。
2.  **大事务分析 (Big Transaction Analysis)**：
    *   支持按“影响行数”阈值识别大事务。
    *   **独立报告**：自动生成独立的 `big_transactions_*.txt` 文件，详细记录大事务的 GTID、开始/结束时间、耗时、行数及涉及表。
    *   **零开销设计**：默认关闭。未开启时（阈值为 0），对性能无任何额外影响。
3.  **智能性能优化 (Smart Checksum)**：
    *   **低延迟极速模式**：启动时自动测量到数据库的 TCP 网络延迟。如果延迟 < 10ms（通常为局域网），自动关闭 Checksum 校验以最大化速度。
    *   **高延迟安全模式**：如果延迟 >= 10ms（通常为跨网段或公网），自动开启校验以确保数据完整性。
4.  **进度可视化**：
    *   实时显示全局处理进度（事件总数）。
    *   分析结束后展示实际处理的 Binlog 文件列表（自动折叠过长的列表）。
4.  **全中文支持**：代码注释、文档说明及程序输出全部本地化为中文，便于学习和维护。

---

## 🧠 分析原理

### 1. 模拟 Slave 协议
本工具基于 `go-mysql` 库，伪装成一个 MySQL Slave 节点。
*   向 MySQL Master 发送 `COM_BINLOG_DUMP` 命令。
*   通过复制协议（Replication Protocol）流式拉取 Binlog 事件。
*   **优势**：相比直接下载 Binlog 文件解析，这种方式实时性更强，且不需要物理文件访问权限，支持远程分析。

### 2. 高效文件定位 (Binary Search)
为了快速定位到用户指定的 `-start-time`，工具实现了一个二分查找算法（在 `pkg/seeker` 中）：
*   获取 MySQL 当前所有的 Binlog 文件列表。
*   对文件列表进行二分查找，快速锁定包含起始时间的那个文件。
*   避免了从第一个文件开始遍历的低效操作，极大缩短了启动时间。

### 3. 并行流水线处理 (Parallel Processing)
一旦确定了需要分析的文件范围，工具会启动一个 Worker Pool：
*   **调度器**：将待分析的 Binlog 文件分配给空闲的 Worker。
*   **Worker**：每个 Worker 独立建立一个数据库连接，负责解析分配到的文件。
*   **聚合器**：所有 Worker 将解析出的 DML/DDL 统计数据发送到主线程进行线程安全的合并。

### 4. 智能容错与重试
*   **网络波动**：内置指数退避重试机制，自动处理 `I/O Timeout`。
*   **解析异常**：遇到底层库无法解析的畸形事件（如非标准字符集），会自动跳过该事件并记录警告，保证整体分析任务不中断。

---

## 🛠 使用指南

### 1. 编译
使用内置脚本一键编译所有平台版本：
```bash
chmod +x build.sh
./build.sh
```
产物位于 `bin/` 目录：
*   `bin/binlog-analyzer-linux-amd64` (生产环境常用)
*   `bin/binlog-analyzer-linux-arm64`
*   `bin/binlog-analyzer-darwin-arm64` (Mac M1/M2/M3)

### 2. 基础用法 (统计 DML/DDL)
最常用的模式，统计指定时间段内各表的 Insert/Update/Delete/DDL 次数。

```bash
./bin/binlog-analyzer-linux-amd64 \
  -host 192.168.1.100 \
  -user root \
  -port 3306 \
  -password "123456" \
  -start-time "2026-01-25 00:00:00" \
  -end-time "2026-01-25 01:00:00"
```

### 3. 进阶用法 (开启大事务分析)
如果您怀疑数据库有大事务导致主从延迟，可以使用 `-big-txn-threshold` 参数。

**示例：查找影响行数超过 5000 行的事务**
```bash
./bin/binlog-analyzer-linux-amd64 \
  -host 192.168.1.100 \
  -user root \
  -port 3306 \
  -start-time "2026-01-25 00:00:00" \
  -end-time "2026-01-25 01:00:00" \
  -big-txn-threshold 5000
```
*   **终端输出**：显示 Top 20 大事务摘要。
*   **文件输出**：在当前目录生成 `big_transactions_20260125_xxxx.txt`，包含完整详情。

### 4. 特殊场景 (手动指定起始文件)
如果您的数据库权限受限（不支持 `SHOW BINARY LOGS`），或者是云数据库，您可以手动指定第一个 Binlog 文件：

```bash
./bin/binlog-analyzer-linux-amd64 \
  ... \
  -start-file "mysql-bin.000810"
```

---

## ⚙️ 参数说明

| 参数 | 说明 | 默认值 | 备注 |
| :--- | :--- | :--- | :--- |
| `-host` | MySQL 地址 | 127.0.0.1 | |
| `-port` | MySQL 端口 | 3306 | |
| `-user` | 用户名 | root | |
| `-password` | 密码 | (空) | 不填则启动后安全输入 |
| `-start-time` | **开始时间** | (必填) | 格式: YYYY-MM-DD HH:MM:SS |
| `-end-time` | **结束时间** | (必填) | 格式: YYYY-MM-DD HH:MM:SS |
| `-start-file` | 起始文件 | (空) | 用于绕过自动查找逻辑 |
| `-big-txn-threshold` | **大事务阈值** | 0 | 0 表示关闭。设置为 >0 的整数开启 |

## 📂 项目结构
*   `main.go` - 程序入口
*   `pkg/analyzer` - 核心分析逻辑（含多线程调度、大事务检测）
*   `pkg/seeker` - Binlog 文件定位逻辑（含二分查找、重试机制）
*   `docs/` - 架构文档

## ⚠️ 注意事项

1.  **GoldenDB 兼容性(基于MySQL的分布式数据库)**：暂不支持 **GoldenDB 61304** 版本。该版本的 Binlog 格式与标准 MySQL 存在差异，可能导致解析失败或数据不准确。
