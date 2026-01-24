package seeker

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// Config 定义 Seeker 的配置参数
type Config struct {
	Host     string // MySQL 主机地址
	Port     int    // MySQL 端口
	User     string // MySQL 用户名
	Password string // MySQL 密码
}

// FindStartBinlog 查找包含目标起始时间的二进制日志文件。
// 该函数使用二分查找算法在所有可用的 Binlog 文件中快速定位。
//
// 参数:
//
//	cfg: 数据库连接配置
//	targetTime: 用户指定的分析起始时间
//
// 返回值:
//
//	string: 找到的起始 Binlog 文件名
//	error: 错误信息
func FindStartBinlog(cfg Config, targetTime time.Time) (string, error) {
	// 1. 连接到 MySQL 以获取二进制日志列表
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	// client.Connect 用于执行普通 SQL 查询
	c, err := client.Connect(addr, cfg.User, cfg.Password, "")
	if err != nil {
		return "", fmt.Errorf("连接数据库失败: %v", err)
	}
	defer c.Close()

	// 2. 执行 SHOW BINARY LOGS 获取所有日志文件
	// 结果包含 Log_name 和 File_size 两列
	res, err := c.Execute("SHOW BINARY LOGS")
	if err != nil {
		return "", fmt.Errorf("执行 show binary logs 失败: %v", err)
	}

	var logFiles []string
	// 遍历结果集提取文件名
	for i := 0; i < res.RowNumber(); i++ {
		name, _ := res.GetString(i, 0)
		logFiles = append(logFiles, name)
	}

	if len(logFiles) == 0 {
		return "", fmt.Errorf("未找到二进制日志")
	}

	// 定义辅助函数：获取指定 Binlog 文件的起始时间
	// 原理：读取文件头部的第一个事件（通常是 FormatDescriptionEvent），其时间戳即为文件创建时间
	// 这是一个核心探针函数，配合二分查找算法使用，用于快速探测文件的时间范围。
	getStartTime := func(filename string) (time.Time, error) {
		var lastErr error
		// 增加重试机制，应对网络抖动或瞬时连接超时
		for retry := 0; retry < 3; retry++ {
			if retry > 0 {
				// 简单的退避策略：1s, 2s
				time.Sleep(time.Second * time.Duration(retry))
			}

			// 1. 创建临时的“伪装 Slave”配置
			// 我们需要建立一个新的临时连接来专门读取这个文件的头部
			syncerCfg := replication.BinlogSyncerConfig{
				ServerID: uint32(rand.Intn(1000000) + 1000), // 使用随机 ID，避免与主程序冲突
				Flavor:   "mysql",
				Host:     cfg.Host,
				Port:     uint16(cfg.Port),
				User:     cfg.User,
				Password: cfg.Password,
				// 设置读取超时，防止底层连接无限等待
				ReadTimeout: 10 * time.Second,
			}
			// 2. 建立连接：创建 BinlogSyncer 实例
			syncer := replication.NewBinlogSyncer(syncerCfg)

			// 使用匿名函数确保 defer 在每次迭代结束时执行
			t, err := func() (time.Time, error) {
				defer syncer.Close() // 函数结束时立即断开连接，用完即弃

				// 3. 发起同步请求 (关键点)
				// StartSync 告诉 MySQL：“我要从这个文件的第 4 个字节开始读”
				// Pos: 4 是 Binlog 的标准起始位置（前 4 字节是魔数）
				streamer, err := syncer.StartSync(mysql.Position{Name: filename, Pos: 4})
				if err != nil {
					return time.Time{}, err
				}

				// 4. 设置超时防卡死
				// 只是读文件头，理论上几毫秒就够了。设置 5 秒超时防止网络问题导致程序挂起。
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				// 5. 循环读取事件
				// 我们通常读到的第一个事件就是 FormatDescriptionEvent。
				// 只要读到一个有效的时间戳 (>0)，就认为它是文件的起始时间，立即返回。
				for {
					ev, err := streamer.GetEvent(ctx)
					if err != nil {
						return time.Time{}, err
					}
					// FormatDescriptionEvent 总是文件的第一个事件
					// 它的 Timestamp 字段代表文件创建时间（Master 上的时间）
					if ev.Header.Timestamp > 0 {
						return time.Unix(int64(ev.Header.Timestamp), 0), nil
					}
				}
			}()

			if err == nil {
				return t, nil
			}
			lastErr = err
			// fmt.Printf("警告: 检查文件 %s 失败 (重试 %d/3): %v\n", filename, retry+1, err)
		}
		return time.Time{}, fmt.Errorf("重试3次后仍然失败: %v", lastErr)
	}

	// 3. 使用二分查找算法定位文件
	// 目标：找到满足 StartTime(file) <= targetTime 的 *最新*（即索引最大）的文件
	// 这样可以确保我们从该文件开始分析时，能覆盖到 targetTime
	low := 0
	high := len(logFiles) - 1
	bestIdx := -1

	fmt.Println("正在搜索起始 binlog 文件...")

	for low <= high {
		mid := low + (high-low)/2
		filename := logFiles[mid]

		// 获取当前中间文件的起始时间
		t, err := getStartTime(filename)
		if err != nil {
			// 如果无法读取文件（可能被物理删除或权限问题），返回错误
			return "", fmt.Errorf("检查文件 %s 失败: %v", filename, err)
		}

		fmt.Printf("已检查 %s: %v\n", filename, t)

		if t.After(targetTime) {
			// 如果当前文件的起始时间 晚于 目标时间
			// 说明目标时间在更早的文件中，向左查找
			high = mid - 1
		} else {
			// 如果当前文件的起始时间 早于或等于 目标时间
			// 说明这个文件 *可能* 是我们要找的，但也可能有更晚的文件也满足条件（更接近目标时间）
			// 记录当前索引为最佳候选，并尝试向右查找更近的文件
			bestIdx = mid
			low = mid + 1
		}
	}

	if bestIdx == -1 {
		// 如果所有文件的起始时间都晚于 targetTime
		// 这意味着目标时间的数据可能已经被清理掉了（Binlog 过期删除）
		// 或者目标时间设置得太早
		fmt.Println("警告：起始时间早于最早可用的 binlog。将从第一个文件开始。")
		return logFiles[0], nil
	}

	// 返回找到的最佳文件
	return logFiles[bestIdx], nil
}

// GetBinlogsAfter 获取指定起始文件及其之后的所有 Binlog 文件列表
// 用于支持多文件并行分析
func GetBinlogsAfter(cfg Config, startFile string) ([]string, error) {
	// 1. 连接到 MySQL
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	c, err := client.Connect(addr, cfg.User, cfg.Password, "")
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}
	defer c.Close()

	// 2. 执行 SHOW BINARY LOGS
	res, err := c.Execute("SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("执行 SHOW BINARY LOGS 失败: %v", err)
	}

	var fileList []string
	found := false

	// 3. 遍历查找起始文件，并收集后续文件
	for i := 0; i < res.RowNumber(); i++ {
		name, _ := res.GetString(i, 0)
		if name == startFile {
			found = true
		}
		if found {
			fileList = append(fileList, name)
		}
	}

	if !found {
		return nil, fmt.Errorf("在服务器上未找到指定的文件: %s", startFile)
	}

	return fileList, nil
}
