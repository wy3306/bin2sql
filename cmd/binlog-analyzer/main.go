package main

import (
	"flag"
	"fmt"
	"os"
	"syscall"
	"time"

	"golang.org/x/term"

	"bin2sql/pkg/analyzer"
	"bin2sql/pkg/seeker"
)

// main 是程序的入口函数
func main() {
	// 1. 定义命令行参数
	// 使用 flag 包来解析启动参数
	host := flag.String("host", "127.0.0.1", "MySQL 主机地址")
	port := flag.Int("port", 3306, "MySQL 端口")
	user := flag.String("user", "root", "MySQL 用户名")
	passwordFlag := flag.String("password", "", "MySQL 密码 (可选，如果不提供则通过交互式输入)")
	// 时间参数接收字符串格式，稍后需要解析
	startTimeStr := flag.String("start-time", "", "开始时间 (格式: YYYY-MM-DD HH:MM:SS)")
	endTimeStr := flag.String("end-time", "", "结束时间 (格式: YYYY-MM-DD HH:MM:SS)")
	// start-file 是可选参数，用于手动指定起始文件，绕过自动查找逻辑
	startFileFlag := flag.String("start-file", "", "手动指定起始 Binlog 文件名 (如果指定，将跳过自动查找)")
	// 大事务分析阈值
	bigTxnThreshold := flag.Int("big-txn-threshold", 0, "大事务行数阈值 (例如 1000)，0 表示不开启")

	// 解析命令行参数
	flag.Parse()

	// 2. 参数基本校验
	if *startTimeStr == "" || *endTimeStr == "" {
		fmt.Println("请提供 -start-time 和 -end-time 参数")
		flag.Usage() // 打印使用说明
		os.Exit(1)
	}

	// 处理密码逻辑：如果命令行未提供密码，则提示用户输入（隐藏显示）
	var finalPassword string
	if *passwordFlag != "" {
		finalPassword = *passwordFlag
	} else {
		fmt.Print("请输入 MySQL 密码: ")
		// 使用 golang.org/x/term 包安全地读取密码，不回显到终端
		bytePassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			fmt.Printf("\n读取密码失败: %v\n", err)
			os.Exit(1)
		}
		finalPassword = string(bytePassword)
		fmt.Println() // 换行，因为 ReadPassword 不会输出换行符
	}

	// 3. 解析时间参数
	layout := "2006-01-02 15:04:05" // Go 语言的标准时间格式模板
	// 使用 ParseInLocation 解析本地时间，而不是默认的 UTC
	startTime, err := time.ParseInLocation(layout, *startTimeStr, time.Local)
	if err != nil {
		fmt.Printf("错误: 起始时间格式不正确: %v\n请使用格式: 'YYYY-MM-DD HH:MM:SS'\n", err)
		os.Exit(1)
	}

	endTime, err := time.ParseInLocation(layout, *endTimeStr, time.Local)
	if err != nil {
		fmt.Printf("错误: 结束时间格式不正确: %v\n请使用格式: 'YYYY-MM-DD HH:MM:SS'\n", err)
		os.Exit(1)
	}

	var startFile string

	// 4. 确定起始 Binlog 文件
	// 无论是手动指定还是自动查找，我们都尝试获取完整的文件列表以支持并行分析

	seekerCfg := seeker.Config{
		Host:     *host,
		Port:     *port,
		User:     *user,
		Password: finalPassword,
	}

	if *startFileFlag != "" {
		// 情况 A: 用户手动指定了起始文件
		startFile = *startFileFlag
		fmt.Printf("使用用户指定的起始文件: %s\n", startFile)
	} else {
		// 情况 B: 自动查找起始文件
		var err error
		startFile, err = seeker.FindStartBinlog(seekerCfg, startTime)
		if err != nil {
			fmt.Printf("查找起始 binlog 失败: %v\n", err)
			fmt.Println("提示: 如果数据库不支持 SHOW BINARY LOGS，请使用 -start-file 参数手动指定起始文件。")
			os.Exit(1)
		}
		fmt.Printf("找到起始 binlog 文件: %s\n", startFile)
	}

	// 5. 尝试获取文件列表以支持多线程并行分析
	// 如果环境支持 SHOW BINARY LOGS，这将极大提升性能
	var binlogFiles []string
	files, err := seeker.GetBinlogsAfter(seekerCfg, startFile)
	if err != nil {
		fmt.Printf("注意: 无法获取 Binlog 文件列表 (%v)，将降级为单线程流式分析模式。\n", err)
		binlogFiles = nil // 保持为空，Analyzer 会自动处理
	} else {
		binlogFiles = files
	}

	// 6. 初始化分析器
	anaCfg := analyzer.Config{
		Host:            *host,
		Port:            *port,
		User:            *user,
		Password:        finalPassword, // 使用最终确定的密码
		StartFile:       startFile,
		BinlogFiles:     binlogFiles, // 传递文件列表
		StartTime:       startTime,
		EndTime:         endTime,
		BigTxnThreshold: *bigTxnThreshold,
	}

	ana := analyzer.New(anaCfg)

	// 7. 运行分析
	// Run() 会阻塞直到分析完成或出错
	if err := ana.Run(); err != nil {
		fmt.Printf("分析失败: %v\n", err)
		// 注意：即使 Run 返回错误，可能已经统计了一部分数据，
		// 所以我们仍然尝试打印当前的统计结果。
	}

	// 8. 打印最终结果
	ana.PrintStats()
}
