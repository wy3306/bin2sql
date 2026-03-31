package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"bin2sql/internal/webui"
)

func main() {
	httpServer := &http.Server{
		Addr:              "0.0.0.0:9000",
		ReadHeaderTimeout: 10 * time.Second,
	}

	server, err := webui.NewServer(func() {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = httpServer.Shutdown(ctx)
		}()
	})
	if err != nil {
		log.Fatalf("初始化 Web 服务失败: %v", err)
	}
	httpServer.Handler = server.Handler()

	log.Printf("Binlog Web UI 已启动，监听 http://0.0.0.0:9000")
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Web 服务启动失败: %v", err)
	}
}
