package hasqlite

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	log "github.com/sirupsen/logrus"
)

type GracefulShutdown struct {
	mu        sync.Mutex
	shutdowns []func() error
	timeout   time.Duration
}

func NewGracefulShutdown(timeout time.Duration) *GracefulShutdown {
	return &GracefulShutdown{
		shutdowns: make([]func() error, 0),
		timeout:   timeout,
	}
}

func (gs *GracefulShutdown) Register(shutdown func() error) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.shutdowns = append(gs.shutdowns, shutdown)
}

func (gs *GracefulShutdown) Wait() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-sigChan
	log.Info("Received shutdown signal, starting graceful shutdown...")
	
	ctx, cancel := context.WithTimeout(context.Background(), gs.timeout)
	defer cancel()
	
	done := make(chan struct{})
	go func() {
		gs.executeShutdowns()
		close(done)
	}()
	
	select {
	case <-done:
		log.Info("Graceful shutdown completed")
	case <-ctx.Done():
		log.Warn("Graceful shutdown timeout, forcing exit")
	}
}

func (gs *GracefulShutdown) executeShutdowns() {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	
	// 反向执行关闭函数
	for i := len(gs.shutdowns) - 1; i >= 0; i-- {
		if err := gs.shutdowns[i](); err != nil {
			log.Errorf("Error during shutdown: %v", err)
		}
	}
}