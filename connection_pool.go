package hasqlite

import (
	"context"
	"sync"
	"time"
	"gorm.io/gorm"
)

type ConnectionPool struct {
	mu          sync.RWMutex
	connections map[string]*gorm.DB
	maxConns    int
	maxIdleTime time.Duration
	lastUsed    map[string]time.Time
}

func NewConnectionPool(maxConns int, maxIdleTime time.Duration) *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string]*gorm.DB),
		maxConns:    maxConns,
		maxIdleTime: maxIdleTime,
		lastUsed:    make(map[string]time.Time),
	}
}

func (cp *ConnectionPool) Get(key string) (*gorm.DB, bool) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	db, exists := cp.connections[key]
	if exists {
		cp.lastUsed[key] = time.Now()
	}
	return db, exists
}

func (cp *ConnectionPool) Put(key string, db *gorm.DB) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	if len(cp.connections) >= cp.maxConns {
		// 清理最旧的连接
		cp.evictOldest()
	}
	
	cp.connections[key] = db
	cp.lastUsed[key] = time.Now()
}

func (cp *ConnectionPool) evictOldest() {
	var oldestKey string
	var oldestTime time.Time
	
	for key, lastUsed := range cp.lastUsed {
		if oldestKey == "" || lastUsed.Before(oldestTime) {
			oldestKey = key
			oldestTime = lastUsed
		}
	}
	
	if oldestKey != "" {
		delete(cp.connections, oldestKey)
		delete(cp.lastUsed, oldestKey)
	}
}

func (cp *ConnectionPool) Cleanup() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	now := time.Now()
	for key, lastUsed := range cp.lastUsed {
		if now.Sub(lastUsed) > cp.maxIdleTime {
			delete(cp.connections, key)
			delete(cp.lastUsed, key)
		}
	}
}