package hasqlite

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/glebarez/sqlite"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// SyncStatus 同步状态
type SyncStatus string

const (
	SyncIdle    SyncStatus = "idle"    // 空闲
	SyncRunning SyncStatus = "running" // 同步中
	SyncFailed  SyncStatus = "failed"  // 同步失败
)

// SyncRecord 同步记录
type SyncRecord struct {
	ID            uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	SyncID        string    `gorm:"uniqueIndex;size:64" json:"sync_id"`
	SourceNode    string    `gorm:"size:64;not null" json:"source_node"`
	TargetNode    string    `gorm:"size:64;not null" json:"target_node"`
	DatabaseType  string    `gorm:"size:50;not null" json:"database_type"`
	StartTime     time.Time `gorm:"not null" json:"start_time"`
	EndTime       *time.Time `json:"end_time,omitempty"`
	Status        SyncStatus `gorm:"size:20;not null" json:"status"`
	RecordsCount  int64     `json:"records_count"`
	ErrorMessage  string    `gorm:"type:text" json:"error_message,omitempty"`
	Checksum      string    `gorm:"size:64" json:"checksum"`
}

// SyncManager 同步管理器
type SyncManager struct {
	mu              sync.RWMutex
	nodeManager     *NodeManager
	eventStore      *EventStore
	syncDB          *gorm.DB
	syncInterval    time.Duration
	ctx             context.Context
	cancel          context.CancelFunc
	isRunning       bool
	lastSyncTime    time.Time
	databasePaths   map[string]string // 数据库类型到路径的映射
	onSyncComplete  func(record *SyncRecord)
	onSyncError     func(err error)
}

// NewSyncManager 创建同步管理器
func NewSyncManager(nodeManager *NodeManager, eventStore *EventStore, syncDBPath string) (*SyncManager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	// 初始化同步记录数据库
	syncDB, err := gorm.Open(sqlite.Open(syncDBPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open sync database: %v", err)
	}
	
	// 启用WAL模式
	syncDB.Exec("PRAGMA journal_mode=WAL")
	
	// 自动迁移表结构
	if err := syncDB.AutoMigrate(&SyncRecord{}); err != nil {
		return nil, fmt.Errorf("failed to migrate sync schema: %v", err)
	}
	
	sm := &SyncManager{
		nodeManager:   nodeManager,
		eventStore:    eventStore,
		syncDB:        syncDB,
		syncInterval:  30 * time.Second, // 默认30秒同步一次
		ctx:           ctx,
		cancel:        cancel,
		databasePaths: make(map[string]string),
	}
	
	return sm, nil
}

// SetDatabasePath 设置数据库路径
func (sm *SyncManager) SetDatabasePath(dbType, path string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.databasePaths[dbType] = path
}

// SetSyncInterval 设置同步间隔
func (sm *SyncManager) SetSyncInterval(interval time.Duration) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.syncInterval = interval
}

// SetSyncCompleteCallback 设置同步完成回调
func (sm *SyncManager) SetSyncCompleteCallback(callback func(record *SyncRecord)) {
	sm.onSyncComplete = callback
}

// SetSyncErrorCallback 设置同步错误回调
func (sm *SyncManager) SetSyncErrorCallback(callback func(err error)) {
	sm.onSyncError = callback
}

// Start 启动同步管理器
func (sm *SyncManager) Start() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	if sm.isRunning {
		return fmt.Errorf("sync manager is already running")
	}
	
	sm.isRunning = true
	log.Info("Starting sync manager")
	
	// 启动同步循环
	go sm.syncLoop()
	
	return nil
}

// Stop 停止同步管理器
func (sm *SyncManager) Stop() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	if !sm.isRunning {
		return
	}
	
	log.Info("Stopping sync manager")
	sm.isRunning = false
	sm.cancel()
}

// SyncNow 立即执行同步
func (sm *SyncManager) SyncNow() error {
	if !sm.nodeManager.IsSecondary() {
		return fmt.Errorf("sync can only be initiated from secondary node")
	}
	
	peerNode := sm.nodeManager.GetPeerNode()
	if peerNode == nil || peerNode.Status != Healthy {
		return fmt.Errorf("primary node is not available")
	}
	
	return sm.performSync()
}

// GetLastSyncTime 获取最后同步时间
func (sm *SyncManager) GetLastSyncTime() time.Time {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastSyncTime
}

// GetSyncHistory 获取同步历史
func (sm *SyncManager) GetSyncHistory(limit int) ([]*SyncRecord, error) {
	var records []*SyncRecord
	query := sm.syncDB.Order("start_time DESC")
	
	if limit > 0 {
		query = query.Limit(limit)
	}
	
	if err := query.Find(&records).Error; err != nil {
		return nil, fmt.Errorf("failed to query sync history: %v", err)
	}
	
	return records, nil
}

// syncLoop 同步循环
func (sm *SyncManager) syncLoop() {
	ticker := time.NewTicker(sm.syncInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			// 只有备节点才执行定时同步
			if sm.nodeManager.IsSecondary() {
				peerNode := sm.nodeManager.GetPeerNode()
				if peerNode != nil && peerNode.Status == Healthy {
					if err := sm.performSync(); err != nil {
						log.Errorf("Sync failed: %v", err)
						if sm.onSyncError != nil {
							go sm.onSyncError(err)
						}
					}
				}
			}
		}
	}
}

// performSync 执行同步
func (sm *SyncManager) performSync() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	log.Debug("Starting database sync")
	
	// 为每个数据库类型执行同步
	for dbType, dbPath := range sm.databasePaths {
		if err := sm.syncDatabase(dbType, dbPath); err != nil {
			log.Errorf("Failed to sync database %s: %v", dbType, err)
			return err
		}
	}
	
	sm.lastSyncTime = time.Now()
	log.Debug("Database sync completed")
	
	return nil
}

// syncDatabase 同步单个数据库
func (sm *SyncManager) syncDatabase(dbType, dbPath string) error {
	// 创建同步记录
	syncRecord := &SyncRecord{
		SyncID:       sm.generateSyncID(dbType),
		SourceNode:   sm.nodeManager.GetPeerNode().ID,
		TargetNode:   sm.nodeManager.GetNodeInfo().ID,
		DatabaseType: dbType,
		StartTime:    time.Now(),
		Status:       SyncRunning,
	}
	
	// 保存同步记录
	if err := sm.syncDB.Create(syncRecord).Error; err != nil {
		return fmt.Errorf("failed to create sync record: %v", err)
	}
	
	// 执行实际的数据库文件同步
	err := sm.copyDatabaseFile(dbType, dbPath)
	
	// 更新同步记录
	endTime := time.Now()
	syncRecord.EndTime = &endTime
	
	if err != nil {
		syncRecord.Status = SyncFailed
		syncRecord.ErrorMessage = err.Error()
	} else {
		syncRecord.Status = SyncIdle
		// 计算记录数（这里简化处理）
		syncRecord.RecordsCount = sm.getDatabaseRecordCount(dbPath)
		syncRecord.Checksum = sm.calculateDatabaseChecksum(dbPath)
	}
	
	// 更新同步记录
	if updateErr := sm.syncDB.Save(syncRecord).Error; updateErr != nil {
		log.Errorf("Failed to update sync record: %v", updateErr)
	}
	
	if err == nil && sm.onSyncComplete != nil {
		go sm.onSyncComplete(syncRecord)
	}
	
	return err
}

// copyDatabaseFile 复制数据库文件
func (sm *SyncManager) copyDatabaseFile(dbType, targetPath string) error {
	// TODO: 实现从主节点获取数据库文件的逻辑
	// 这里应该通过网络从主节点下载数据库文件
	// 为了演示，这里只是创建一个简单的文件复制逻辑
	
	peerNode := sm.nodeManager.GetPeerNode()
	if peerNode == nil {
		return fmt.Errorf("peer node not available")
	}
	
	// 构造源文件路径（这里假设主节点的文件路径）
	sourcePath := sm.constructRemoteDBPath(peerNode.Address, dbType)
	
	// 创建备份文件
	backupPath := targetPath + ".backup"
	if err := sm.createBackup(targetPath, backupPath); err != nil {
		log.Warnf("Failed to create backup for %s: %v", targetPath, err)
	}
	
	// 执行文件同步（这里需要实现实际的网络传输逻辑）
	if err := sm.downloadDatabaseFile(sourcePath, targetPath); err != nil {
		// 如果同步失败，尝试恢复备份
		if restoreErr := sm.restoreBackup(backupPath, targetPath); restoreErr != nil {
			log.Errorf("Failed to restore backup: %v", restoreErr)
		}
		return fmt.Errorf("failed to download database file: %v", err)
	}
	
	// 清理备份文件
	if err := os.Remove(backupPath); err != nil {
		log.Warnf("Failed to remove backup file %s: %v", backupPath, err)
	}
	
	return nil
}

// createBackup 创建备份
func (sm *SyncManager) createBackup(sourcePath, backupPath string) error {
	if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
		return nil // 源文件不存在，无需备份
	}
	
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer sourceFile.Close()
	
	backupFile, err := os.Create(backupPath)
	if err != nil {
		return err
	}
	defer backupFile.Close()
	
	_, err = io.Copy(backupFile, sourceFile)
	return err
}

// restoreBackup 恢复备份
func (sm *SyncManager) restoreBackup(backupPath, targetPath string) error {
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup file does not exist")
	}
	
	return os.Rename(backupPath, targetPath)
}

// constructRemoteDBPath 构造远程数据库路径
func (sm *SyncManager) constructRemoteDBPath(peerAddress, dbType string) string {
	// TODO: 实现根据对等节点地址和数据库类型构造远程路径的逻辑
	return fmt.Sprintf("http://%s/api/database/%s", peerAddress, dbType)
}

// downloadDatabaseFile 下载数据库文件
func (sm *SyncManager) downloadDatabaseFile(sourceURL, targetPath string) error {
	// TODO: 实现实际的HTTP下载逻辑
	// 这里应该从主节点下载数据库文件
	log.Debugf("Downloading database from %s to %s", sourceURL, targetPath)
	
	// 确保目标目录存在
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return fmt.Errorf("failed to create target directory: %v", err)
	}
	
	// 这里应该实现实际的网络下载逻辑
	// 为了演示，这里只是创建一个空文件
	file, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	file.Close()
	
	return nil
}

// getDatabaseRecordCount 获取数据库记录数
func (sm *SyncManager) getDatabaseRecordCount(dbPath string) int64 {
	// 简化实现，实际应该连接数据库查询记录数
	if stat, err := os.Stat(dbPath); err == nil {
		return stat.Size() // 使用文件大小作为简化的记录数指标
	}
	return 0
}

// calculateDatabaseChecksum 计算数据库校验和
func (sm *SyncManager) calculateDatabaseChecksum(dbPath string) string {
	// 简化实现，实际应该计算文件的MD5或SHA256
	if stat, err := os.Stat(dbPath); err == nil {
		return fmt.Sprintf("%x", stat.Size()+stat.ModTime().Unix())
	}
	return ""
}

// generateSyncID 生成同步ID
func (sm *SyncManager) generateSyncID(dbType string) string {
	return fmt.Sprintf("sync_%s_%s_%d", 
		sm.nodeManager.GetNodeInfo().ID, dbType, time.Now().UnixNano())
}

// Close 关闭同步管理器
func (sm *SyncManager) Close() error {
	sm.Stop()
	
	// 关闭同步数据库连接
	sqlDB, err := sm.syncDB.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying sync DB: %v", err)
	}
	
	return sqlDB.Close()
}