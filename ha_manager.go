package hasqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"ws-oams/internal/database"

	// "github.com/glebarez/sqlite"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	// "gorm.io/gorm/logger"
)

// HAConfig 高可用配置
type HAConfig struct {
	NodeID            string        `json:"node_id"`
	Address           string        `json:"address"`
	PeerAddress       string        `json:"peer_address"`
	DataDir           string        `json:"data_dir"`
	SyncInterval      time.Duration `json:"sync_interval"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	FailoverTimeout   time.Duration `json:"failover_timeout"`
	EnableEventStore  bool          `json:"enable_event_store"`
}

// DefaultHAConfig 默认高可用配置
func DefaultHAConfig() *HAConfig {
	return &HAConfig{
		SyncInterval:      30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		FailoverTimeout:   30 * time.Second,
		EnableEventStore:  true,
	}
}

// HAManager 高可用管理器
type HAManager struct {
	mu            sync.RWMutex
	config        *HAConfig
	nodeManager   *NodeManager
	eventStore    *EventStore
	syncManager   *SyncManager
	dbManager     *database.DBManager
	ctx           context.Context
	cancel        context.CancelFunc
	isInitialized bool
	isRunning     bool
	databases     map[database.DBType]*gorm.DB
	originalDBs   map[database.DBType]*gorm.DB // 保存原始数据库连接
}

// NewHAManager 创建高可用管理器
func NewHAManager(config *HAConfig) (*HAManager, error) {
	if config == nil {
		config = DefaultHAConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	ham := &HAManager{
		config:      config,
		ctx:         ctx,
		cancel:      cancel,
		databases:   make(map[database.DBType]*gorm.DB),
		originalDBs: make(map[database.DBType]*gorm.DB),
	}

	return ham, nil
}

// Initialize 初始化高可用管理器
func (ham *HAManager) Initialize() error {
	ham.mu.Lock()
	defer ham.mu.Unlock()

	if ham.isInitialized {
		return fmt.Errorf("HA manager is already initialized")
	}

	log.Info("Initializing HA SQLite manager")

	// 初始化节点管理器
	nodeManager := NewNodeManager(ham.config.NodeID, ham.config.Address)
	nodeManager.heartbeatInterval = ham.config.HeartbeatInterval
	nodeManager.failoverTimeout = ham.config.FailoverTimeout

	// 设置回调函数
	nodeManager.SetRoleChangeCallback(ham.onRoleChange)
	nodeManager.SetNodeFailureCallback(ham.onNodeFailure)
	nodeManager.SetNodeRecoveryCallback(ham.onNodeRecovery)

	ham.nodeManager = nodeManager

	// 初始化事件存储
	if ham.config.EnableEventStore {
		eventStorePath := filepath.Join(ham.config.DataDir, "events.db")
		eventStore, err := NewEventStore(eventStorePath, ham.config.NodeID)
		if err != nil {
			return fmt.Errorf("failed to initialize event store: %v", err)
		}
		ham.eventStore = eventStore
	}

	// 初始化同步管理器
	syncDBPath := filepath.Join(ham.config.DataDir, "sync.db")
	syncManager, err := NewSyncManager(ham.nodeManager, ham.eventStore, syncDBPath)
	if err != nil {
		return fmt.Errorf("failed to initialize sync manager: %v", err)
	}
	syncManager.SetSyncInterval(ham.config.SyncInterval)
	syncManager.SetSyncCompleteCallback(ham.onSyncComplete)
	syncManager.SetSyncErrorCallback(ham.onSyncError)
	ham.syncManager = syncManager

	// 获取现有的数据库管理器
	ham.dbManager = database.Manager
	if ham.dbManager == nil {
		return fmt.Errorf("database manager is not initialized")
	}

	ham.isInitialized = true
	log.Info("HA SQLite manager initialized successfully")

	return nil
}

// Start 启动高可用管理器
func (ham *HAManager) Start() error {
	ham.mu.Lock()
	defer ham.mu.Unlock()

	if !ham.isInitialized {
		return fmt.Errorf("HA manager is not initialized")
	}

	if ham.isRunning {
		return fmt.Errorf("HA manager is already running")
	}

	log.Info("Starting HA SQLite manager")

	// 启动节点管理器
	if err := ham.nodeManager.Start(); err != nil {
		return fmt.Errorf("failed to start node manager: %v", err)
	}

	// 启动同步管理器
	if err := ham.syncManager.Start(); err != nil {
		return fmt.Errorf("failed to start sync manager: %v", err)
	}

	// 设置数据库路径到同步管理器
	ham.setupDatabasePaths()

	ham.isRunning = true
	log.Info("HA SQLite manager started successfully")

	return nil
}

// Stop 停止高可用管理器
func (ham *HAManager) Stop() {
	ham.mu.Lock()
	defer ham.mu.Unlock()

	if !ham.isRunning {
		return
	}

	log.Info("Stopping HA SQLite manager")

	// 停止同步管理器
	if ham.syncManager != nil {
		ham.syncManager.Stop()
	}

	// 停止节点管理器
	if ham.nodeManager != nil {
		ham.nodeManager.Stop()
	}

	// 禁用事件存储
	if ham.eventStore != nil {
		ham.eventStore.Disable()
	}

	ham.isRunning = false
	ham.cancel()

	log.Info("HA SQLite manager stopped")
}

// Close 关闭高可用管理器
func (ham *HAManager) Close() error {
	ham.Stop()

	// 关闭事件存储
	if ham.eventStore != nil {
		if err := ham.eventStore.Close(); err != nil {
			log.Errorf("Failed to close event store: %v", err)
		}
	}

	// 关闭同步管理器
	if ham.syncManager != nil {
		if err := ham.syncManager.Close(); err != nil {
			log.Errorf("Failed to close sync manager: %v", err)
		}
	}

	return nil
}

// GetDB 获取数据库连接（带HA包装）
func (ham *HAManager) GetDB(dbType database.DBType) (*gorm.DB, error) {
	ham.mu.RLock()
	defer ham.mu.RUnlock()

	// 如果有包装的数据库连接，返回包装的连接
	if db, exists := ham.databases[dbType]; exists {
		return db, nil
	}

	// 否则返回原始连接
	return ham.dbManager.GetDB(dbType)
}

// SetPeerNode 设置对等节点
func (ham *HAManager) SetPeerNode(nodeID, address string, role NodeRole) {
	peerNode := &NodeInfo{
		ID:       nodeID,
		Address:  address,
		Role:     role,
		Status:   Healthy,
		LastSeen: time.Now(),
	}

	ham.nodeManager.SetPeerNode(peerNode)
}

// GetNodeRole 获取当前节点角色
func (ham *HAManager) GetNodeRole() NodeRole {
	return ham.nodeManager.GetCurrentRole()
}

// GetNodeStatus 获取当前节点状态
func (ham *HAManager) GetNodeStatus() NodeStatus {
	return ham.nodeManager.GetCurrentStatus()
}

// PromoteToPrimary 提升为主节点
func (ham *HAManager) PromoteToPrimary() error {
	return ham.nodeManager.PromoteToPrimary()
}

// DemoteToSecondary 降级为备节点
func (ham *HAManager) DemoteToSecondary() error {
	return ham.nodeManager.DemoteToSecondary()
}

// SyncNow 立即执行同步
func (ham *HAManager) SyncNow() error {
	return ham.syncManager.SyncNow()
}

// GetSyncHistory 获取同步历史
func (ham *HAManager) GetSyncHistory(limit int) ([]*SyncRecord, error) {
	return ham.syncManager.GetSyncHistory(limit)
}

// GetEventCount 获取事件总数
func (ham *HAManager) GetEventCount() (int64, error) {
	if ham.eventStore == nil {
		return 0, fmt.Errorf("event store is not enabled")
	}
	return ham.eventStore.GetEventCount()
}

// onRoleChange 角色变更回调
func (ham *HAManager) onRoleChange(oldRole, newRole NodeRole) {
	log.Infof("Node role changed from %s to %s", oldRole, newRole)

	if newRole == Primary {
		// 成为主节点时，禁用事件记录，启用数据库写入
		if ham.eventStore != nil {
			ham.eventStore.Disable()
		}
		log.Info("Node promoted to primary - event recording disabled")
	} else if newRole == Secondary {
		// 成为备节点时，启用事件记录
		if ham.eventStore != nil {
			ham.eventStore.Enable()
		}
		log.Info("Node demoted to secondary - event recording enabled")
	}
}

// onNodeFailure 节点故障回调
func (ham *HAManager) onNodeFailure(nodeID string) {
	log.Warnf("Node %s has failed", nodeID)

	// 如果当前节点变成主节点，开始记录变更事件
	if ham.nodeManager.IsPrimary() && ham.eventStore != nil {
		ham.eventStore.Enable()
		log.Info("Started recording changes as new primary node")
	}
}

// onNodeRecovery 节点恢复回调
func (ham *HAManager) onNodeRecovery(nodeID string) {
	log.Infof("Node %s has recovered", nodeID)

	// 如果对等节点恢复且是主节点，需要同步事件
	peerNode := ham.nodeManager.GetPeerNode()
	if peerNode != nil && peerNode.Role == Primary && ham.nodeManager.IsSecondary() {
		log.Info("Primary node recovered, will sync events")
		go ham.syncEventsFromPrimary()
	}
}

// onSyncComplete 同步完成回调
func (ham *HAManager) onSyncComplete(record *SyncRecord) {
	log.Infof("Sync completed for database %s: %d records, checksum: %s",
		record.DatabaseType, record.RecordsCount, record.Checksum)
}

// onSyncError 同步错误回调
func (ham *HAManager) onSyncError(err error) {
	log.Errorf("Sync error: %v", err)
}

// setupDatabasePaths 设置数据库路径
func (ham *HAManager) setupDatabasePaths() {
	// 获取默认数据库配置
	configs := database.GetDefaultDBConfig(ham.config.DataDir)

	for _, config := range configs {
		ham.syncManager.SetDatabasePath(string(config.Name), config.Path)
	}
}

// syncEventsFromPrimary 从主节点同步事件
func (ham *HAManager) syncEventsFromPrimary() {
	if ham.eventStore == nil {
		return
	}

	// 获取未应用的事件
	events, err := ham.eventStore.GetUnappliedEvents(1000)
	if err != nil {
		log.Errorf("Failed to get unapplied events: %v", err)
		return
	}

	if len(events) == 0 {
		return
	}

	log.Infof("Applying %d unapplied events", len(events))

	// 应用事件
	for _, event := range events {
		if err := ham.applyEvent(event); err != nil {
			log.Errorf("Failed to apply event %s: %v", event.EventID, err)
			continue
		}

		// 标记事件为已应用
		if err := ham.eventStore.MarkEventApplied(event.EventID); err != nil {
			log.Errorf("Failed to mark event %s as applied: %v", event.EventID, err)
		}
	}
}

// applyEvent 应用事件
func (ham *HAManager) applyEvent(event *DatabaseEvent) error {
	// 根据事件类型应用到数据库
	// 这里需要根据具体的事件类型和数据执行相应的数据库操作
	log.Debugf("Applying event %s: %s on table %s",
		event.EventID, event.EventType, event.TableName)

	// TODO: 实现具体的事件应用逻辑
	// 这里应该根据事件的SQL或数据内容执行相应的数据库操作

	return nil
}

// WrapDatabaseWithHA 用HA功能包装数据库连接
func (ham *HAManager) WrapDatabaseWithHA(dbType database.DBType, db *gorm.DB) *gorm.DB {
	ham.mu.Lock()
	defer ham.mu.Unlock()

	// 保存原始连接
	ham.originalDBs[dbType] = db

	// 创建包装的连接（这里可以添加拦截器来记录事件）
	wrappedDB := db.Session(&gorm.Session{})

	// 添加回调来记录数据库操作
	if ham.eventStore != nil {
		ham.addDatabaseCallbacks(wrappedDB, dbType)
	}

	ham.databases[dbType] = wrappedDB
	return wrappedDB
}

// addDatabaseCallbacks 添加数据库回调
func (ham *HAManager) addDatabaseCallbacks(db *gorm.DB, dbType database.DBType) {
	// 添加创建回调
	db.Callback().Create().After("gorm:create").Register("ha:after_create", func(db *gorm.DB) {
		if ham.eventStore != nil && ham.eventStore.IsEnabled() {
			ham.recordCreateEvent(db, dbType)
		}
	})

	// 添加更新回调
	db.Callback().Update().After("gorm:update").Register("ha:after_update", func(db *gorm.DB) {
		if ham.eventStore != nil && ham.eventStore.IsEnabled() {
			ham.recordUpdateEvent(db, dbType)
		}
	})

	// 添加删除回调
	db.Callback().Delete().After("gorm:delete").Register("ha:after_delete", func(db *gorm.DB) {
		if ham.eventStore != nil && ham.eventStore.IsEnabled() {
			ham.recordDeleteEvent(db, dbType)
		}
	})
}

// recordCreateEvent 记录创建事件
func (ham *HAManager) recordCreateEvent(db *gorm.DB, dbType database.DBType) {
	if db.Error != nil {
		return
	}

	tableName := db.Statement.Table
	if tableName == "" {
		return
	}

	// 简化的事件记录
	event := &DatabaseEvent{
		EventType: EventInsert,
		TableName: tableName,
		SQL:       db.Statement.SQL.String(),
	}

	if err := ham.eventStore.RecordEvent(event); err != nil {
		log.Errorf("Failed to record create event: %v", err)
	}
}

// recordUpdateEvent 记录更新事件
func (ham *HAManager) recordUpdateEvent(db *gorm.DB, dbType database.DBType) {
	if db.Error != nil {
		return
	}

	tableName := db.Statement.Table
	if tableName == "" {
		return
	}

	// 简化的事件记录
	event := &DatabaseEvent{
		EventType: EventUpdate,
		TableName: tableName,
		SQL:       db.Statement.SQL.String(),
	}

	if err := ham.eventStore.RecordEvent(event); err != nil {
		log.Errorf("Failed to record update event: %v", err)
	}
}

// recordDeleteEvent 记录删除事件
func (ham *HAManager) recordDeleteEvent(db *gorm.DB, dbType database.DBType) {
	if db.Error != nil {
		return
	}

	tableName := db.Statement.Table
	if tableName == "" {
		return
	}

	// 简化的事件记录
	event := &DatabaseEvent{
		EventType: EventDelete,
		TableName: tableName,
		SQL:       db.Statement.SQL.String(),
	}

	if err := ham.eventStore.RecordEvent(event); err != nil {
		log.Errorf("Failed to record delete event: %v", err)
	}
}
