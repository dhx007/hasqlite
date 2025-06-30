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
	EventStore    *EventStore
	SyncManager   *SyncManager
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
		ham.EventStore = eventStore
	}

	// 初始化同步管理器
	syncDBPath := filepath.Join(ham.config.DataDir, "sync.db")
	syncManager, err := NewSyncManager(ham.nodeManager, ham.EventStore, syncDBPath)
	if err != nil {
		return fmt.Errorf("failed to initialize sync manager: %v", err)
	}
	syncManager.SetSyncInterval(ham.config.SyncInterval)
	syncManager.SetSyncCompleteCallback(ham.onSyncComplete)
	syncManager.SetSyncErrorCallback(ham.onSyncError)
	ham.SyncManager = syncManager

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
	if err := ham.SyncManager.Start(); err != nil {
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
	if ham.SyncManager != nil {
		ham.SyncManager.Stop()
	}

	// 停止节点管理器
	if ham.nodeManager != nil {
		ham.nodeManager.Stop()
	}

	// 禁用事件存储
	if ham.EventStore != nil {
		ham.EventStore.Disable()
	}

	ham.isRunning = false
	ham.cancel()

	log.Info("HA SQLite manager stopped")
}

// Close 关闭高可用管理器
func (ham *HAManager) Close() error {
	ham.Stop()

	// 关闭事件存储
	if ham.EventStore != nil {
		if err := ham.EventStore.Close(); err != nil {
			log.Errorf("Failed to close event store: %v", err)
		}
	}

	// 关闭同步管理器
	if ham.SyncManager != nil {
		if err := ham.SyncManager.Close(); err != nil {
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
	return ham.SyncManager.SyncNow()
}

// GetSyncHistory 获取同步历史
func (ham *HAManager) GetSyncHistory(limit int) ([]*SyncRecord, error) {
	return ham.SyncManager.GetSyncHistory(limit)
}

// GetEventCount 获取事件总数
func (ham *HAManager) GetEventCount() (int64, error) {
	if ham.EventStore == nil {
		return 0, fmt.Errorf("event store is not enabled")
	}
	return ham.EventStore.GetEventCount()
}

// onRoleChange 角色变更回调
func (ham *HAManager) onRoleChange(oldRole, newRole NodeRole) {
	log.Infof("Node role changed from %s to %s", oldRole, newRole)

	if newRole == Primary {
		// 成为主节点时，禁用事件记录，启用数据库写入
		if ham.EventStore != nil {
			ham.EventStore.Disable()
		}
		log.Info("Node promoted to primary - event recording disabled")
	} else if newRole == Secondary {
		// 成为备节点时，启用事件记录
		if ham.EventStore != nil {
			ham.EventStore.Enable()
		}
		log.Info("Node demoted to secondary - event recording enabled")
	}
}

// onNodeFailure 节点故障回调
func (ham *HAManager) onNodeFailure(nodeID string) {
	log.Warnf("Node %s has failed", nodeID)

	// 如果当前节点变成主节点，开始记录变更事件
	if ham.nodeManager.IsPrimary() && ham.EventStore != nil {
		ham.EventStore.Enable()
		log.Info("Started recording changes as new primary node")
	}
}

// onNodeRecovery 节点恢复回调
func (ham *HAManager) onNodeRecovery(nodeID string) {
	log.Infof("Node %s has recovered", nodeID)

	// 获取恢复的节点信息
	peerNode := ham.nodeManager.GetPeerNode()
	if peerNode == nil {
		return
	}

	// 如果恢复的是原主节点，且当前节点是主节点（原备节点）
	if peerNode.ID == nodeID && peerNode.Role == Primary && ham.nodeManager.IsPrimary() {
		log.Info("Original primary node recovered, starting event replay and role restoration")
		
		// 1. 从当前主节点（原备节点）获取事件并发送给恢复的原主节点
		go ham.replayEventsToRecoveredPrimary(nodeID)
		
		// 2. 等待事件重播完成后，恢复原有角色
		go ham.restoreOriginalRoles(nodeID)
	}
	
	// 如果当前节点是原主节点刚恢复上线
	if ham.nodeManager.GetNodeInfo().ID == nodeID && ham.nodeManager.IsSecondary() {
		log.Info("Current node (original primary) recovered, requesting event replay")
		
		// 请求从当前主节点获取事件进行重播
		go ham.requestEventReplayFromCurrentPrimary()
	}
}

// replayEventsToRecoveredPrimary 向恢复的原主节点重播事件
func (ham *HAManager) replayEventsToRecoveredPrimary(recoveredNodeID string) {
	if ham.EventStore == nil {
		return
	}

	log.Info("Starting event replay to recovered primary node")

	// 获取故障期间的所有事件
	events, err := ham.EventStore.GetUnappliedEvents(0) // 获取所有未应用事件
	if err != nil {
		log.Errorf("Failed to get events for replay: %v", err)
		return
	}

	if len(events) == 0 {
		log.Info("No events to replay")
		return
	}

	log.Infof("Sending %d events to recovered primary node %s", len(events), recoveredNodeID)

	// TODO: 通过API发送事件到恢复的原主节点
	// 这里需要调用API接口将事件发送给恢复的节点
	if err := ham.sendEventsToNode(recoveredNodeID, events); err != nil {
		log.Errorf("Failed to send events to recovered node: %v", err)
		return
	}

	log.Info("Event replay to recovered primary completed")
}

// requestEventReplayFromCurrentPrimary 从当前主节点请求事件重播
func (ham *HAManager) requestEventReplayFromCurrentPrimary() {
	peerNode := ham.nodeManager.GetPeerNode()
	if peerNode == nil || peerNode.Role != Primary {
		log.Warn("No primary peer node found for event replay")
		return
	}

	log.Infof("Requesting event replay from current primary node %s", peerNode.ID)

	// TODO: 通过API从当前主节点获取事件
	// 这里需要调用API接口从当前主节点获取事件
	events, err := ham.requestEventsFromNode(peerNode.ID)
	if err != nil {
		log.Errorf("Failed to request events from primary: %v", err)
		return
	}

	if len(events) == 0 {
		log.Info("No events received for replay")
		return
	}

	log.Infof("Received %d events from primary, starting replay", len(events))

	// 应用接收到的事件
	successCount := 0
	for _, event := range events {
		if err := ham.applyEvent(event); err != nil {
			log.Errorf("Failed to apply event %s: %v", event.EventID, err)
			continue
		}
		successCount++
	}

	log.Infof("Event replay completed: %d/%d events applied successfully", successCount, len(events))

	// 事件重播完成后，恢复为主节点角色
	if err := ham.nodeManager.PromoteToPrimary(); err != nil {
		log.Errorf("Failed to promote to primary after event replay: %v", err)
	} else {
		log.Info("Successfully restored to primary role after event replay")
	}
}

// restoreOriginalRoles 恢复原有角色
func (ham *HAManager) restoreOriginalRoles(recoveredNodeID string) {
	// 等待一段时间确保事件重播完成
	time.Sleep(5 * time.Second)

	log.Info("Restoring original node roles")

	// 将当前节点（原备节点）降级为备节点
	if err := ham.nodeManager.DemoteToSecondary(); err != nil {
		log.Errorf("Failed to demote to secondary: %v", err)
		return
	}

	log.Info("Successfully restored original roles - current node demoted to secondary")
}

// sendEventsToNode 发送事件到指定节点
func (ham *HAManager) sendEventsToNode(nodeID string, events []*DatabaseEvent) error {
	// TODO: 实现通过API发送事件到指定节点的逻辑
	// 这里需要调用对应节点的API接口
	log.Infof("Sending %d events to node %s (implementation needed)", len(events), nodeID)
	return nil
}

// requestEventsFromNode 从指定节点请求事件
func (ham *HAManager) requestEventsFromNode(nodeID string) ([]*DatabaseEvent, error) {
	// TODO: 实现从指定节点请求事件的逻辑
	// 这里需要调用对应节点的API接口
	log.Infof("Requesting events from node %s (implementation needed)", nodeID)
	return nil, nil
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
		ham.SyncManager.SetDatabasePath(string(config.Name), config.Path)
	}
}

// syncEventsFromPrimary 从主节点同步事件
func (ham *HAManager) syncEventsFromPrimary() {
	if ham.EventStore == nil {
		return
	}

	// 获取未应用的事件
	events, err := ham.EventStore.GetUnappliedEvents(1000)
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
		if err := ham.EventStore.MarkEventApplied(event.EventID); err != nil {
			log.Errorf("Failed to mark event %s as applied: %v", event.EventID, err)
		}
	}
}

// applyEvent 应用事件
func (ham *HAManager) applyEvent(event *DatabaseEvent) error {
	// 根据事件类型应用到数据库
	log.Debugf("Applying event %s: %s on table %s",
		event.EventID, event.EventType, event.TableName)

	// 获取对应的数据库连接
	var db *gorm.DB
	var err error
	
	// 根据表名推断数据库类型（简化实现）
	dbType := ham.inferDBTypeFromTable(event.TableName)
	db, err = ham.GetDB(dbType)
	if err != nil {
		return fmt.Errorf("failed to get database for table %s: %v", event.TableName, err)
	}

	// 根据事件类型执行相应操作
	switch event.EventType {
	case EventInsert:
		return ham.applyInsertEvent(db, event)
	case EventUpdate:
		return ham.applyUpdateEvent(db, event)
	case EventDelete:
		return ham.applyDeleteEvent(db, event)
	case EventDDL:
		return ham.applyDDLEvent(db, event)
	default:
		return fmt.Errorf("unknown event type: %s", event.EventType)
	}
}

// inferDBTypeFromTable 从表名推断数据库类型
func (ham *HAManager) inferDBTypeFromTable(tableName string) database.DBType {
	// 这里可以根据表名前缀或配置来推断数据库类型
	// 简化实现，默认返回OamsDB
	return database.OamsDB
}

// applyInsertEvent 应用插入事件
func (ham *HAManager) applyInsertEvent(db *gorm.DB, event *DatabaseEvent) error {
	if event.SQL != "" {
		// 如果有SQL语句，直接执行
		return db.Exec(event.SQL).Error
	}
	
	if event.NewData != "" {
		// 如果有新数据，解析并插入
		// 这里需要根据具体的数据结构来实现
		log.Debugf("Applying insert with data: %s", event.NewData)
		// TODO: 实现具体的数据插入逻辑
	}
	
	return nil
}

// applyUpdateEvent 应用更新事件
func (ham *HAManager) applyUpdateEvent(db *gorm.DB, event *DatabaseEvent) error {
	if event.SQL != "" {
		// 如果有SQL语句，直接执行
		return db.Exec(event.SQL).Error
	}
	
	if event.NewData != "" && event.PrimaryKey != "" {
		// 如果有新数据和主键，执行更新
		log.Debugf("Applying update for key %s with data: %s", event.PrimaryKey, event.NewData)
		// TODO: 实现具体的数据更新逻辑
	}
	
	return nil
}

// applyDeleteEvent 应用删除事件
func (ham *HAManager) applyDeleteEvent(db *gorm.DB, event *DatabaseEvent) error {
	if event.SQL != "" {
		// 如果有SQL语句，直接执行
		return db.Exec(event.SQL).Error
	}
	
	if event.PrimaryKey != "" {
		// 如果有主键，执行删除
		log.Debugf("Applying delete for key %s", event.PrimaryKey)
		// TODO: 实现具体的数据删除逻辑
	}
	
	return nil
}

// applyDDLEvent 应用DDL事件
func (ham *HAManager) applyDDLEvent(db *gorm.DB, event *DatabaseEvent) error {
	if event.SQL != "" {
		// DDL语句直接执行
		return db.Exec(event.SQL).Error
	}
	
	return nil
}

// ReplayEventsFromTimestamp 从指定时间戳重播事件
func (ham *HAManager) ReplayEventsFromTimestamp(timestamp time.Time) error {
	if ham.EventStore == nil {
		return fmt.Errorf("event store is not enabled")
	}

	log.Infof("Starting event replay from timestamp: %v", timestamp)

	// 获取指定时间后的所有事件
	events, err := ham.EventStore.GetEventsAfter(timestamp, 0)
	if err != nil {
		return fmt.Errorf("failed to get events after timestamp: %v", err)
	}

	log.Infof("Found %d events to replay", len(events))

	// 按时间顺序应用事件
	successCount := 0
	for _, event := range events {
		if err := ham.applyEvent(event); err != nil {
			log.Errorf("Failed to apply event %s: %v", event.EventID, err)
			continue
		}

		// 标记事件为已应用
		if err := ham.EventStore.MarkEventApplied(event.EventID); err != nil {
			log.Errorf("Failed to mark event %s as applied: %v", event.EventID, err)
		}

		successCount++
	}

	log.Infof("Event replay completed: %d/%d events applied successfully", successCount, len(events))
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
	if ham.EventStore != nil {
		ham.addDatabaseCallbacks(wrappedDB, dbType)
	}

	ham.databases[dbType] = wrappedDB
	return wrappedDB
}

// addDatabaseCallbacks 添加数据库回调
func (ham *HAManager) addDatabaseCallbacks(db *gorm.DB, dbType database.DBType) {
	// 添加创建回调
	db.Callback().Create().After("gorm:create").Register("ha:after_create", func(db *gorm.DB) {
		if ham.EventStore != nil && ham.EventStore.IsEnabled() {
			ham.recordCreateEvent(db, dbType)
		}
	})

	// 添加更新回调
	db.Callback().Update().After("gorm:update").Register("ha:after_update", func(db *gorm.DB) {
		if ham.EventStore != nil && ham.EventStore.IsEnabled() {
			ham.recordUpdateEvent(db, dbType)
		}
	})

	// 添加删除回调
	db.Callback().Delete().After("gorm:delete").Register("ha:after_delete", func(db *gorm.DB) {
		if ham.EventStore != nil && ham.EventStore.IsEnabled() {
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

	if err := ham.EventStore.RecordEvent(event); err != nil {
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

	if err := ham.EventStore.RecordEvent(event); err != nil {
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

	if err := ham.EventStore.RecordEvent(event); err != nil {
		log.Errorf("Failed to record delete event: %v", err)
	}
}

// 添加重试配置
type RetryConfig struct {
	MaxRetries    int           `json:"max_retries"`
	RetryInterval time.Duration `json:"retry_interval"`
	BackoffFactor float64       `json:"backoff_factor"`
}

// 在HAConfig中添加重试配置
type HAConfig struct {
	NodeID            string        `json:"node_id"`
	Address           string        `json:"address"`
	PeerAddress       string        `json:"peer_address"`
	DataDir           string        `json:"data_dir"`
	SyncInterval      time.Duration `json:"sync_interval"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	FailoverTimeout   time.Duration `json:"failover_timeout"`
	EnableEventStore  bool          `json:"enable_event_store"`
	RetryConfig   *RetryConfig  `json:"retry_config"`
	TimeoutConfig *TimeoutConfig `json:"timeout_config"`
}

type TimeoutConfig struct {
	SyncTimeout     time.Duration `json:"sync_timeout"`
	EventTimeout    time.Duration `json:"event_timeout"`
	NetworkTimeout  time.Duration `json:"network_timeout"`
}

// 添加重试机制
func (ham *HAManager) retryOperation(operation func() error, config *RetryConfig) error {
	var lastErr error
	interval := config.RetryInterval
	
	for i := 0; i <= config.MaxRetries; i++ {
		if err := operation(); err != nil {
			lastErr = err
			if i < config.MaxRetries {
				log.Warnf("Operation failed (attempt %d/%d): %v, retrying in %v", 
					i+1, config.MaxRetries+1, err, interval)
				time.Sleep(interval)
				interval = time.Duration(float64(interval) * config.BackoffFactor)
			}
		} else {
			return nil
		}
	}
	return fmt.Errorf("operation failed after %d retries: %v", config.MaxRetries, lastErr)
}
