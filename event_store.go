package hasqlite

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/glebarez/sqlite"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// EventType 事件类型
type EventType string

const (
	EventInsert EventType = "INSERT"
	EventUpdate EventType = "UPDATE"
	EventDelete EventType = "DELETE"
	EventDDL    EventType = "DDL"
)

// DatabaseEvent 数据库事件
type DatabaseEvent struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	EventID     string    `gorm:"uniqueIndex;size:64" json:"event_id"`
	EventType   EventType `gorm:"size:20;not null" json:"event_type"`
	TableName   string    `gorm:"size:100;not null" json:"table_name"`
	PrimaryKey  string    `gorm:"size:255" json:"primary_key"`
	OldData     string    `gorm:"type:text" json:"old_data,omitempty"`
	NewData     string    `gorm:"type:text" json:"new_data,omitempty"`
	SQL         string    `gorm:"type:text" json:"sql"`
	NodeID      string    `gorm:"size:64;not null" json:"node_id"`
	Timestamp   time.Time `gorm:"not null;index" json:"timestamp"`
	Applied     bool      `gorm:"default:false;index" json:"applied"`
	Checksum    string    `gorm:"size:64" json:"checksum"`
}

// EventStore 事件存储
type EventStore struct {
	mu       sync.RWMutex
	db       *gorm.DB
	nodeID   string
	enabled  bool
	batchSize int
	buffer   []*DatabaseEvent
	bufferMu sync.Mutex
}

// NewEventStore 创建事件存储
func NewEventStore(dbPath, nodeID string) (*EventStore, error) {
	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open event store database: %v", err)
	}

	// 启用WAL模式以提高并发性能
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA synchronous=NORMAL")
	db.Exec("PRAGMA cache_size=10000")
	db.Exec("PRAGMA temp_store=memory")

	// 自动迁移表结构
	if err := db.AutoMigrate(&DatabaseEvent{}); err != nil {
		return nil, fmt.Errorf("failed to migrate event store schema: %v", err)
	}

	es := &EventStore{
		db:        db,
		nodeID:    nodeID,
		enabled:   false,
		batchSize: 100,
		buffer:    make([]*DatabaseEvent, 0, 100),
	}

	log.Infof("Event store initialized for node %s", nodeID)
	return es, nil
}

// Enable 启用事件记录
func (es *EventStore) Enable() {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.enabled = true
	log.Info("Event store enabled")
}

// Disable 禁用事件记录
func (es *EventStore) Disable() {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.enabled = false
	log.Info("Event store disabled")
}

// IsEnabled 检查是否启用事件记录
func (es *EventStore) IsEnabled() bool {
	es.mu.RLock()
	defer es.mu.RUnlock()
	return es.enabled
}

// RecordEvent 记录事件
func (es *EventStore) RecordEvent(event *DatabaseEvent) error {
	if !es.IsEnabled() {
		return nil
	}

	// 设置节点ID和时间戳
	event.NodeID = es.nodeID
	event.Timestamp = time.Now()
	event.EventID = es.generateEventID(event)
	event.Checksum = es.calculateChecksum(event)

	// 添加到缓冲区
	es.bufferMu.Lock()
	es.buffer = append(es.buffer, event)
	bufferLen := len(es.buffer)
	es.bufferMu.Unlock()

	// 如果缓冲区满了，批量写入
	if bufferLen >= es.batchSize {
		return es.flushBuffer()
	}

	return nil
}

// RecordInsert 记录插入事件
func (es *EventStore) RecordInsert(tableName, primaryKey string, newData interface{}, sql string) error {
	newDataJSON, err := json.Marshal(newData)
	if err != nil {
		return fmt.Errorf("failed to marshal new data: %v", err)
	}

	event := &DatabaseEvent{
		EventType:  EventInsert,
		TableName:  tableName,
		PrimaryKey: primaryKey,
		NewData:    string(newDataJSON),
		SQL:        sql,
	}

	return es.RecordEvent(event)
}

// RecordUpdate 记录更新事件
func (es *EventStore) RecordUpdate(tableName, primaryKey string, oldData, newData interface{}, sql string) error {
	oldDataJSON, err := json.Marshal(oldData)
	if err != nil {
		return fmt.Errorf("failed to marshal old data: %v", err)
	}

	newDataJSON, err := json.Marshal(newData)
	if err != nil {
		return fmt.Errorf("failed to marshal new data: %v", err)
	}

	event := &DatabaseEvent{
		EventType:  EventUpdate,
		TableName:  tableName,
		PrimaryKey: primaryKey,
		OldData:    string(oldDataJSON),
		NewData:    string(newDataJSON),
		SQL:        sql,
	}

	return es.RecordEvent(event)
}

// RecordDelete 记录删除事件
func (es *EventStore) RecordDelete(tableName, primaryKey string, oldData interface{}, sql string) error {
	oldDataJSON, err := json.Marshal(oldData)
	if err != nil {
		return fmt.Errorf("failed to marshal old data: %v", err)
	}

	event := &DatabaseEvent{
		EventType:  EventDelete,
		TableName:  tableName,
		PrimaryKey: primaryKey,
		OldData:    string(oldDataJSON),
		SQL:        sql,
	}

	return es.RecordEvent(event)
}

// GetEventsAfter 获取指定时间后的事件
func (es *EventStore) GetEventsAfter(timestamp time.Time, limit int) ([]*DatabaseEvent, error) {
	var events []*DatabaseEvent
	query := es.db.Where("timestamp > ? AND node_id = ?", timestamp, es.nodeID).Order("timestamp ASC")
	
	if limit > 0 {
		query = query.Limit(limit)
	}
	
	if err := query.Find(&events).Error; err != nil {
		return nil, fmt.Errorf("failed to query events: %v", err)
	}
	
	return events, nil
}

// GetEventsBetween 获取指定时间范围内的事件
func (es *EventStore) GetEventsBetween(startTime, endTime time.Time) ([]*DatabaseEvent, error) {
	var events []*DatabaseEvent
	if err := es.db.Where("timestamp BETWEEN ? AND ? AND node_id = ?", startTime, endTime, es.nodeID).
		Order("timestamp ASC").Find(&events).Error; err != nil {
		return nil, fmt.Errorf("failed to query events between %v and %v: %v", startTime, endTime, err)
	}
	
	return events, nil
}

// GetUnappliedEvents 获取未应用的事件
func (es *EventStore) GetUnappliedEvents(limit int) ([]*DatabaseEvent, error) {
	var events []*DatabaseEvent
	query := es.db.Where("applied = ? AND node_id = ?", false, es.nodeID).Order("timestamp ASC")
	
	if limit > 0 {
		query = query.Limit(limit)
	}
	
	if err := query.Find(&events).Error; err != nil {
		return nil, fmt.Errorf("failed to query unapplied events: %v", err)
	}
	
	return events, nil
}

// MarkEventApplied 标记事件为已应用
func (es *EventStore) MarkEventApplied(eventID string) error {
	if err := es.db.Model(&DatabaseEvent{}).Where("event_id = ?", eventID).
		Update("applied", true).Error; err != nil {
		return fmt.Errorf("failed to mark event %s as applied: %v", eventID, err)
	}
	
	return nil
}

// MarkEventsApplied 批量标记事件为已应用
func (es *EventStore) MarkEventsApplied(eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	
	if err := es.db.Model(&DatabaseEvent{}).Where("event_id IN ?", eventIDs).
		Update("applied", true).Error; err != nil {
		return fmt.Errorf("failed to mark events as applied: %v", err)
	}
	
	return nil
}

// CleanupOldEvents 清理旧事件
func (es *EventStore) CleanupOldEvents(olderThan time.Time) error {
	result := es.db.Where("timestamp < ? AND applied = ?", olderThan, true).Delete(&DatabaseEvent{})
	if result.Error != nil {
		return fmt.Errorf("failed to cleanup old events: %v", result.Error)
	}
	
	log.Infof("Cleaned up %d old events", result.RowsAffected)
	return nil
}

// GetEventCount 获取事件总数
func (es *EventStore) GetEventCount() (int64, error) {
	var count int64
	if err := es.db.Model(&DatabaseEvent{}).Where("node_id = ?", es.nodeID).Count(&count).Error; err != nil {
		return 0, fmt.Errorf("failed to count events: %v", err)
	}
	return count, nil
}

// FlushBuffer 刷新缓冲区
func (es *EventStore) flushBuffer() error {
	es.bufferMu.Lock()
	defer es.bufferMu.Unlock()
	
	if len(es.buffer) == 0 {
		return nil
	}
	
	// 批量插入事件
	if err := es.db.CreateInBatches(es.buffer, es.batchSize).Error; err != nil {
		return fmt.Errorf("failed to flush event buffer: %v", err)
	}
	
	log.Debugf("Flushed %d events to store", len(es.buffer))
	
	// 清空缓冲区
	es.buffer = es.buffer[:0]
	return nil
}

// Flush 强制刷新缓冲区
func (es *EventStore) Flush() error {
	return es.flushBuffer()
}

// Close 关闭事件存储
func (es *EventStore) Close() error {
	// 刷新缓冲区
	if err := es.flushBuffer(); err != nil {
		log.Errorf("Failed to flush buffer on close: %v", err)
	}
	
	// 关闭数据库连接
	sqlDB, err := es.db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying DB: %v", err)
	}
	
	return sqlDB.Close()
}

// generateEventID 生成事件ID
func (es *EventStore) generateEventID(event *DatabaseEvent) string {
	return fmt.Sprintf("%s_%s_%d", es.nodeID, event.EventType, time.Now().UnixNano())
}

// calculateChecksum 计算事件校验和
func (es *EventStore) calculateChecksum(event *DatabaseEvent) string {
	// 简单的校验和计算，实际应用中可以使用更复杂的算法
	data := fmt.Sprintf("%s_%s_%s_%s_%s", 
		event.EventType, event.TableName, event.PrimaryKey, event.OldData, event.NewData)
	return fmt.Sprintf("%x", len(data)) // 简化的校验和
}