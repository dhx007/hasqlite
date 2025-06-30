package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"ws-oams/internal/database"
	"ws-oams/internal/hasqlite"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// TestRecord 测试记录结构
type TestRecord struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Name      string    `gorm:"size:100;not null" json:"name"`
	Value     int       `gorm:"not null" json:"value"`
	NodeID    string    `gorm:"size:64" json:"node_id"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

// HANode 高可用节点
type HANode struct {
	ID         string
	Address    string
	Port       int
	DataDir    string
	HAManager  *hasqlite.HAManager
	APIServer  *hasqlite.APIServer
	DB         *gorm.DB
	Ctx        context.Context
	Cancel     context.CancelFunc
	IsRunning  bool
	Mu         sync.RWMutex
}

// NewHANode 创建新的HA节点
func NewHANode(nodeID, address string, port int, dataDir string) *HANode {
	ctx, cancel := context.WithCancel(context.Background())
	return &HANode{
		ID:      nodeID,
		Address: address,
		Port:    port,
		DataDir: dataDir,
		Ctx:     ctx,
		Cancel:  cancel,
	}
}

// Start 启动节点
func (node *HANode) Start(peerAddress string, initialRole hasqlite.NodeRole) error {
	node.Mu.Lock()
	defer node.Mu.Unlock()

	if node.IsRunning {
		return fmt.Errorf("node %s is already running", node.ID)
	}

	log.Infof("Starting HA node %s at %s:%d", node.ID, node.Address, node.Port)

	// 创建数据目录
	if err := os.MkdirAll(node.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}

	// 创建HA配置
	config := &hasqlite.HAConfig{
		NodeID:            node.ID,
		Address:           fmt.Sprintf("%s:%d", node.Address, node.Port),
		PeerAddress:       peerAddress,
		DataDir:           node.DataDir,
		SyncInterval:      10 * time.Second, // 10秒同步间隔
		HeartbeatInterval: 3 * time.Second,  // 3秒心跳间隔
		FailoverTimeout:   10 * time.Second, // 10秒故障转移超时
		EnableEventStore:  true,
	}

	// 创建并初始化HA管理器
	haManager, err := hasqlite.NewHAManager(config)
	if err != nil {
		return fmt.Errorf("failed to create HA manager: %v", err)
	}

	if err := haManager.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize HA manager: %v", err)
	}

	// 设置初始角色
	if initialRole == hasqlite.Primary {
		if err := haManager.PromoteToPrimary(); err != nil {
			log.Warnf("Failed to promote to primary: %v", err)
		}
	}

	// 启动HA管理器
	if err := haManager.Start(); err != nil {
		return fmt.Errorf("failed to start HA manager: %v", err)
	}

	// 创建并启动API服务器
	apiServer := hasqlite.NewAPIServer(haManager, node.Port)
	if err := apiServer.Start(); err != nil {
		return fmt.Errorf("failed to start API server: %v", err)
	}

	// 创建测试数据库
	dbPath := filepath.Join(node.DataDir, "test.db")
	testDB, err := node.createTestDatabase(dbPath)
	if err != nil {
		return fmt.Errorf("failed to create test database: %v", err)
	}

	// 用HA功能包装数据库
	haDB := haManager.WrapDatabaseWithHA(database.OamsDB, testDB)

	node.HAManager = haManager
	node.APIServer = apiServer
	node.DB = haDB
	node.IsRunning = true

	log.Infof("HA node %s started successfully as %s", node.ID, initialRole)
	return nil
}

// Stop 停止节点
func (node *HANode) Stop() error {
	node.Mu.Lock()
	defer node.Mu.Unlock()

	if !node.IsRunning {
		return nil
	}

	log.Infof("Stopping HA node %s", node.ID)

	// 停止API服务器
	if node.APIServer != nil {
		if err := node.APIServer.Stop(); err != nil {
			log.Errorf("Failed to stop API server: %v", err)
		}
	}

	// 停止HA管理器
	if node.HAManager != nil {
		node.HAManager.Stop()
		if err := node.HAManager.Close(); err != nil {
			log.Errorf("Failed to close HA manager: %v", err)
		}
	}

	node.Cancel()
	node.IsRunning = false

	log.Infof("HA node %s stopped", node.ID)
	return nil
}

// createTestDatabase 创建测试数据库
func (node *HANode) createTestDatabase(dbPath string) (*gorm.DB, error) {
	// 这里简化实现，实际应该使用database包的方法
	// 为了演示，我们直接创建一个SQLite连接
	db, err := database.NewSQLiteConnection(dbPath)
	if err != nil {
		return nil, err
	}

	// 自动迁移测试表
	if err := db.AutoMigrate(&TestRecord{}); err != nil {
		return nil, fmt.Errorf("failed to migrate test table: %v", err)
	}

	return db, nil
}

// GetRole 获取节点角色
func (node *HANode) GetRole() hasqlite.NodeRole {
	node.Mu.RLock()
	defer node.Mu.RUnlock()

	if node.HAManager == nil {
		return hasqlite.Unknown
	}
	return node.HAManager.GetNodeRole()
}

// GetStatus 获取节点状态
func (node *HANode) GetStatus() hasqlite.NodeStatus {
	node.Mu.RLock()
	defer node.Mu.RUnlock()

	if node.HAManager == nil {
		return hasqlite.Disconnected
	}
	return node.HAManager.GetNodeStatus()
}

// PerformDatabaseOperations 执行数据库操作
func (node *HANode) PerformDatabaseOperations(operationCount int) error {
	node.Mu.RLock()
	db := node.DB
	node.Mu.RUnlock()

	if db == nil {
		return fmt.Errorf("database not available")
	}

	log.Infof("Node %s performing %d database operations", node.ID, operationCount)

	for i := 0; i < operationCount; i++ {
		// 插入记录
		record := &TestRecord{
			Name:   fmt.Sprintf("record_%s_%d", node.ID, i),
			Value:  i * 10,
			NodeID: node.ID,
		}

		if err := db.Create(record).Error; err != nil {
			log.Errorf("Failed to create record: %v", err)
			continue
		}

		log.Debugf("Node %s created record ID %d: %s", node.ID, record.ID, record.Name)

		// 更新记录
		record.Value = record.Value + 1
		if err := db.Save(record).Error; err != nil {
			log.Errorf("Failed to update record: %v", err)
		}

		// 每5个操作休息一下
		if i%5 == 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	return nil
}

// GetRecordCount 获取记录数量
func (node *HANode) GetRecordCount() (int64, error) {
	node.Mu.RLock()
	db := node.DB
	node.Mu.RUnlock()

	if db == nil {
		return 0, fmt.Errorf("database not available")
	}

	var count int64
	if err := db.Model(&TestRecord{}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

// HASimulation 高可用模拟器
type HASimulation struct {
	Node1 *HANode
	Node2 *HANode
	Ctx   context.Context
	Cancel context.CancelFunc
}

// NewHASimulation 创建新的HA模拟器
func NewHASimulation() *HASimulation {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建临时目录
	tmpDir := "/tmp/ha-sqlite-simulation"
	os.RemoveAll(tmpDir) // 清理之前的数据

	node1 := NewHANode("node-1", "localhost", 8080, filepath.Join(tmpDir, "node1"))
	node2 := NewHANode("node-2", "localhost", 8081, filepath.Join(tmpDir, "node2"))

	return &HASimulation{
		Node1:  node1,
		Node2:  node2,
		Ctx:    ctx,
		Cancel: cancel,
	}
}

// Run 运行模拟
func (sim *HASimulation) Run() error {
	log.Info("=== 开始高可用SQLite系统模拟 ===")

	// 设置日志级别
	log.SetLevel(log.InfoLevel)

	// 1. 启动两个节点
	log.Info("\n=== 阶段1: 启动两个节点 ===")
	if err := sim.startNodes(); err != nil {
		return err
	}

	// 2. 主节点执行数据库操作（5分钟）
	log.Info("\n=== 阶段2: 主节点执行数据库操作（5分钟）===")
	if err := sim.primaryNodeOperations(); err != nil {
		return err
	}

	// 3. 主节点下线（模拟故障）
	log.Info("\n=== 阶段3: 主节点下线（模拟故障）===")
	if err := sim.simulatePrimaryFailure(); err != nil {
		return err
	}

	// 4. 备节点成为主节点并执行操作（3分钟）
	log.Info("\n=== 阶段4: 备节点成为主节点并执行操作（3分钟）===")
	if err := sim.secondaryTakeoverOperations(); err != nil {
		return err
	}

	// 5. 原主节点重新上线并进行事件重播
	log.Info("\n=== 阶段5: 原主节点重新上线并进行事件重播 ===")
	if err := sim.primaryRecoveryAndReplay(); err != nil {
		return err
	}

	// 6. 验证数据一致性
	log.Info("\n=== 阶段6: 验证数据一致性 ===")
	if err := sim.verifyDataConsistency(); err != nil {
		return err
	}

	log.Info("\n=== 模拟完成 ===")
	return nil
}

// startNodes 启动节点
func (sim *HASimulation) startNodes() error {
	// 启动节点1作为主节点
	log.Info("启动节点1作为主节点...")
	if err := sim.Node1.Start("localhost:8081", hasqlite.Primary); err != nil {
		return fmt.Errorf("failed to start node1: %v", err)
	}

	// 等待一下
	time.Sleep(2 * time.Second)

	// 启动节点2作为备节点
	log.Info("启动节点2作为备节点...")
	if err := sim.Node2.Start("localhost:8080", hasqlite.Secondary); err != nil {
		return fmt.Errorf("failed to start node2: %v", err)
	}

	// 设置对等节点关系
	sim.Node1.HAManager.SetPeerNode("node-2", "localhost:8081", hasqlite.Secondary)
	sim.Node2.HAManager.SetPeerNode("node-1", "localhost:8080", hasqlite.Primary)

	time.Sleep(3 * time.Second)

	log.Infof("节点1角色: %s, 状态: %s", sim.Node1.GetRole(), sim.Node1.GetStatus())
	log.Infof("节点2角色: %s, 状态: %s", sim.Node2.GetRole(), sim.Node2.GetStatus())

	return nil
}

// primaryNodeOperations 主节点操作
func (sim *HASimulation) primaryNodeOperations() error {
	log.Info("主节点开始执行数据库操作...")

	// 模拟5分钟的操作，这里缩短为30秒用于演示
	duration := 30 * time.Second
	operationInterval := 2 * time.Second
	operationsPerInterval := 5

	ticker := time.NewTicker(operationInterval)
	defer ticker.Stop()

	timeout := time.After(duration)
	operationCount := 0

operationLoop:
	for {
		select {
		case <-timeout:
			break operationLoop
		case <-ticker.C:
			if sim.Node1.GetRole() == hasqlite.Primary {
				if err := sim.Node1.PerformDatabaseOperations(operationsPerInterval); err != nil {
					log.Errorf("Primary node operation failed: %v", err)
				}
				operationCount += operationsPerInterval
				log.Infof("主节点已执行 %d 个操作", operationCount)
			}
		case <-sim.Ctx.Done():
			return sim.Ctx.Err()
		}
	}

	// 显示当前数据统计
	count1, _ := sim.Node1.GetRecordCount()
	count2, _ := sim.Node2.GetRecordCount()
	log.Infof("操作完成 - 节点1记录数: %d, 节点2记录数: %d", count1, count2)

	return nil
}

// simulatePrimaryFailure 模拟主节点故障
func (sim *HASimulation) simulatePrimaryFailure() error {
	log.Warn("模拟主节点故障，停止节点1...")

	if err := sim.Node1.Stop(); err != nil {
		return fmt.Errorf("failed to stop node1: %v", err)
	}

	// 等待故障检测和切换
	log.Info("等待故障检测和自动切换...")
	time.Sleep(15 * time.Second)

	// 手动提升节点2为主节点（模拟自动故障转移）
	log.Info("提升节点2为主节点...")
	if err := sim.Node2.HAManager.PromoteToPrimary(); err != nil {
		log.Warnf("Failed to promote node2: %v", err)
	}

	time.Sleep(2 * time.Second)
	log.Infof("节点2新角色: %s, 状态: %s", sim.Node2.GetRole(), sim.Node2.GetStatus())

	return nil
}

// secondaryTakeoverOperations 备节点接管操作
func (sim *HASimulation) secondaryTakeoverOperations() error {
	log.Info("新主节点（原备节点）开始执行数据库操作...")

	// 模拟3分钟的操作，这里缩短为20秒用于演示
	duration := 20 * time.Second
	operationInterval := 2 * time.Second
	operationsPerInterval := 3

	ticker := time.NewTicker(operationInterval)
	defer ticker.Stop()

	timeout := time.After(duration)
	operationCount := 0

operationLoop:
	for {
		select {
		case <-timeout:
			break operationLoop
		case <-ticker.C:
			if sim.Node2.GetRole() == hasqlite.Primary {
				if err := sim.Node2.PerformDatabaseOperations(operationsPerInterval); err != nil {
					log.Errorf("New primary node operation failed: %v", err)
				}
				operationCount += operationsPerInterval
				log.Infof("新主节点已执行 %d 个操作", operationCount)
			}
		case <-sim.Ctx.Done():
			return sim.Ctx.Err()
		}
	}

	// 显示事件统计
	if sim.Node2.HAManager.EventStore != nil {
		eventCount, _ := sim.Node2.HAManager.GetEventCount()
		log.Infof("故障期间记录的事件数: %d", eventCount)
	}

	return nil
}

// primaryRecoveryAndReplay 主节点恢复和事件重播
func (sim *HASimulation) primaryRecoveryAndReplay() error {
	log.Info("重新启动原主节点...")

	// 重新启动节点1作为备节点
	if err := sim.Node1.Start("localhost:8081", hasqlite.Secondary); err != nil {
		return fmt.Errorf("failed to restart node1: %v", err)
	}

	// 重新设置对等节点关系
	sim.Node1.HAManager.SetPeerNode("node-2", "localhost:8081", hasqlite.Primary)
	sim.Node2.HAManager.SetPeerNode("node-1", "localhost:8080", hasqlite.Secondary)

	time.Sleep(3 * time.Second)

	log.Infof("节点1恢复后角色: %s, 状态: %s", sim.Node1.GetRole(), sim.Node1.GetStatus())
	log.Infof("节点2当前角色: %s, 状态: %s", sim.Node2.GetRole(), sim.Node2.GetStatus())

	// 执行数据同步
	log.Info("开始数据同步...")
	if err := sim.Node1.HAManager.SyncNow(); err != nil {
		log.Errorf("Sync failed: %v", err)
	} else {
		log.Info("数据同步完成")
	}

	// 等待同步完成
	time.Sleep(5 * time.Second)

	// 模拟事件重播（这里简化实现）
	log.Info("开始事件重播...")
	if sim.Node2.HAManager.EventStore != nil {
		events, err := sim.Node2.HAManager.EventStore.GetUnappliedEvents(100)
		if err != nil {
			log.Errorf("Failed to get unapplied events: %v", err)
		} else {
			log.Infof("找到 %d 个未应用的事件", len(events))
			// 这里应该将事件应用到节点1，简化为日志输出
			for _, event := range events {
				log.Debugf("重播事件: %s %s on %s", event.EventID, event.EventType, event.TableName)
			}
			log.Info("事件重播完成")
		}
	}

	return nil
}

// verifyDataConsistency 验证数据一致性
func (sim *HASimulation) verifyDataConsistency() error {
	log.Info("验证数据一致性...")

	// 获取两个节点的记录数
	count1, err1 := sim.Node1.GetRecordCount()
	count2, err2 := sim.Node2.GetRecordCount()

	if err1 != nil {
		log.Errorf("Failed to get count from node1: %v", err1)
	}
	if err2 != nil {
		log.Errorf("Failed to get count from node2: %v", err2)
	}

	log.Infof("最终数据统计:")
	log.Infof("  节点1记录数: %d", count1)
	log.Infof("  节点2记录数: %d", count2)

	if count1 == count2 {
		log.Info("✅ 数据一致性验证通过")
	} else {
		log.Warn("⚠️  数据不一致，可能需要进一步同步")
	}

	// 显示同步历史
	if history, err := sim.Node1.HAManager.GetSyncHistory(5); err == nil {
		log.Infof("同步历史记录:")
		for _, record := range history {
			log.Infof("  %s: %s -> %s, 状态: %s, 记录数: %d",
				record.SyncID, record.SourceNode, record.TargetNode,
				record.Status, record.RecordsCount)
		}
	}

	return nil
}

// Cleanup 清理资源
func (sim *HASimulation) Cleanup() {
	log.Info("清理资源...")

	if sim.Node1 != nil {
		sim.Node1.Stop()
	}
	if sim.Node2 != nil {
		sim.Node2.Stop()
	}

	sim.Cancel()

	// 清理临时文件
	os.RemoveAll("/tmp/ha-sqlite-simulation")
}

// main 主函数
func main() {
	// 设置日志格式
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	// 创建模拟器
	sim := NewHASimulation()
	defer sim.Cleanup()

	// 设置信号处理
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 在goroutine中运行模拟
	errChan := make(chan error, 1)
	go func() {
		errChan <- sim.Run()
	}()

	// 等待完成或中断
	select {
	case err := <-errChan:
		if err != nil {
			log.Errorf("Simulation failed: %v", err)
			os.Exit(1)
		}
		log.Info("模拟成功完成")
	case <-sigChan:
		log.Info("收到中断信号，正在停止模拟...")
		sim.Cancel()
	}
}