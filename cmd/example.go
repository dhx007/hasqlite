package main

import (
	"fmt"
	"path/filepath"
	"time"

	"ws-oams/internal/database"
	"ws-oams/internal/hasqlite"
	"ws-oams/internal/install"

	log "github.com/sirupsen/logrus"
)

// ExampleUsage 展示如何使用高可用SQLite系统
func ExampleUsage() {
	// 1. 创建高可用配置
	config := &hasqlite.HAConfig{
		NodeID:            "node-1",
		Address:           "192.168.1.100:8080",
		PeerAddress:       "192.168.1.101:8080",
		DataDir:           filepath.Join(install.AppDirectory, "ha-data"),
		SyncInterval:      30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		FailoverTimeout:   30 * time.Second,
		EnableEventStore:  true,
	}

	// 2. 创建并初始化HA管理器
	haManager, err := hasqlite.NewHAManager(config)
	if err != nil {
		log.Fatalf("Failed to create HA manager: %v", err)
	}

	if err := haManager.Initialize(); err != nil {
		log.Fatalf("Failed to initialize HA manager: %v", err)
	}

	// 3. 设置对等节点信息
	haManager.SetPeerNode("node-2", "192.168.1.101:8080", hasqlite.Primary)

	// 4. 启动HA管理器
	if err := haManager.Start(); err != nil {
		log.Fatalf("Failed to start HA manager: %v", err)
	}

	// 5. 创建并启动API服务器
	apiServer := hasqlite.NewAPIServer(haManager, 8080)
	if err := apiServer.Start(); err != nil {
		log.Fatalf("Failed to start API server: %v", err)
	}

	// 6. 包装现有数据库连接
	if database.Manager != nil {
		// 获取原始数据库连接
		oamsDB, err := database.Manager.GetDB(database.OamsDB)
		if err == nil {
			// 用HA功能包装数据库连接
			haDB := haManager.WrapDatabaseWithHA(database.OamsDB, oamsDB)
			log.Infof("OAMS database wrapped with HA functionality")
			_ = haDB // 使用包装后的数据库连接
		}

		monitorDB, err := database.Manager.GetDB(database.MonitorDB)
		if err == nil {
			haDB := haManager.WrapDatabaseWithHA(database.MonitorDB, monitorDB)
			log.Infof("Monitor database wrapped with HA functionality")
			_ = haDB
		}

		resourceDB, err := database.Manager.GetDB(database.ResourceDB)
		if err == nil {
			haDB := haManager.WrapDatabaseWithHA(database.ResourceDB, resourceDB)
			log.Infof("Resource database wrapped with HA functionality")
			_ = haDB
		}
	}

	// 7. 监控节点状态
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				role := haManager.GetNodeRole()
				status := haManager.GetNodeStatus()
				log.Infof("Node status: role=%s, status=%s", role, status)

				// 如果是备节点，显示最后同步时间
				if role == hasqlite.Secondary {
					lastSync := haManager.SyncManager.GetLastSyncTime()
					if !lastSync.IsZero() {
						log.Infof("Last sync: %v ago", time.Since(lastSync))
					}
				}
			}
		}
	}()

	log.Info("HA SQLite system started successfully")

	// 注意：在实际应用中，你需要适当地处理关闭逻辑
	// defer haManager.Close()
	// defer apiServer.Stop()
}

// ExampleManualFailover 展示如何手动执行故障转移
func ExampleManualFailover(haManager *hasqlite.HAManager) {
	currentRole := haManager.GetNodeRole()
	log.Infof("Current node role: %s", currentRole)

	if currentRole == hasqlite.Secondary {
		// 手动提升为主节点
		if err := haManager.PromoteToPrimary(); err != nil {
			log.Errorf("Failed to promote to primary: %v", err)
		} else {
			log.Info("Successfully promoted to primary")
		}
	} else if currentRole == hasqlite.Primary {
		// 手动降级为备节点
		if err := haManager.DemoteToSecondary(); err != nil {
			log.Errorf("Failed to demote to secondary: %v", err)
		} else {
			log.Info("Successfully demoted to secondary")
		}
	}
}

// ExampleSyncOperations 展示同步操作
func ExampleSyncOperations(haManager *hasqlite.HAManager) {
	// 立即执行同步
	if err := haManager.SyncNow(); err != nil {
		log.Errorf("Failed to sync now: %v", err)
	} else {
		log.Info("Sync initiated successfully")
	}

	// 获取同步历史
	history, err := haManager.GetSyncHistory(10)
	if err != nil {
		log.Errorf("Failed to get sync history: %v", err)
	} else {
		log.Infof("Retrieved %d sync records", len(history))
		for _, record := range history {
			log.Infof("Sync %s: %s -> %s, status: %s, records: %d",
				record.SyncID, record.SourceNode, record.TargetNode,
				record.Status, record.RecordsCount)
		}
	}
}

// ExampleEventOperations 展示事件操作
func ExampleEventOperations(haManager *hasqlite.HAManager) {
	// 获取事件总数
	count, err := haManager.GetEventCount()
	if err != nil {
		log.Errorf("Failed to get event count: %v", err)
	} else {
		log.Infof("Total events: %d", count)
	}

	// 如果事件存储可用，获取未应用的事件
	if haManager.EventStore != nil {
		events, err := haManager.EventStore.GetUnappliedEvents(10)
		if err != nil {
			log.Errorf("Failed to get unapplied events: %v", err)
		} else {
			log.Infof("Unapplied events: %d", len(events))
			for _, event := range events {
				log.Infof("Event %s: %s on %s at %v",
					event.EventID, event.EventType, event.TableName, event.Timestamp)
			}
		}
	}
}

// ExampleDatabaseOperations 展示数据库操作
func ExampleDatabaseOperations(haManager *hasqlite.HAManager) {
	// 获取包装后的数据库连接
	db, err := haManager.GetDB(database.OamsDB)
	if err != nil {
		log.Errorf("Failed to get database: %v", err)
		return
	}

	// 执行数据库操作（这些操作会被事件存储记录）
	// 注意：这里只是示例，实际的表结构需要根据你的应用定义

	// 示例：创建一个测试表
	type TestRecord struct {
		ID    uint   `gorm:"primaryKey"`
		Name  string `gorm:"size:100"`
		Value int
	}

	// 自动迁移表结构
	if err := db.AutoMigrate(&TestRecord{}); err != nil {
		log.Errorf("Failed to migrate test table: %v", err)
		return
	}

	// 插入记录
	testRecord := &TestRecord{
		Name:  "test",
		Value: 123,
	}

	if err := db.Create(testRecord).Error; err != nil {
		log.Errorf("Failed to create test record: %v", err)
	} else {
		log.Infof("Created test record with ID: %d", testRecord.ID)
	}

	// 更新记录
	if err := db.Model(testRecord).Update("value", 456).Error; err != nil {
		log.Errorf("Failed to update test record: %v", err)
	} else {
		log.Info("Updated test record")
	}

	// 删除记录
	if err := db.Delete(testRecord).Error; err != nil {
		log.Errorf("Failed to delete test record: %v", err)
	} else {
		log.Info("Deleted test record")
	}
}

// ExampleCleanup 展示如何正确清理资源
func ExampleCleanup(haManager *hasqlite.HAManager, apiServer *hasqlite.APIServer) {
	log.Info("Starting cleanup...")

	// 停止API服务器
	if err := apiServer.Stop(); err != nil {
		log.Errorf("Failed to stop API server: %v", err)
	}

	// 关闭HA管理器
	if err := haManager.Close(); err != nil {
		log.Errorf("Failed to close HA manager: %v", err)
	}

	log.Info("Cleanup completed")
}

// ExampleConfiguration 展示不同的配置选项
func ExampleConfiguration() {
	// 基本配置
	basicConfig := &hasqlite.HAConfig{
		NodeID:            "node-basic",
		Address:           "localhost:8080",
		DataDir:           "/tmp/ha-sqlite",
		SyncInterval:      30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		FailoverTimeout:   30 * time.Second,
		EnableEventStore:  true,
	}

	// 高频同步配置（适用于对数据一致性要求较高的场景）
	highFreqConfig := &hasqlite.HAConfig{
		NodeID:            "node-highfreq",
		Address:           "localhost:8081",
		DataDir:           "/tmp/ha-sqlite-hf",
		SyncInterval:      5 * time.Second,  // 5秒同步一次
		HeartbeatInterval: 3 * time.Second,  // 3秒心跳一次
		FailoverTimeout:   10 * time.Second, // 10秒故障转移
		EnableEventStore:  true,
	}

	// 低频同步配置（适用于对性能要求较高的场景）
	lowFreqConfig := &hasqlite.HAConfig{
		NodeID:            "node-lowfreq",
		Address:           "localhost:8082",
		DataDir:           "/tmp/ha-sqlite-lf",
		SyncInterval:      5 * time.Minute,  // 5分钟同步一次
		HeartbeatInterval: 30 * time.Second, // 30秒心跳一次
		FailoverTimeout:   2 * time.Minute,  // 2分钟故障转移
		EnableEventStore:  false,            // 禁用事件存储以提高性能
	}

	fmt.Printf("Basic config: %+v\n", basicConfig)
	fmt.Printf("High frequency config: %+v\n", highFreqConfig)
	fmt.Printf("Low frequency config: %+v\n", lowFreqConfig)
}
