# 高可用SQLite系统 (HA SQLite)

这是一个为ws-oams项目设计的高可用SQLite数据库系统，实现了主备节点管理、自动故障转移、定时同步和基于事件溯源的数据恢复机制。

## 系统架构

### 核心组件

1. **节点管理器 (NodeManager)** - `node_manager.go`
   - 管理主备节点状态
   - 心跳检测和故障发现
   - 自动故障转移

2. **事件存储 (EventStore)** - `event_store.go`
   - 基于事件溯源模式记录数据库操作
   - 支持事件回放和数据恢复
   - 批量写入优化性能

3. **同步管理器 (SyncManager)** - `sync_manager.go`
   - 定时同步主备数据库
   - 增量同步和完整同步
   - 同步历史记录

4. **高可用管理器 (HAManager)** - `ha_manager.go`
   - 统一管理所有HA组件
   - 与现有database包集成
   - 数据库连接包装和拦截

5. **API服务器 (APIServer)** - `api_server.go`
   - 提供HTTP API接口
   - 节点间通信
   - 数据库文件传输

## 主要特性

### 1. 主备架构
- 支持一主一备的高可用架构
- 自动故障检测和切换
- 手动故障转移支持

### 2. 定时同步
- 备节点每30秒同步一次主节点数据
- 可配置同步间隔
- 支持立即同步

### 3. 事件溯源
- 记录所有数据库操作事件
- 支持主节点恢复时的数据回放
- 事件完整性校验

### 4. 故障处理
- 主节点宕机时备节点自动提升
- 主节点恢复时自动同步变更记录
- 数据一致性保证

### 5. 监控和管理
- 实时节点状态监控
- 同步历史查询
- 事件统计和查询

## 快速开始

### 1. 基本配置

```go
package main

import (
    "ws-oams/internal/hasqlite"
    "ws-oams/internal/install"
    "path/filepath"
    "time"
)

func main() {
    // 创建配置
    config := &hasqlite.HAConfig{
        NodeID:           "node-1",
        Address:          "192.168.1.100:8080",
        PeerAddress:      "192.168.1.101:8080",
        DataDir:          filepath.Join(install.AppDirectory, "ha-data"),
        SyncInterval:     30 * time.Second,
        HeartbeatInterval: 10 * time.Second,
        FailoverTimeout:  30 * time.Second,
        EnableEventStore: true,
    }

    // 创建并初始化HA管理器
    haManager, err := hasqlite.NewHAManager(config)
    if err != nil {
        panic(err)
    }

    if err := haManager.Initialize(); err != nil {
        panic(err)
    }

    // 设置对等节点
    haManager.SetPeerNode("node-2", "192.168.1.101:8080", hasqlite.Primary)

    // 启动HA管理器
    if err := haManager.Start(); err != nil {
        panic(err)
    }

    // 启动API服务器
    apiServer := hasqlite.NewAPIServer(haManager, 8080)
    if err := apiServer.Start(); err != nil {
        panic(err)
    }

    // 包装现有数据库连接
    if database.Manager != nil {
        oamsDB, _ := database.Manager.GetDB(database.OamsDB)
        haDB := haManager.WrapDatabaseWithHA(database.OamsDB, oamsDB)
        // 使用haDB进行数据库操作
    }

    defer haManager.Close()
    defer apiServer.Stop()
}
```

### 2. 与现有系统集成

在现有的数据库初始化代码中添加HA支持：

```go
// 在database包的初始化函数中
func InitializeWithHA() error {
    // 先初始化普通数据库管理器
    manager := NewDBManager(nil)
    if err := manager.Initialize(); err != nil {
        return err
    }

    // 创建HA配置
    haConfig := hasqlite.DefaultHAConfig()
    haConfig.NodeID = "node-1"
    haConfig.Address = "localhost:8080"
    haConfig.DataDir = install.AppDirectory

    // 初始化HA管理器
    haManager, err := hasqlite.NewHAManager(haConfig)
    if err != nil {
        return err
    }

    if err := haManager.Initialize(); err != nil {
        return err
    }

    if err := haManager.Start(); err != nil {
        return err
    }

    // 包装所有数据库连接
    for _, dbType := range []DBType{OamsDB, MonitorDB, ResourceDB, SpliterDB} {
        if db, err := manager.GetDB(dbType); err == nil {
            haManager.WrapDatabaseWithHA(dbType, db)
        }
    }

    return nil
}
```

## API接口

### 节点管理

- `GET /api/node/info` - 获取节点信息
- `GET /api/node/role` - 获取节点角色
- `POST /api/node/promote` - 提升为主节点
- `POST /api/node/demote` - 降级为备节点
- `POST /api/heartbeat` - 心跳接口

### 同步管理

- `POST /api/sync/now` - 立即执行同步
- `GET /api/sync/history?limit=10` - 获取同步历史

### 事件管理

- `GET /api/events/count` - 获取事件总数
- `GET /api/events/unapplied?limit=100` - 获取未应用事件

### 数据库文件传输

- `GET /api/database/{dbType}` - 下载数据库文件
- `POST /api/database/{dbType}` - 上传数据库文件

### 健康检查

- `GET /health` - 健康检查

## 配置选项

### HAConfig 结构

```go
type HAConfig struct {
    NodeID           string        // 节点ID
    Address          string        // 节点地址
    PeerAddress      string        // 对等节点地址
    DataDir          string        // 数据目录
    SyncInterval     time.Duration // 同步间隔
    HeartbeatInterval time.Duration // 心跳间隔
    FailoverTimeout  time.Duration // 故障转移超时
    EnableEventStore bool          // 是否启用事件存储
}
```

### 默认配置

```go
config := hasqlite.DefaultHAConfig()
// SyncInterval:     30 * time.Second
// HeartbeatInterval: 10 * time.Second
// FailoverTimeout:  30 * time.Second
// EnableEventStore: true
```

## 工作流程

### 正常运行

1. **主节点**：
   - 处理所有读写请求
   - 定期发送心跳
   - 不记录事件（性能优化）

2. **备节点**：
   - 每30秒从主节点同步数据
   - 监听主节点心跳
   - 只处理读请求

### 故障转移

1. **主节点故障**：
   - 备节点检测到心跳超时
   - 备节点自动提升为主节点
   - 开始记录所有数据库操作事件

2. **主节点恢复**：
   - 原主节点重新上线后变为备节点
   - 从新主节点同步故障期间的变更事件
   - 应用所有未处理的事件

### 事件溯源

1. **事件记录**：
   - 拦截所有数据库操作
   - 记录操作类型、表名、数据变更
   - 生成唯一事件ID和校验和

2. **事件回放**：
   - 按时间顺序获取未应用事件
   - 重新执行数据库操作
   - 标记事件为已应用

## 性能优化

### 1. 批量写入
- 事件存储使用批量写入减少I/O
- 可配置批量大小

### 2. WAL模式
- 所有SQLite数据库启用WAL模式
- 提高并发读写性能

### 3. 连接池
- 复用数据库连接
- 减少连接开销

### 4. 异步处理
- 心跳检测异步执行
- 事件记录异步处理

## 监控和调试

### 日志级别

```go
// 设置日志级别
log.SetLevel(log.DebugLevel) // 详细调试信息
log.SetLevel(log.InfoLevel)  // 一般信息
log.SetLevel(log.WarnLevel)  // 警告信息
```

### 关键指标

- 节点角色和状态
- 最后同步时间
- 事件总数和未应用事件数
- 同步成功率
- 故障转移次数

## 故障排除

### 常见问题

1. **同步失败**
   - 检查网络连接
   - 验证对等节点状态
   - 查看同步历史记录

2. **事件丢失**
   - 检查事件存储是否启用
   - 验证数据库回调是否正确注册
   - 查看事件存储日志

3. **故障转移不生效**
   - 检查心跳配置
   - 验证故障检测逻辑
   - 查看节点状态变化日志

### 调试命令

```bash
# 查看节点状态
curl http://localhost:8080/api/node/info

# 查看同步历史
curl http://localhost:8080/api/sync/history?limit=5

# 查看事件统计
curl http://localhost:8080/api/events/count

# 手动触发同步
curl -X POST http://localhost:8080/api/sync/now
```

## 注意事项

1. **数据一致性**：在网络分区情况下可能出现脑裂，需要外部仲裁机制
2. **性能影响**：事件记录会增加写操作延迟，可根据需要禁用
3. **存储空间**：事件存储会占用额外磁盘空间，需要定期清理
4. **网络依赖**：节点间通信依赖网络稳定性

## 扩展功能

### 未来计划

1. **多备节点支持**：支持一主多备架构
2. **自动发现**：节点自动发现和注册
3. **负载均衡**：读请求负载均衡
4. **数据压缩**：同步数据压缩传输
5. **加密传输**：节点间通信加密

## 许可证

本项目遵循与ws-oams主项目相同的许可证。