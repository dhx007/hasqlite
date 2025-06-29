package hasqlite

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// NodeRole 节点角色
type NodeRole string

const (
	Primary   NodeRole = "primary"   // 主节点
	Secondary NodeRole = "secondary" // 备节点
	Unknown   NodeRole = "unknown"   // 未知状态
)

// NodeStatus 节点状态
type NodeStatus string

const (
	Healthy     NodeStatus = "healthy"     // 健康
	Unhealthy   NodeStatus = "unhealthy"   // 不健康
	Disconnected NodeStatus = "disconnected" // 断开连接
)

// NodeInfo 节点信息
type NodeInfo struct {
	ID       string     `json:"id"`
	Address  string     `json:"address"`
	Role     NodeRole   `json:"role"`
	Status   NodeStatus `json:"status"`
	LastSeen time.Time  `json:"last_seen"`
}

// NodeManager 节点管理器
type NodeManager struct {
	mu           sync.RWMutex
	currentNode  *NodeInfo
	peerNode     *NodeInfo
	heartbeatCh  chan bool
	failoverCh   chan NodeRole
	ctx          context.Context
	cancel       context.CancelFunc
	heartbeatInterval time.Duration
	failoverTimeout   time.Duration
	onRoleChange      func(oldRole, newRole NodeRole)
	onNodeFailure     func(nodeID string)
	onNodeRecovery    func(nodeID string)
}

// NewNodeManager 创建节点管理器
func NewNodeManager(nodeID, address string) *NodeManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	nm := &NodeManager{
		currentNode: &NodeInfo{
			ID:       nodeID,
			Address:  address,
			Role:     Secondary, // 默认启动为备节点
			Status:   Healthy,
			LastSeen: time.Now(),
		},
		heartbeatCh:       make(chan bool, 1),
		failoverCh:        make(chan NodeRole, 1),
		ctx:              ctx,
		cancel:           cancel,
		heartbeatInterval: 10 * time.Second,
		failoverTimeout:   30 * time.Second,
	}
	
	return nm
}

// Start 启动节点管理器
func (nm *NodeManager) Start() error {
	log.Infof("Starting node manager for node %s at %s", nm.currentNode.ID, nm.currentNode.Address)
	
	// 启动心跳检测
	go nm.heartbeatLoop()
	
	// 启动故障检测
	go nm.failureDetectionLoop()
	
	return nil
}

// Stop 停止节点管理器
func (nm *NodeManager) Stop() {
	log.Info("Stopping node manager")
	nm.cancel()
}

// GetCurrentRole 获取当前节点角色
func (nm *NodeManager) GetCurrentRole() NodeRole {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.currentNode.Role
}

// GetCurrentStatus 获取当前节点状态
func (nm *NodeManager) GetCurrentStatus() NodeStatus {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.currentNode.Status
}

// GetPeerNode 获取对等节点信息
func (nm *NodeManager) GetPeerNode() *NodeInfo {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	if nm.peerNode == nil {
		return nil
	}
	// 返回副本避免并发修改
	peer := *nm.peerNode
	return &peer
}

// SetPeerNode 设置对等节点
func (nm *NodeManager) SetPeerNode(nodeInfo *NodeInfo) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	nm.peerNode = nodeInfo
	log.Infof("Peer node set: %s at %s (role: %s)", nodeInfo.ID, nodeInfo.Address, nodeInfo.Role)
}

// PromoteToPrimary 提升为主节点
func (nm *NodeManager) PromoteToPrimary() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	
	oldRole := nm.currentNode.Role
	if oldRole == Primary {
		return fmt.Errorf("node is already primary")
	}
	
	nm.currentNode.Role = Primary
	log.Infof("Node %s promoted to primary", nm.currentNode.ID)
	
	if nm.onRoleChange != nil {
		go nm.onRoleChange(oldRole, Primary)
	}
	
	return nil
}

// DemoteToSecondary 降级为备节点
func (nm *NodeManager) DemoteToSecondary() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	
	oldRole := nm.currentNode.Role
	if oldRole == Secondary {
		return fmt.Errorf("node is already secondary")
	}
	
	nm.currentNode.Role = Secondary
	log.Infof("Node %s demoted to secondary", nm.currentNode.ID)
	
	if nm.onRoleChange != nil {
		go nm.onRoleChange(oldRole, Secondary)
	}
	
	return nil
}

// UpdatePeerHeartbeat 更新对等节点心跳
func (nm *NodeManager) UpdatePeerHeartbeat() {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	
	if nm.peerNode != nil {
		nm.peerNode.LastSeen = time.Now()
		nm.peerNode.Status = Healthy
	}
	
	// 发送心跳信号
	select {
	case nm.heartbeatCh <- true:
	default:
	}
}

// SetRoleChangeCallback 设置角色变更回调
func (nm *NodeManager) SetRoleChangeCallback(callback func(oldRole, newRole NodeRole)) {
	nm.onRoleChange = callback
}

// SetNodeFailureCallback 设置节点故障回调
func (nm *NodeManager) SetNodeFailureCallback(callback func(nodeID string)) {
	nm.onNodeFailure = callback
}

// SetNodeRecoveryCallback 设置节点恢复回调
func (nm *NodeManager) SetNodeRecoveryCallback(callback func(nodeID string)) {
	nm.onNodeRecovery = callback
}

// heartbeatLoop 心跳循环
func (nm *NodeManager) heartbeatLoop() {
	ticker := time.NewTicker(nm.heartbeatInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-nm.ctx.Done():
			return
		case <-ticker.C:
			nm.sendHeartbeat()
		}
	}
}

// failureDetectionLoop 故障检测循环
func (nm *NodeManager) failureDetectionLoop() {
	ticker := time.NewTicker(5 * time.Second) // 每5秒检查一次
	defer ticker.Stop()
	
	for {
		select {
		case <-nm.ctx.Done():
			return
		case <-ticker.C:
			nm.checkPeerHealth()
		}
	}
}

// sendHeartbeat 发送心跳
func (nm *NodeManager) sendHeartbeat() {
	nm.mu.Lock()
	nm.currentNode.LastSeen = time.Now()
	nm.mu.Unlock()
	
	// TODO: 实现实际的心跳发送逻辑
	// 这里可以通过HTTP、gRPC或其他方式发送心跳到对等节点
	log.Debugf("Sending heartbeat from node %s", nm.currentNode.ID)
}

// checkPeerHealth 检查对等节点健康状态
func (nm *NodeManager) checkPeerHealth() {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	
	if nm.peerNode == nil {
		return
	}
	
	// 检查对等节点是否超时
	timeSinceLastSeen := time.Since(nm.peerNode.LastSeen)
	if timeSinceLastSeen > nm.failoverTimeout {
		if nm.peerNode.Status != Disconnected {
			log.Warnf("Peer node %s appears to be disconnected (last seen: %v ago)", 
				nm.peerNode.ID, timeSinceLastSeen)
			nm.peerNode.Status = Disconnected
			
			// 如果对等节点是主节点且已断开连接，则提升当前节点为主节点
			if nm.peerNode.Role == Primary && nm.currentNode.Role == Secondary {
				log.Infof("Primary node %s is down, promoting current node to primary", nm.peerNode.ID)
				oldRole := nm.currentNode.Role
				nm.currentNode.Role = Primary
				
				if nm.onRoleChange != nil {
					go nm.onRoleChange(oldRole, Primary)
				}
				
				if nm.onNodeFailure != nil {
					go nm.onNodeFailure(nm.peerNode.ID)
				}
			}
		}
	} else if nm.peerNode.Status == Disconnected {
		// 对等节点恢复连接
		log.Infof("Peer node %s has recovered", nm.peerNode.ID)
		nm.peerNode.Status = Healthy
		
		if nm.onNodeRecovery != nil {
			go nm.onNodeRecovery(nm.peerNode.ID)
		}
	}
}

// IsPrimary 检查当前节点是否为主节点
func (nm *NodeManager) IsPrimary() bool {
	return nm.GetCurrentRole() == Primary
}

// IsSecondary 检查当前节点是否为备节点
func (nm *NodeManager) IsSecondary() bool {
	return nm.GetCurrentRole() == Secondary
}

// GetNodeInfo 获取当前节点信息
func (nm *NodeManager) GetNodeInfo() *NodeInfo {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	// 返回副本避免并发修改
	node := *nm.currentNode
	return &node
}