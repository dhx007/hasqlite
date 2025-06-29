package hasqlite

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

// APIServer HTTP API服务器
type APIServer struct {
	haManager *HAManager
	server    *http.Server
	port      int
}

// NewAPIServer 创建API服务器
func NewAPIServer(haManager *HAManager, port int) *APIServer {
	return &APIServer{
		haManager: haManager,
		port:      port,
	}
}

// Start 启动API服务器
func (api *APIServer) Start() error {
	mux := http.NewServeMux()
	
	// 注册路由
	api.registerRoutes(mux)
	
	api.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", api.port),
		Handler: mux,
	}
	
	log.Infof("Starting HA SQLite API server on port %d", api.port)
	
	go func() {
		if err := api.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("API server error: %v", err)
		}
	}()
	
	return nil
}

// Stop 停止API服务器
func (api *APIServer) Stop() error {
	if api.server == nil {
		return nil
	}
	
	log.Info("Stopping HA SQLite API server")
	return api.server.Close()
}

// registerRoutes 注册路由
func (api *APIServer) registerRoutes(mux *http.ServeMux) {
	// 健康检查
	mux.HandleFunc("/health", api.handleHealth)
	
	// 节点信息
	mux.HandleFunc("/api/node/info", api.handleNodeInfo)
	mux.HandleFunc("/api/node/role", api.handleNodeRole)
	mux.HandleFunc("/api/node/promote", api.handlePromoteNode)
	mux.HandleFunc("/api/node/demote", api.handleDemoteNode)
	
	// 心跳
	mux.HandleFunc("/api/heartbeat", api.handleHeartbeat)
	
	// 数据库文件传输
	mux.HandleFunc("/api/database/", api.handleDatabaseFile)
	
	// 同步管理
	mux.HandleFunc("/api/sync/now", api.handleSyncNow)
	mux.HandleFunc("/api/sync/history", api.handleSyncHistory)
	
	// 事件管理
	mux.HandleFunc("/api/events/count", api.handleEventCount)
	mux.HandleFunc("/api/events/unapplied", api.handleUnappliedEvents)
}

// handleHealth 健康检查处理器
func (api *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"node_role": api.haManager.GetNodeRole(),
		"node_status": api.haManager.GetNodeStatus(),
	}
	
	api.writeJSONResponse(w, http.StatusOK, response)
}

// handleNodeInfo 节点信息处理器
func (api *APIServer) handleNodeInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	
	nodeInfo := api.haManager.nodeManager.GetNodeInfo()
	peerInfo := api.haManager.nodeManager.GetPeerNode()
	
	response := map[string]interface{}{
		"current_node": nodeInfo,
		"peer_node":    peerInfo,
	}
	
	api.writeJSONResponse(w, http.StatusOK, response)
}

// handleNodeRole 节点角色处理器
func (api *APIServer) handleNodeRole(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	
	response := map[string]interface{}{
		"role":   api.haManager.GetNodeRole(),
		"status": api.haManager.GetNodeStatus(),
	}
	
	api.writeJSONResponse(w, http.StatusOK, response)
}

// handlePromoteNode 提升节点处理器
func (api *APIServer) handlePromoteNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	
	if err := api.haManager.PromoteToPrimary(); err != nil {
		api.writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}
	
	response := map[string]interface{}{
		"message": "Node promoted to primary",
		"role":    api.haManager.GetNodeRole(),
	}
	
	api.writeJSONResponse(w, http.StatusOK, response)
}

// handleDemoteNode 降级节点处理器
func (api *APIServer) handleDemoteNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	
	if err := api.haManager.DemoteToSecondary(); err != nil {
		api.writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}
	
	response := map[string]interface{}{
		"message": "Node demoted to secondary",
		"role":    api.haManager.GetNodeRole(),
	}
	
	api.writeJSONResponse(w, http.StatusOK, response)
}

// handleHeartbeat 心跳处理器
func (api *APIServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	
	// 更新对等节点心跳
	api.haManager.nodeManager.UpdatePeerHeartbeat()
	
	response := map[string]interface{}{
		"message":   "Heartbeat received",
		"timestamp": time.Now(),
	}
	
	api.writeJSONResponse(w, http.StatusOK, response)
}

// handleDatabaseFile 数据库文件处理器
func (api *APIServer) handleDatabaseFile(w http.ResponseWriter, r *http.Request) {
	// 从URL路径中提取数据库类型
	dbType := filepath.Base(r.URL.Path)
	if dbType == "" {
		api.writeErrorResponse(w, http.StatusBadRequest, "Database type not specified")
		return
	}
	
	switch r.Method {
	case http.MethodGet:
		api.handleDownloadDatabase(w, r, dbType)
	case http.MethodPost:
		api.handleUploadDatabase(w, r, dbType)
	default:
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleDownloadDatabase 下载数据库文件
func (api *APIServer) handleDownloadDatabase(w http.ResponseWriter, r *http.Request, dbType string) {
	// 只有主节点才能提供数据库文件下载
	if !api.haManager.nodeManager.IsPrimary() {
		api.writeErrorResponse(w, http.StatusForbidden, "Only primary node can serve database files")
		return
	}
	
	// 构造数据库文件路径
	dbPath := api.getDatabasePath(dbType)
	if dbPath == "" {
		api.writeErrorResponse(w, http.StatusNotFound, "Database type not found")
		return
	}
	
	// 检查文件是否存在
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		api.writeErrorResponse(w, http.StatusNotFound, "Database file not found")
		return
	}
	
	// 打开文件
	file, err := os.Open(dbPath)
	if err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to open database file: %v", err))
		return
	}
	defer file.Close()
	
	// 获取文件信息
	fileInfo, err := file.Stat()
	if err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to get file info: %v", err))
		return
	}
	
	// 设置响应头
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s.db", dbType))
	w.Header().Set("Content-Length", strconv.FormatInt(fileInfo.Size(), 10))
	
	// 复制文件内容到响应
	if _, err := io.Copy(w, file); err != nil {
		log.Errorf("Failed to send database file %s: %v", dbType, err)
	}
	
	log.Infof("Database file %s sent to client", dbType)
}

// handleUploadDatabase 上传数据库文件
func (api *APIServer) handleUploadDatabase(w http.ResponseWriter, r *http.Request, dbType string) {
	// 只有备节点才能接收数据库文件上传
	if !api.haManager.nodeManager.IsSecondary() {
		api.writeErrorResponse(w, http.StatusForbidden, "Only secondary node can receive database files")
		return
	}
	
	// 构造数据库文件路径
	dbPath := api.getDatabasePath(dbType)
	if dbPath == "" {
		api.writeErrorResponse(w, http.StatusNotFound, "Database type not found")
		return
	}
	
	// 创建临时文件
	tempPath := dbPath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to create temp file: %v", err))
		return
	}
	defer tempFile.Close()
	
	// 复制上传的内容到临时文件
	if _, err := io.Copy(tempFile, r.Body); err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to write temp file: %v", err))
		os.Remove(tempPath)
		return
	}
	
	// 关闭临时文件
	tempFile.Close()
	
	// 原子性地替换原文件
	if err := os.Rename(tempPath, dbPath); err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to replace database file: %v", err))
		os.Remove(tempPath)
		return
	}
	
	response := map[string]interface{}{
		"message": fmt.Sprintf("Database %s uploaded successfully", dbType),
	}
	
	api.writeJSONResponse(w, http.StatusOK, response)
	log.Infof("Database file %s received from client", dbType)
}

// handleSyncNow 立即同步处理器
func (api *APIServer) handleSyncNow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	
	if err := api.haManager.SyncNow(); err != nil {
		api.writeErrorResponse(w, http.StatusBadRequest, err.Error())
		return
	}
	
	response := map[string]interface{}{
		"message": "Sync initiated",
	}
	
	api.writeJSONResponse(w, http.StatusOK, response)
}

// handleSyncHistory 同步历史处理器
func (api *APIServer) handleSyncHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	
	// 获取limit参数
	limitStr := r.URL.Query().Get("limit")
	limit := 10 // 默认限制
	if limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}
	
	history, err := api.haManager.GetSyncHistory(limit)
	if err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	
	response := map[string]interface{}{
		"history": history,
		"count":   len(history),
	}
	
	api.writeJSONResponse(w, http.StatusOK, response)
}

// handleEventCount 事件计数处理器
func (api *APIServer) handleEventCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	
	count, err := api.haManager.GetEventCount()
	if err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	
	response := map[string]interface{}{
		"event_count": count,
	}
	
	api.writeJSONResponse(w, http.StatusOK, response)
}

// handleUnappliedEvents 未应用事件处理器
func (api *APIServer) handleUnappliedEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.writeErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	
	if api.haManager.eventStore == nil {
		api.writeErrorResponse(w, http.StatusServiceUnavailable, "Event store is not enabled")
		return
	}
	
	// 获取limit参数
	limitStr := r.URL.Query().Get("limit")
	limit := 100 // 默认限制
	if limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}
	
	events, err := api.haManager.eventStore.GetUnappliedEvents(limit)
	if err != nil {
		api.writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	
	response := map[string]interface{}{
		"events": events,
		"count":  len(events),
	}
	
	api.writeJSONResponse(w, http.StatusOK, response)
}

// getDatabasePath 获取数据库路径
func (api *APIServer) getDatabasePath(dbType string) string {
	// 这里应该从配置或数据库管理器中获取实际的数据库路径
	// 简化实现
	switch dbType {
	case "oams":
		return filepath.Join(api.haManager.config.DataDir, "oams.db")
	case "monitor":
		return filepath.Join(api.haManager.config.DataDir, "monitor.db")
	case "resource":
		return filepath.Join(api.haManager.config.DataDir, "resource.db")
	case "spliter":
		return filepath.Join(api.haManager.config.DataDir, "spliter.db")
	default:
		return ""
	}
}

// writeJSONResponse 写入JSON响应
func (api *APIServer) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Errorf("Failed to encode JSON response: %v", err)
	}
}

// writeErrorResponse 写入错误响应
func (api *APIServer) writeErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	errorResponse := map[string]interface{}{
		"error":     message,
		"timestamp": time.Now(),
	}
	
	api.writeJSONResponse(w, statusCode, errorResponse)
}