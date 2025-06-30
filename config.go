package hasqlite

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

type Config struct {
	HA       *HAConfig       `json:"ha"`
	Security *SecurityConfig `json:"security"`
	Logging  *LoggingConfig  `json:"logging"`
	Metrics  *MetricsConfig  `json:"metrics"`
}

type LoggingConfig struct {
	Level      string `json:"level"`
	Format     string `json:"format"`
	OutputFile string `json:"output_file"`
	MaxSize    int    `json:"max_size"`
	MaxBackups int    `json:"max_backups"`
	MaxAge     int    `json:"max_age"`
}

type MetricsConfig struct {
	Enabled    bool   `json:"enabled"`
	Port       int    `json:"port"`
	Path       string `json:"path"`
	Interval   time.Duration `json:"interval"`
}

func LoadConfig(configPath string) (*Config, error) {
	if configPath == "" {
		configPath = "config.json"
	}
	
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}
	
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}
	
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %v", err)
	}
	
	return &config, nil
}

func (c *Config) Validate() error {
	if c.HA == nil {
		return fmt.Errorf("HA config is required")
	}
	
	if c.HA.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	
	if c.HA.Address == "" {
		return fmt.Errorf("node address is required")
	}
	
	if c.HA.DataDir == "" {
		return fmt.Errorf("data directory is required")
	}
	
	// 检查数据目录是否存在，不存在则创建
	if err := os.MkdirAll(c.HA.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}
	
	// 验证其他配置项
	if c.HA.SyncInterval < time.Second {
		return fmt.Errorf("sync interval too short")
	}
	
	return nil
}

func (c *Config) SaveToFile(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	
	return ioutil.WriteFile(path, data, 0644)
}