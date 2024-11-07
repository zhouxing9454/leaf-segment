package core

import (
	"encoding/json"
	"os"
)

// Config 定义配置文件的格式
type Config struct {
	DSN              string `json:"dsn"`                // 数据库连接字符串
	Table            string `json:"table"`              // 数据库中用于存储段的表名
	HttpPort         int    `json:"http_port"`          // HTTP服务器的监听端口
	HttpReadTimeout  int    `json:"http_read_timeout"`  // HTTP读取请求的超时时间（毫秒）
	HttpWriteTimeout int    `json:"http_write_timeout"` // HTTP写入响应的超时时间（毫秒）
}

// DefaultConfig 是一个全局的配置变量，用于存储加载后的配置
var DefaultConfig *Config

// LoadConfig 从指定的JSON文件加载配置到DefaultConfig
func LoadConfig(filename string) error {
	// 读取配置文件内容,如果读取文件失败，返回错误
	content, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	// 创建Config实例用于解析JSON
	config := Config{}

	// 将JSON内容解析到config结构体,如果解析JSON失败，返回错误
	err = json.Unmarshal(content, &config)
	if err != nil {
		return err
	}

	// 配置文件加载成功，将解析后的配置赋值给全局变量DefaultConfig
	DefaultConfig = &config

	// 返回nil表示加载成功
	return nil
}
