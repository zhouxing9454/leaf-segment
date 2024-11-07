package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

// AllocResponse 用于封装分配ID请求的响应
type AllocResponse struct {
	ErrNo int    `json:"err_no"` // 错误码
	Msg   string `json:"msg"`    // 错误或成功消息
	ID    int64  `json:"id"`     // 分配的ID
}

// HealthResponse 用于封装健康检查请求的响应
type HealthResponse struct {
	ErrNo int    `json:"err_no"` // 错误码
	Msg   string `json:"msg"`    // 错误或成功消息
	Left  int64  `json:"left"`   // 剩余ID数量
}

// handleAlloc 处理分配 ID 的 HTTP 请求
func handleAlloc(w http.ResponseWriter, r *http.Request) {
	var (
		resp   = AllocResponse{} // 响应数据
		err    error             // 错误信息
		bytes  []byte            // 响应数据的JSON字节数组
		bizTag string            // 业务标签
	)

	// 解析请求参数
	if err = r.ParseForm(); err != nil {
		goto RESP // 解析失败则跳转到响应逻辑
	}

	// 获取并验证 biz_tag 参数
	if bizTag = r.Form.Get("biz_tag"); bizTag == "" {
		err = errors.New("need biz_tag param") // 缺少biz_tag参数
		goto RESP
	}

	// 循环分配ID，确保ID不为0
	for {
		if resp.ID, err = DefaultAlloc.NextId(bizTag); err != nil {
			goto RESP // 分配ID出错则跳转到响应逻辑
		}
		if resp.ID != 0 { // 跳过ID为0的情况
			break
		}
	}

RESP:
	// 设置响应信息和状态码
	if err != nil {
		resp.ErrNo = -1                               // 错误码
		resp.Msg = fmt.Sprintf("%v", err)             // 错误信息
		w.WriteHeader(http.StatusInternalServerError) // 设置HTTP500错误码
	} else {
		resp.Msg = "success" // 成功消息
	}

	// 将响应数据编码为JSON并写入响应
	if bytes, err = json.Marshal(&resp); err == nil {
		_, _ = w.Write(bytes) // 写入响应数据
	} else {
		w.WriteHeader(http.StatusInternalServerError) // JSON 编码失败返回 HTTP 500
	}
}

// handleHealth 处理健康检查的 HTTP 请求
func handleHealth(w http.ResponseWriter, r *http.Request) {
	var (
		resp   = HealthResponse{} // 响应数据
		err    error              // 错误信息
		bizTag string             // 业务标签
	)

	// 解析请求参数
	if err = r.ParseForm(); err != nil {
		goto RESP // 解析失败则跳转到响应逻辑
	}

	// 获取并验证 biz_tag 参数
	if bizTag = r.Form.Get("biz_tag"); bizTag == "" {
		err = errors.New("need biz_tag param") // 缺少 biz_tag 参数
		goto RESP
	}

	// 查询剩余 ID 数量
	resp.Left = DefaultAlloc.LeftCount(bizTag)
	if resp.Left == 0 { // 没有剩余 ID
		err = errors.New("no available id")
		goto RESP
	}

RESP:
	// 设置响应信息和状态码
	if err != nil {
		resp.ErrNo = -1                               // 错误码
		resp.Msg = fmt.Sprintf("%v", err)             // 错误信息
		w.WriteHeader(http.StatusInternalServerError) // 设置 HTTP 500 错误码
	} else {
		resp.Msg = "success" // 成功消息
	}

	// 将响应数据编码为 JSON 并写入响应
	if bytes, err := json.Marshal(&resp); err == nil {
		_, _ = w.Write(bytes) // 写入响应数据
	} else {
		w.WriteHeader(http.StatusInternalServerError) // JSON 编码失败返回 HTTP 500
	}
}

// StartServer 启动 HTTP 服务器
func StartServer() error {
	// 创建 HTTP 路由多路复用器
	mux := http.NewServeMux()
	mux.HandleFunc("/alloc", handleAlloc)   // 路由分配 ID 请求
	mux.HandleFunc("/health", handleHealth) // 路由健康检查请求

	// 初始化 HTTP 服务器
	srv := &http.Server{
		ReadTimeout:  time.Duration(DefaultConfig.HttpReadTimeout) * time.Millisecond,  // 读取超时时间
		WriteTimeout: time.Duration(DefaultConfig.HttpWriteTimeout) * time.Millisecond, // 写入超时时间
		Handler:      mux,                                                              // 路由处理器
	}

	// 设置服务器监听端口
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(DefaultConfig.HttpPort))
	if err != nil {
		return err // 监听失败返回错误
	}

	// 启动 HTTP 服务器
	return srv.Serve(listener)
}
