package main

import (
	"flag"
	"fmt"
	"go-id-alloc/core"
	"os"
	"runtime"
)

var (
	configFile string // 配置文件路径
)

// initCmd 初始化命令行参数
func initCmd() {
	// 设置 configFile 变量的默认值为 "./allocate.json"，并允许通过命令行传递配置文件路径
	flag.StringVar(&configFile, "config", "./allocate.json", "配置文件路径，默认是 ./allocate.json")
	// 解析命令行参数
	flag.Parse()
}

// initEnv 初始化环境配置
func initEnv() {
	// 设置 Go 运行时的最大 CPU 核心数为物理 CPU 核心数
	runtime.GOMAXPROCS(runtime.NumCPU())
}

// main 程序入口
func main() {
	// 初始化环境
	initEnv()

	// 初始化命令行参数
	initCmd()

	var err error = nil

	// 加载配置文件
	if err = core.LoadConfig(configFile); err != nil {
		// 如果加载配置失败，跳转到错误处理
		goto ERROR
	}

	// 初始化 MySQL 连接
	if err = core.InitData(); err != nil {
		// 如果初始化 MySQL 失败，跳转到错误处理
		goto ERROR
	}

	// 初始化分配器
	if err = core.InitAlloc(); err != nil {
		// 如果初始化分配器失败，跳转到错误处理
		goto ERROR
	}

	// 启动服务器
	if err = core.StartServer(); err != nil {
		// 如果启动服务器失败，跳转到错误处理
		goto ERROR
	}

	// 程序正常退出
	os.Exit(0)

ERROR:
	// 发生错误时，输出错误信息并退出程序
	fmt.Println(err)
	os.Exit(-1)
}

/*
	测试命令：
		curl http://localhost:8880/alloc?biz_tag=test
		curl http://localhost:8880/health?biz_tag=test
*/
