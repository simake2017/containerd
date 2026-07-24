/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package shim

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/containerd/containerd/events"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/namespaces"
	shimapi "github.com/containerd/containerd/runtime/v2/task"
	"github.com/containerd/containerd/version"
	"github.com/containerd/ttrpc"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Client for a shim server
type Client struct {
	service shimapi.TaskService
	context context.Context
	signals chan os.Signal
}

// Publisher for events
type Publisher interface {
	events.Publisher
	io.Closer
}

// StartOpts describes shim start configuration received from containerd
type StartOpts struct {
	ID               string
	ContainerdBinary string
	Address          string
	TTRPCAddress     string
}

// Init func for the creation of a shim server
type Init func(context.Context, string, Publisher, func()) (Shim, error)

// Shim 是 shim 进程内部的服务接口
// wy: 🚀 它组合了三个能力:
//   1. shimapi.TaskService: 标准 Task RPC（Create/Start/Kill/Delete/Exec 等）
//   2. Cleanup: 清理已死容器（daemon 调用 containerd-shim-runc-v2 delete）
//   3. StartShim: 启动 shim server（daemon 调用 containerd-shim-runc-v2 start）
//
// 默认实现: runtime/v2/runc/v2.service（使用 runc 作为 OCI runtime）
type Shim interface {
	shimapi.TaskService
	Cleanup(ctx context.Context) (*shimapi.DeleteResponse, error)
	StartShim(ctx context.Context, opts StartOpts) (string, error)
}

// OptsKey is the context key for the Opts value.
type OptsKey struct{}

// Opts are context options associated with the shim invocation.
type Opts struct {
	BundlePath string
	Debug      bool
}

// BinaryOpts allows the configuration of a shims binary setup
type BinaryOpts func(*Config)

// Config of shim binary options provided by shim implementations
type Config struct {
	// NoSubreaper disables setting the shim as a child subreaper
	NoSubreaper bool
	// NoReaper disables the shim binary from reaping any child process implicitly
	NoReaper bool
	// NoSetupLogger disables automatic configuration of logrus to use the shim FIFO
	NoSetupLogger bool
}

var (
	debugFlag            bool
	versionFlag          bool
	idFlag               string
	namespaceFlag        string
	socketFlag           string
	bundlePath           string
	addressFlag          string
	containerdBinaryFlag string
	action               string
)

const (
	ttrpcAddressEnv = "TTRPC_ADDRESS"
)

func parseFlags() {
	flag.BoolVar(&debugFlag, "debug", false, "enable debug output in logs")
	flag.BoolVar(&versionFlag, "v", false, "show the shim version and exit")
	flag.StringVar(&namespaceFlag, "namespace", "", "namespace that owns the shim")
	flag.StringVar(&idFlag, "id", "", "id of the task")
	flag.StringVar(&socketFlag, "socket", "", "socket path to serve")
	flag.StringVar(&bundlePath, "bundle", "", "path to the bundle if not workdir")

	flag.StringVar(&addressFlag, "address", "", "grpc address back to main containerd")
	flag.StringVar(&containerdBinaryFlag, "publish-binary", "containerd", "path to publish binary (used for publishing events)")

	flag.Parse()
	action = flag.Arg(0) // action 是第一个参数
}

func setRuntime() {
	debug.SetGCPercent(40)
	go func() {
		for range time.Tick(30 * time.Second) {
			debug.FreeOSMemory()
		}
	}()
	if os.Getenv("GOMAXPROCS") == "" {
		// If GOMAXPROCS hasn't been set, we default to a value of 2 to reduce
		// the number of Go stacks present in the shim.
		runtime.GOMAXPROCS(2)
	}
}

func setLogger(ctx context.Context, id string) error {
	logrus.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: log.RFC3339NanoFixed,
		FullTimestamp:   true,
	})
	if debugFlag {
		logrus.SetLevel(logrus.DebugLevel)
	}
	f, err := openLog(ctx, id)
	if err != nil {
		return err
	}
	logrus.SetOutput(f)
	return nil
}

// Run initializes and runs a shim server
func Run(id string, initFunc Init, opts ...BinaryOpts) {
	var config Config
	for _, o := range opts {
		o(&config)
	}
	if err := run(id, initFunc, config); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", id, err)
		os.Exit(1)
	}
}

// run 是 shim 进程的实际入口
// wy: 🚀 shim 进程支持三种 action 模式:
//
//   1. "start" — daemon 调用 shim 启动新实例
//      → 创建 TTRPC socket → fork 自身 → 父进程打印 socket 地址到 stdout → 退出
//      → daemon 从 stdout 读取地址，建立 TTRPC 连接
//
//   2. "delete" — daemon 调用 shim 清理死容器
//      → 调用 service.Cleanup() → runc delete → 输出 DeleteResponse 到 stdout
//
//   3. 默认（无 action） — 作为 shim server 运行
//      → 创建 TTRPC server → 注册 TaskService → 在 socket 上监听
//      → 处理 daemon 发来的 Create/Start/Kill/Delete/Exec 等 RPC
func run(id string, initFunc Init, config Config) error {
	parseFlags()
	if versionFlag {
		fmt.Printf("%s:\n", os.Args[0])
		fmt.Println("  Version: ", version.Version)
		fmt.Println("  Revision:", version.Revision)
		fmt.Println("  Go version:", version.GoVersion)
		fmt.Println("")
		return nil
	}

	if namespaceFlag == "" {
		return fmt.Errorf("shim namespace cannot be empty")
	}

	// wy: 设置运行时参数: GC 百分比=40%, GOMAXPROCS=2, 定期释放 OS 内存
	// 目的: 减少 shim 进程的内存占用和 CPU 占用（shim 是长驻进程）
	setRuntime()

	signals, err := setupSignals(config)
	if err != nil {
		return err
	}

	// wy: 🚀 核心底层交互: 将 shim 进程设置为 subreaper
	// subreaper 的作用: 当容器进程成为孤儿进程时，它会被 shim 收养（而非 init/systemd）
	// 这确保 shim 能通过 wait4() 收集到所有容器子进程的退出状态
	if !config.NoSubreaper {
		if err := subreaper(); err != nil {
			return err
		}
	}

	// wy: 创建事件发布器——通过 TTRPC 连接到 containerd daemon 的事件服务
	ttrpcAddress := os.Getenv(ttrpcAddressEnv)
	publisher, err := NewPublisher(ttrpcAddress)
	if err != nil {
		return err
	}
	defer publisher.Close()

	// wy: 设置 context（注入 namespace 和 bundle 路径）
	ctx := namespaces.WithNamespace(context.Background(), namespaceFlag)
	ctx = context.WithValue(ctx, OptsKey{}, Opts{BundlePath: bundlePath, Debug: debugFlag})
	ctx = log.WithLogger(ctx, log.G(ctx).WithField("runtime", id))
	ctx, cancel := context.WithCancel(ctx)

	// wy: 调用具体 runtime 的初始化函数（如 runc/v2.New）
	service, err := initFunc(ctx, idFlag, publisher, cancel)
	if err != nil {
		return err
	}

	// wy: 🚀 根据 action 参数选择不同的运行模式
	switch action {
	case "delete":
		// wy: 清理模式: daemon 发现 shim 异常退出后，调用 shim delete 清理残留容器
		logger := logrus.WithFields(logrus.Fields{
			"pid":       os.Getpid(),
			"namespace": namespaceFlag,
		})
		go handleSignals(ctx, logger, signals)
		response, err := service.Cleanup(ctx) // wy: 内部: runc delete --force
		if err != nil {
			return err
		}
		data, err := proto.Marshal(response)
		if err != nil {
			return err
		}
		// wy: 将清理结果写入 stdout，daemon 从 stdout 读取
		if _, err := os.Stdout.Write(data); err != nil {
			return err
		}
		return nil

	case "start":
		// wy: 启动模式: daemon fork 新的 shim 进程
		// StartShim 内部: 创建 socket → fork 自身为 daemon → 返回 socket 地址
		opts := StartOpts{
			ID:               idFlag,
			ContainerdBinary: containerdBinaryFlag,
			Address:          addressFlag,
			TTRPCAddress:     ttrpcAddress,
		}
		address, err := service.StartShim(ctx, opts)
		if err != nil {
			return err
		}
		// wy: 🚀 将 TTRPC socket 地址打印到 stdout
		// daemon 端通过读取子进程 stdout 获取此地址
		if _, err := os.Stdout.WriteString(address); err != nil {
			return err
		}
		return nil

	default:
		// wy: 默认模式: 作为 shim server 运行（这是 fork 后的子进程）
		if !config.NoSetupLogger {
			if err := setLogger(ctx, idFlag); err != nil {
				return err
			}
		}
		// wy: 🚀 创建 shim client 并启动 TTRPC server
		client := NewShimClient(ctx, service, signals)
		if err := client.Serve(); err != nil {
			if err != context.Canceled {
				return err
			}
		}

		// wy: 清理: 如果 shim 异常退出（如 OOM killer），删除遗留的 socket 文件
		if address, err := ReadAddress("address"); err == nil {
			_ = RemoveSocket(address)
		}

		// wy: 等待事件发布器完成所有待发送事件的投递
		select {
		case <-publisher.Done():
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("publisher not closed")
		}
	}
}

// NewShimClient creates a new shim server client
func NewShimClient(ctx context.Context, svc shimapi.TaskService, signals chan os.Signal) *Client {
	s := &Client{
		service: svc,
		context: ctx,
		signals: signals,
	}
	return s
}

// Serve 启动 shim 的 TTRPC server 并阻塞等待
// wy: 🚀 这是 shim server 的主循环:
//   1. 创建 TTRPC server
//   2. 注册 TaskService（Create/Start/Kill/Delete/Exec/State/Wait 等 RPC）
//   3. 在 Unix socket 上监听
//   4. 处理信号（SIGTERM → 优雅退出，SIGUSR1 → dump stacks）
func (s *Client) Serve() error {
	dump := make(chan os.Signal, 32)
	setupDumpStacks(dump) // wy: SIGUSR1 信号触发 goroutine 栈 dump（调试用）

	path, err := os.Getwd()
	if err != nil {
		return err
	}
	server, err := newServer()
	if err != nil {
		return errors.Wrap(err, "failed creating server")
	}

	// wy: 🚀 核心: 将 TaskService 注册到 TTRPC server
	// 此后 daemon 可以通过 TTRPC 调用:
	//   TaskService.Create → shim 创建容器
	//   TaskService.Start  → shim 启动容器
	//   TaskService.Kill   → shim 向容器发信号
	//   TaskService.Delete → shim 删除容器
	//   TaskService.Exec   → shim 在容器内创建新进程
	//   TaskService.State  → shim 查询容器状态
	//   TaskService.Wait   → shim 等待容器退出
	logrus.Debug("registering ttrpc server")
	shimapi.RegisterTaskService(server, s.service)

	// wy: 在 Unix socket 上启动 TTRPC server
	if err := serve(s.context, server, socketFlag); err != nil {
		return err
	}
	logger := logrus.WithFields(logrus.Fields{
		"pid":       os.Getpid(),
		"path":      path,
		"namespace": namespaceFlag,
	})
	go func() {
		for range dump {
			dumpStacks(logger)
		}
	}()
	// wy: 阻塞在信号处理上（直到收到退出信号）
	return handleSignals(s.context, logger, s.signals)
}

// serve serves the ttrpc API over a unix socket at the provided path
// this function does not block
func serve(ctx context.Context, server *ttrpc.Server, path string) error {
	l, err := serveListener(path)
	if err != nil {
		return err
	}
	go func() {
		defer l.Close()
		if err := server.Serve(ctx, l); err != nil &&
			!strings.Contains(err.Error(), "use of closed network connection") {
			logrus.WithError(err).Fatal("containerd-shim: ttrpc server failure")
		}
	}()
	return nil
}

func dumpStacks(logger *logrus.Entry) {
	var (
		buf       []byte
		stackSize int
	)
	bufferLen := 16384
	for stackSize == len(buf) {
		buf = make([]byte, bufferLen)
		stackSize = runtime.Stack(buf, true)
		bufferLen *= 2
	}
	buf = buf[:stackSize]
	logger.Infof("=== BEGIN goroutine stack dump ===\n%s\n=== END goroutine stack dump ===", buf)
}
