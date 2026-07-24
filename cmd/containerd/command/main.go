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

package command

import (
	gocontext "context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	"github.com/containerd/containerd/defaults"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/services/server"
	srvconfig "github.com/containerd/containerd/services/server/config"
	"github.com/containerd/containerd/sys"
	"github.com/containerd/containerd/version"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"google.golang.org/grpc/grpclog"
)

const usage = `
                    __        _                     __
  _________  ____  / /_____ _(_)___  ___  _________/ /
 / ___/ __ \/ __ \/ __/ __ ` + "`" + `/ / __ \/ _ \/ ___/ __  /
/ /__/ /_/ / / / / /_/ /_/ / / / / /  __/ /  / /_/ /
\___/\____/_/ /_/\__/\__,_/_/_/ /_/\___/_/   \__,_/

high performance container runtime
`

func init() {
	// Discard grpc logs so that they don't mess with our stdio
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(ioutil.Discard, ioutil.Discard, ioutil.Discard))

	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Println(c.App.Name, version.Package, c.App.Version, version.Revision)
	}
}

// App 创建 containerd 的 CLI 应用
// wy: containerd daemon 的启动入口，通过 urfave/cli 框架实现
// 默认 action 是启动 daemon（前台运行），子命令包括:
//   - config: 生成/查看默认配置
//   - publish: 发布事件（被 shim 调用）
//   - oci-hook: OCI 运行时 hook 支持
func App() *cli.App {
	app := cli.NewApp()
	app.Name = "containerd"
	app.Version = version.Version
	app.Usage = usage
	app.Description = `
containerd is a high performance container runtime whose daemon can be started
by using this command. If none of the *config*, *publish*, or *help* commands
are specified, the default action of the **containerd** command is to start the
containerd daemon in the foreground.


A default configuration is used if no TOML configuration is specified or located
at the default file location. The *containerd config* command can be used to
generate the default configuration for containerd. The output of that command
can be used and modified as necessary as a custom configuration.`
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config,c",
			Usage: "path to the configuration file",
			Value: filepath.Join(defaults.DefaultConfigDir, "config.toml"), // wangyang 这里会有一个默认的配置文件
		},
		cli.StringFlag{
			Name:  "log-level,l",
			Usage: "set the logging level [trace, debug, info, warn, error, fatal, panic]",
		},
		cli.StringFlag{
			Name:  "address,a",
			Usage: "address for containerd's GRPC server",
		},
		cli.StringFlag{
			Name:  "root",
			Usage: "containerd root directory",
		},
		cli.StringFlag{
			Name:  "state",
			Usage: "containerd state directory",
		},
	}
	app.Flags = append(app.Flags, serviceFlags()...)
	app.Commands = []cli.Command{
		configCommand,
		publishCommand,
		ociHook,
	}
	// wy: 🚀 默认 Action: 启动 containerd daemon（前台运行）
	app.Action = func(context *cli.Context) error {
		var (
			start   = time.Now()
			signals = make(chan os.Signal, 2048) // wy: 大容量信号缓冲，防止信号丢失
			serverC = make(chan *server.Server, 1)
			ctx     = gocontext.Background()
			config  = defaultConfig()
		)

		// wy: Step 1: 加载 TOML 配置文件
		// 默认路径: /etc/containerd/config.toml
		// 配置文件控制: gRPC 地址、插件启用/禁用、各插件的详细参数
		configPath := context.GlobalString("config")
		_, err := os.Stat(configPath)
		if !os.IsNotExist(err) || context.GlobalIsSet("config") {
			if err := srvconfig.LoadConfig(configPath, config); err != nil {
				return err
			}
		}

		// wy: Step 2: 命令行 flag 覆盖配置文件中的值（flag 优先级更高）
		if err := applyFlags(context, config); err != nil {
			return err
		}

		// wy: Step 3: 创建顶层目录
		//   Root:  /var/lib/containerd  (持久化: content blobs, meta.db, snapshots)
		//   State: /run/containerd      (运行时: shim socket, bundle, 临时挂载)
		if err := server.CreateTopLevelDirectories(config); err != nil {
			return err
		}

		// Stop if we are registering or unregistering against Windows SCM.
		stop, err := registerUnregisterService(config.Root)
		if err != nil {
			logrus.Fatal(err)
		}
		if stop {
			return nil
		}

		// wy: Step 4: 启动信号处理器（尽早启动，确保不丢失 boot 过程中的信号）
		// SIGTERM/SIGINT → 优雅退出
		// SIGUSR1 → 打印所有 goroutine 栈（调试用）
		done := handleSignals(ctx, signals, serverC)
		signal.Notify(signals, handledSignals...)

		// wy: Step 5: 清理上次异常退出遗留的临时挂载点
		// 场景: daemon 崩溃时，overlayfs 的临时挂载可能未被卸载
		if err := mount.SetTempMountLocation(filepath.Join(config.Root, "tmpmounts")); err != nil {
			return errors.Wrap(err, "creating temp mount location")
		}
		warnings, err := mount.CleanupTempMounts(0)
		if err != nil {
			log.G(ctx).WithError(err).Error("unmounting temp mounts")
		}
		for _, w := range warnings {
			log.G(ctx).WithError(w).Warn("cleanup temp mount")
		}

		// wy: Step 6: 验证并补全监听地址
		if config.GRPC.Address == "" {
			return errors.Wrap(errdefs.ErrInvalidArgument, "grpc address cannot be empty")
		}
		if config.TTRPC.Address == "" {
			// wy: TTRPC 地址默认基于 gRPC 地址生成（加 .ttrpc 后缀）
			// 例如: /run/containerd/containerd.sock → /run/containerd/containerd.sock.ttrpc
			config.TTRPC.Address = fmt.Sprintf("%s.ttrpc", config.GRPC.Address)
			config.TTRPC.UID = config.GRPC.UID
			config.TTRPC.GID = config.GRPC.GID
		}
		log.G(ctx).WithFields(logrus.Fields{
			"version":  version.Version,
			"revision": version.Revision,
		}).Info("starting containerd")

		// wy: Step 7: 🚀 创建 Server 实例（核心！内部加载并初始化所有 Plugin）
		server, err := server.New(ctx, config)
		if err != nil {
			return err
		}

		// Launch as a Windows Service if necessary
		if err := launchService(server, done); err != nil {
			logrus.Fatal(err)
		}

		serverC <- server

		// wy: Step 8: 启动各种监听端口
		// 每个 serve() 都在独立的 goroutine 中运行

		// wy: 8a. Debug Server（pprof + expvar）
		// 提供性能分析和运行时变量查看: /debug/pprof/, /debug/vars
		if config.Debug.Address != "" {
			var l net.Listener
			if isLocalAddress(config.Debug.Address) {
				if l, err = sys.GetLocalListener(config.Debug.Address, config.Debug.UID, config.Debug.GID); err != nil {
					return errors.Wrapf(err, "failed to get listener for debug endpoint")
				}
			} else {
				if l, err = net.Listen("tcp", config.Debug.Address); err != nil {
					return errors.Wrapf(err, "failed to get listener for debug endpoint")
				}
			}
			serve(ctx, l, server.ServeDebug)
		}

		// wy: 8b. Metrics Server（Prometheus 指标端点）
		if config.Metrics.Address != "" {
			l, err := net.Listen("tcp", config.Metrics.Address)
			if err != nil {
				return errors.Wrapf(err, "failed to get listener for metrics endpoint")
			}
			serve(ctx, l, server.ServeMetrics)
		}

		// wy: 8c. 🚀 TTRPC Server（给 Shim 进程用的轻量 RPC 端点）
		// 默认地址: /run/containerd/containerd.sock.ttrpc
		// Shim 进程通过此端口向 Daemon 发布事件
		tl, err := sys.GetLocalListener(config.TTRPC.Address, config.TTRPC.UID, config.TTRPC.GID)
		if err != nil {
			return errors.Wrapf(err, "failed to get listener for main ttrpc endpoint")
		}
		serve(ctx, tl, server.ServeTTRPC)

		// wy: 8d. TCP gRPC Server（可选的远程访问端点，需配置 TLS）
		if config.GRPC.TCPAddress != "" {
			l, err := net.Listen("tcp", config.GRPC.TCPAddress)
			if err != nil {
				return errors.Wrapf(err, "failed to get listener for TCP grpc endpoint")
			}
			serve(ctx, l, server.ServeTCP)
		}

		// wy: 8e. 🚀 主 gRPC Server（Client 端连接的主要端点）
		// 默认地址: /run/containerd/containerd.sock（Unix Domain Socket）
		// ctr、Docker、Kubernetes kubelet 都通过此端口与 containerd 通信
		l, err := sys.GetLocalListener(config.GRPC.Address, config.GRPC.UID, config.GRPC.GID)
		if err != nil {
			return errors.Wrapf(err, "failed to get listener for main endpoint")
		}
		serve(ctx, l, server.ServeGRPC)

		// wy: 通知 systemd 等服务：daemon 已就绪
		if err := notifyReady(ctx); err != nil {
			log.G(ctx).WithError(err).Warn("notify ready failed")
		}

		log.G(ctx).Infof("containerd successfully booted in %fs", time.Since(start).Seconds())
		<-done // wy: 阻塞等待退出信号（SIGTERM/SIGINT）
		return nil
	}
	return app
}

func serve(ctx gocontext.Context, l net.Listener, serveFunc func(net.Listener) error) {
	path := l.Addr().String()
	log.G(ctx).WithField("address", path).Info("serving...")
	go func() { // 单独在一个协程里面执行
		defer l.Close()
		if err := serveFunc(l); err != nil {
			log.G(ctx).WithError(err).WithField("address", path).Fatal("serve failure")
		}
	}()
}

func applyFlags(context *cli.Context, config *srvconfig.Config) error {
	// the order for config vs flag values is that flags will always override
	// the config values if they are set
	if err := setLogLevel(context, config); err != nil {
		return err
	}
	if err := setLogFormat(config); err != nil {
		return err
	}
	for _, v := range []struct {
		name string
		d    *string
	}{
		{
			name: "root",
			d:    &config.Root,
		},
		{
			name: "state",
			d:    &config.State,
		},
		{
			name: "address",
			d:    &config.GRPC.Address, // wangyang获取相应的变量地址
		},
	} {
		if s := context.GlobalString(v.name); s != "" {
			*v.d = s
		}
	}

	applyPlatformFlags(context)

	return nil
}

func setLogLevel(context *cli.Context, config *srvconfig.Config) error {
	l := context.GlobalString("log-level")
	if l == "" {
		l = config.Debug.Level
	}
	if l != "" {
		lvl, err := logrus.ParseLevel(l)
		if err != nil {
			return err
		}
		logrus.SetLevel(lvl)
	}
	return nil
}

func setLogFormat(config *srvconfig.Config) error {
	f := config.Debug.Format
	if f == "" {
		f = log.TextFormat
	}

	switch f {
	case log.TextFormat:
		logrus.SetFormatter(&logrus.TextFormatter{
			TimestampFormat: log.RFC3339NanoFixed,
			FullTimestamp:   true,
		})
	case log.JSONFormat:
		logrus.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: log.RFC3339NanoFixed,
		})
	default:
		return errors.Errorf("unknown log format: %s", f)
	}

	return nil
}

func dumpStacks(writeToFile bool) {
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
	logrus.Infof("=== BEGIN goroutine stack dump ===\n%s\n=== END goroutine stack dump ===", buf)

	if writeToFile {
		// Also write to file to aid gathering diagnostics
		name := filepath.Join(os.TempDir(), fmt.Sprintf("containerd.%d.stacks.log", os.Getpid()))
		f, err := os.Create(name)
		if err != nil {
			return
		}
		defer f.Close()
		f.WriteString(string(buf))
		logrus.Infof("goroutine stack dump written to %s", name)
	}
}
