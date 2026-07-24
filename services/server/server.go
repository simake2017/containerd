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

package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"expvar"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	csapi "github.com/containerd/containerd/api/services/content/v1"
	ssapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/content/local"
	csproxy "github.com/containerd/containerd/content/proxy"
	"github.com/containerd/containerd/defaults"
	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/events/exchange"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/metadata"
	"github.com/containerd/containerd/pkg/dialer"
	"github.com/containerd/containerd/pkg/timeout"
	"github.com/containerd/containerd/plugin"
	srvconfig "github.com/containerd/containerd/services/server/config"
	"github.com/containerd/containerd/snapshots"
	ssproxy "github.com/containerd/containerd/snapshots/proxy"
	"github.com/containerd/containerd/sys"
	"github.com/containerd/ttrpc"
	metrics "github.com/docker/go-metrics"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
)

// CreateTopLevelDirectories 创建 containerd 的两个顶层目录
// wy: 🚀 两个目录的用途：
//   Root (/var/lib/containerd):
//     存放持久化数据——Content Store blobs、meta.db、snapshot 的 committed 层
//     即使容器全部删除，这些数据也应该保留
//   State (/run/containerd):
//     存放运行时状态——shim 的 TTRPC socket、OCI bundle 目录、临时挂载点
//     /run 通常是 tmpfs，重启后自动清除
//
// 两者必须分开：持久化数据不能放在 tmpfs 上，运行时状态不适合放在慢速磁盘上
func CreateTopLevelDirectories(config *srvconfig.Config) error {
	switch {
	case config.Root == "":
		return errors.New("root must be specified")
	case config.State == "":
		return errors.New("state must be specified")
	case config.Root == config.State:
		return errors.New("root and state must be different paths")
	}

	// wy: 🚀 核心底层交互: 创建目录并设置 ACL 权限
	// 0711 权限 = 所有者 rwx + 其他 x（允许其他用户进入目录但不能列出内容）
	if err := sys.MkdirAllWithACL(config.Root, 0711); err != nil {
		return err
	}

	return sys.MkdirAllWithACL(config.State, 0711)
}

// New 创建并初始化 containerd Server 实例
// wy: 🚀 这是 containerd daemon 启动的核心函数，完成以下关键步骤：
//   1. 加载并排序所有 Plugin（LoadPlugins）
//   2. 创建 gRPC Server 和 TTRPC Server
//   3. 按拓扑序逐个初始化 Plugin（调用 InitFn）
//   4. 收集实现了 Service/TTRPCService/TCPService 接口的插件实例
//   5. 将服务注册到对应的 Server 上
//
// 调用链: main() → command.App().Action → server.New()
func New(ctx context.Context, config *srvconfig.Config) (*Server, error) {
	if err := apply(ctx, config); err != nil {
		return nil, err
	}
	// wy: 解析配置文件中的超时设置（如 task 状态查询超时默认 2s）
	for key, sec := range config.Timeouts {
		d, err := time.ParseDuration(sec)
		if err != nil {
			return nil, errors.Errorf("unable to parse %s into a time duration", sec)
		}
		timeout.Set(key, d)
	}

	// wy: 🚀 Step 1: 加载所有 Plugin（编译期注册 + 动态 .so + 内置 content/metadata）
	// 返回的是经过拓扑排序的有序 Registration 列表
	plugins, err := LoadPlugins(ctx, config)
	if err != nil {
		return nil, err
	}

	// wy: 注册外部 Stream Processor（用于自定义 diff 处理方式）
	for id, p := range config.StreamProcessors {
		diff.RegisterProcessor(diff.BinaryHandler(id, p.Returns, p.Accepts, p.Path, p.Args, p.Env))
	}

	// wy: 🚀 Step 2: 创建 gRPC Server，配置 Prometheus 指标拦截器
	serverOpts := []grpc.ServerOption{
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
	}
	if config.GRPC.MaxRecvMsgSize > 0 {
		serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(config.GRPC.MaxRecvMsgSize))
	}
	if config.GRPC.MaxSendMsgSize > 0 {
		serverOpts = append(serverOpts, grpc.MaxSendMsgSize(config.GRPC.MaxSendMsgSize))
	}

	// wy: 创建 TTRPC Server——用于 Daemon ↔ Shim 之间的轻量通信
	ttrpcServer, err := newTTRPCServer()
	if err != nil {
		return nil, err
	}
	tcpServerOpts := serverOpts
	if config.GRPC.TCPTLSCert != "" {
		log.G(ctx).Info("setting up tls on tcp GRPC services...")

		tlsCert, err := tls.LoadX509KeyPair(config.GRPC.TCPTLSCert, config.GRPC.TCPTLSKey)
		if err != nil {
			return nil, err
		}
		tlsConfig := &tls.Config{Certificates: []tls.Certificate{tlsCert}}

		if config.GRPC.TCPTLSCA != "" {
			caCertPool := x509.NewCertPool()
			caCert, err := ioutil.ReadFile(config.GRPC.TCPTLSCA)
			if err != nil {
				return nil, errors.Wrap(err, "failed to load CA file")
			}
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.ClientCAs = caCertPool
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}

		tcpServerOpts = append(tcpServerOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	var (
		// wy: 🚀 三种 Server:
		//   grpcServer: Unix Socket gRPC —— Client (ctr/docker/kubelet) 连接这个
		//   ttrpcServer: TTRPC —— Daemon 与 Shim 之间通信（更轻量）
		//   tcpServer: TCP gRPC —— 远程访问（可选，需配置 TLS）
		grpcServer = grpc.NewServer(serverOpts...)
		tcpServer  = grpc.NewServer(tcpServerOpts...)

		// wy: 收集三类服务实例，初始化完所有插件后再统一注册
		grpcServices  []plugin.Service
		tcpServices   []plugin.TCPService
		ttrpcServices []plugin.TTRPCService

		s = &Server{
			grpcServer:  grpcServer,
			tcpServer:   tcpServer,
			ttrpcServer: ttrpcServer,
			events:      exchange.NewExchange(), // wy: 创建全局事件总线（发布/订阅模式）
			config:      config,
		}
		initialized = plugin.NewPluginSet() // wy: 已初始化的插件集合，供后续插件通过 ic.Get() 查找依赖
		required    = make(map[string]struct{})
	)
	// wy: 从配置文件中标记哪些插件是必须的（required），失败时 daemon 直接退出
	for _, r := range config.RequiredPlugins {
		required[r] = struct{}{}
	}

	// wy: 🚀 Step 3: 按拓扑序逐个初始化 Plugin
	// plugins 列表已经过 Graph() 拓扑排序，保证被依赖者先初始化
	for _, p := range plugins {
		id := p.URI() // wy: 格式: "<Type>.<ID>"，如 "io.containerd.snapshotter.v1.overlayfs"
		reqID := id
		if config.GetVersion() == 1 {
			reqID = p.ID
		}
		log.G(ctx).WithField("type", p.Type).Infof("loading plugin %q...", id)

		// wy: 创建 InitContext，包含:
		//   - Root/State 目录路径（每个插件有自己的子目录）
		//   - 已初始化的插件集合（initialized）——插件可通过 ic.Get(Type) 获取依赖
		//   - Events（全局事件总线）
		//   - Address（gRPC/TTRPC 监听地址）
		initContext := plugin.NewContext(
			ctx,
			p,
			initialized,
			config.Root,
			config.State,
		)
		initContext.Events = s.events
		initContext.Address = config.GRPC.Address
		initContext.TTRPCAddress = config.TTRPC.Address

		// wy: 从 TOML 配置中解码该插件专属的配置段
		// 例如 [plugins."io.containerd.snapshotter.v1.overlayfs"] 下的配置
		if p.Config != nil {
			pc, err := config.Decode(p)
			if err != nil {
				return nil, err
			}
			initContext.Config = pc
		}

		// wy: 🚀 核心——调用插件的 InitFn，真正创建插件实例
		// 此时 InitFn 内部可以:
		//   - ic.Get(plugin.ContentPlugin) → 获取已初始化的 Content Store
		//   - ic.Get(plugin.MetadataPlugin) → 获取已初始化的 BoltDB
		//   - ic.GetByType(plugin.SnapshotPlugin) → 获取所有 Snapshotter
		result := p.Init(initContext)
		if err := initialized.Add(result); err != nil {
			return nil, errors.Wrapf(err, "could not add plugin result to plugin set")
		}

		// wy: 获取插件实例，检查初始化是否成功
		instance, err := result.Instance()
		if err != nil {
			if plugin.IsSkipPlugin(err) {
				// wy: SkipPlugin 表示插件主动跳过加载（如环境不支持）
				log.G(ctx).WithError(err).WithField("type", p.Type).Infof("skip loading plugin %q...", id)
			} else {
				log.G(ctx).WithError(err).Warnf("failed to load plugin %s", id)
			}
			// wy: 如果是 required 插件失败，daemon 直接退出
			if _, ok := required[reqID]; ok {
				return nil, errors.Wrapf(err, "load required plugin %s", id)
			}
			continue
		}

		delete(required, reqID)

		// wy: 🚀 Step 4: 检查插件实例实现了哪些服务接口，分类收集
		// 一个插件可以同时实现多个接口（如既是 gRPC Service 又是 TTRPC Service）
		if src, ok := instance.(plugin.Service); ok {
			grpcServices = append(grpcServices, src) // wy: 将被注册到 Unix Socket gRPC Server
		}
		if src, ok := instance.(plugin.TTRPCService); ok {
			ttrpcServices = append(ttrpcServices, src) // wy: 将被注册到 TTRPC Server
		}
		if service, ok := instance.(plugin.TCPService); ok {
			tcpServices = append(tcpServices, service) // wy: 将被注册到 TCP gRPC Server
		}

		s.plugins = append(s.plugins, result)
	}
	if len(required) != 0 {
		var missing []string
		for id := range required {
			missing = append(missing, id)
		}
		return nil, errors.Errorf("required plugin %s not included", missing)
	}

	// wy: 🚀 Step 5: 所有插件初始化完毕后，统一注册服务到对应的 Server
	// 之所以延迟注册而非在初始化时就注册，是因为 gRPC 服务的 handler 可能依赖
	// 多个已初始化的插件实例，必须等全部就绪后才能工作
	for _, service := range grpcServices {
		if err := service.Register(grpcServer); err != nil {
			return nil, err
		}
	}
	for _, service := range ttrpcServices {
		if err := service.RegisterTTRPC(ttrpcServer); err != nil {
			return nil, err
		}
	}
	for _, service := range tcpServices {
		if err := service.RegisterTCP(tcpServer); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Server 是 containerd 的主守护进程对象
// wy: 它持有三种 Server（gRPC/TTRPC/TCP）和全局事件总线
// 生命周期由 command/main.go 中的 app.Action 管理
type Server struct {
	grpcServer  *grpc.Server         // wy: Unix Socket gRPC Server —— Client 端连接此
	ttrpcServer *ttrpc.Server        // wy: TTRPC Server —— Shim 进程连接此
	tcpServer   *grpc.Server         // wy: TCP gRPC Server —— 远程访问（可选）
	events      *exchange.Exchange   // wy: 全局事件总线（发布/订阅模式，所有事件流经此处）
	config      *srvconfig.Config    // wy: daemon 配置
	plugins     []*plugin.Plugin     // wy: 所有已初始化的插件（按拓扑序排列，Stop 时逆序关闭）
}

// ServeGRPC provides the containerd grpc APIs on the provided listener
func (s *Server) ServeGRPC(l net.Listener) error {
	if s.config.Metrics.GRPCHistogram {
		// enable grpc time histograms to measure rpc latencies
		grpc_prometheus.EnableHandlingTimeHistogram()
	}
	// before we start serving the grpc API register the grpc_prometheus metrics
	// handler.  This needs to be the last service registered so that it can collect
	// metrics for every other service
	grpc_prometheus.Register(s.grpcServer)
	return trapClosedConnErr(s.grpcServer.Serve(l))
}

// ServeTTRPC provides the containerd ttrpc APIs on the provided listener
func (s *Server) ServeTTRPC(l net.Listener) error {
	return trapClosedConnErr(s.ttrpcServer.Serve(context.Background(), l))
}

// ServeMetrics provides a prometheus endpoint for exposing metrics
func (s *Server) ServeMetrics(l net.Listener) error {
	m := http.NewServeMux()
	m.Handle("/v1/metrics", metrics.Handler())
	return trapClosedConnErr(http.Serve(l, m))
}

// ServeTCP allows services to serve over tcp
func (s *Server) ServeTCP(l net.Listener) error {
	grpc_prometheus.Register(s.tcpServer)
	return trapClosedConnErr(s.tcpServer.Serve(l))
}

// ServeDebug provides a debug endpoint
func (s *Server) ServeDebug(l net.Listener) error {
	// don't use the default http server mux to make sure nothing gets registered
	// that we don't want to expose via containerd
	m := http.NewServeMux()
	m.Handle("/debug/vars", expvar.Handler())
	m.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	m.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	m.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	m.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	m.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	return trapClosedConnErr(http.Serve(l, m))
}

// Stop the containerd server canceling any open connections
func (s *Server) Stop() {
	s.grpcServer.Stop()
	for i := len(s.plugins) - 1; i >= 0; i-- {
		p := s.plugins[i]
		instance, err := p.Instance()
		if err != nil {
			log.L.WithError(err).WithField("id", p.Registration.URI()).
				Errorf("could not get plugin instance")
			continue
		}
		closer, ok := instance.(io.Closer)
		if !ok {
			continue
		}
		if err := closer.Close(); err != nil {
			log.L.WithError(err).WithField("id", p.Registration.URI()).
				Errorf("failed to close plugin")
		}
	}
}

// LoadPlugins 加载所有插件并返回拓扑排序后的有序列表
// wy: 🚀 三个阶段的插件来源:
//   1. 动态 .so 插件: plugin.Load(path) 从磁盘加载编译好的 Go 插件
//   2. 编译期内置插件: 通过 blank import 在 init() 中自动注册（如 snapshotter、runtime）
//   3. Server 层直接注册: Content Plugin 和 Metadata Plugin 在此函数中注册
//
// 返回的有序列表会被 Server.New() 按序初始化
func LoadPlugins(ctx context.Context, config *srvconfig.Config) ([]*plugin.Registration, error) {
	// wy: Step 1: 加载动态 .so 插件（如第三方自定义的 snapshotter）
	path := config.PluginDir
	if path == "" {
		path = filepath.Join(config.Root, "plugins") // wy: 默认: /var/lib/containerd/plugins/
	}
	if err := plugin.Load(path); err != nil {
		return nil, err
	}

	// wy: Step 2: 🚀 注册 Content Plugin（内容存储）
	// 这是唯一的本地 Content Store 实现，基于文件系统
	// blob 存储路径: <Root>/io.containerd.content.v1.content/blobs/sha256/<digest>
	// 所有镜像的 layer/config/manifest 数据最终都写入这里
	plugin.Register(&plugin.Registration{
		Type: plugin.ContentPlugin,
		ID:   "content",
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			ic.Meta.Exports["root"] = ic.Root
			return local.NewStore(ic.Root) // wy: 创建本地文件系统 CAS Store
		},
	})
	// wy: Step 3: 🚀 注册 Metadata Plugin（BoltDB 元数据引擎）
	// 这是 containerd 的数据中枢——所有上层 Service（Image/Container/Task/Snapshot）
	// 的元数据都存储在同一个 BoltDB 文件 (meta.db) 中
	//
	// 依赖关系:
	//   - ContentPlugin: 提供 blob 存储能力（metadata 中的 content 引用指向 content store）
	//   - SnapshotPlugin: 提供快照管理能力（metadata 中的 snapshot 索引对应实际的快照数据）
	plugin.Register(&plugin.Registration{
		Type: plugin.MetadataPlugin,
		ID:   "bolt",
		Requires: []plugin.Type{
			plugin.ContentPlugin,   // wy: 依赖 Content Store（获取内容存储实例）
			plugin.SnapshotPlugin,  // wy: 依赖所有 Snapshotter（获取快照器实例）
		},
		Config: &srvconfig.BoltConfig{
			ContentSharingPolicy: srvconfig.SharingPolicyShared,
		},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			if err := os.MkdirAll(ic.Root, 0711); err != nil {
				return nil, err
			}

			// wy: 从已初始化的插件集合中获取 Content Store 实例
			// 🚀 这依赖拓扑排序保证 ContentPlugin 在 MetadataPlugin 之前初始化
			cs, err := ic.Get(plugin.ContentPlugin)
			if err != nil {
				return nil, err
			}

			// wy: 获取所有已初始化的 Snapshotter 实例（可能有多个: overlayfs、native 等）
			snapshottersRaw, err := ic.GetByType(plugin.SnapshotPlugin)
			if err != nil {
				return nil, err
			}

			// wy: 过滤掉加载失败的 snapshotter，只保留可用的
			snapshotters := make(map[string]snapshots.Snapshotter)
			for name, sn := range snapshottersRaw {
				sn, err := sn.Instance()
				if err != nil {
					if !plugin.IsSkipPlugin(err) {
						log.G(ic.Context).WithError(err).
							Warnf("could not use snapshotter %v in metadata plugin", name)
					}
					continue
				}
				snapshotters[name] = sn.(snapshots.Snapshotter)
			}

			// wy: Content 共享策略配置
			// shared: 不同 namespace 的 container 可共享同一个 content blob（默认，节省磁盘）
			// isolated: 每个 namespace 的 content 完全隔离
			shared := true
			ic.Meta.Exports["policy"] = srvconfig.SharingPolicyShared
			if cfg, ok := ic.Config.(*srvconfig.BoltConfig); ok {
				if cfg.ContentSharingPolicy != "" {
					if err := cfg.Validate(); err != nil {
						return nil, err
					}
					if cfg.ContentSharingPolicy == srvconfig.SharingPolicyIsolated {
						ic.Meta.Exports["policy"] = srvconfig.SharingPolicyIsolated
						shared = false
					}

					log.L.WithField("policy", cfg.ContentSharingPolicy).Info("metadata content store policy set")
				}
			}

			// wy: 🚀 核心底层交互: 打开 BoltDB 数据库文件
			// 路径: /var/lib/containerd/io.containerd.metadata.v1.bolt/meta.db
			// BoltDB 特性:
			//   - 嵌入式 KV 数据库（无需独立进程）
			//   - 基于 B+ Tree 索引，支持事务
			//   - 只允许一个写事务（所有写操作串行化）
			//   - 多个并发读事务（MVCC）
			path := filepath.Join(ic.Root, "meta.db")
			ic.Meta.Exports["path"] = path

			db, err := bolt.Open(path, 0644, nil)
			if err != nil {
				return nil, err
			}

			var dbopts []metadata.DBOpt
			if !shared {
				dbopts = append(dbopts, metadata.WithPolicyIsolated)
			}

			// wy: 🚀 创建 metadata.DB——它将 BoltDB + Content Store + Snapshotters
			// 封装为统一的元数据管理接口，上层所有 Service 都通过它读写数据
			// DB 内部维护了多个 Bucket:
			//   v1/content/<namespace>/blobs/<digest> → content 索引
			//   v1/images/<namespace>/<name> → 镜像记录
			//   v1/containers/<namespace>/<id> → 容器元数据
			//   v1/snapshots/<namespace>/<snapshotter>/<key> → 快照索引
			//   v1/leases/<namespace>/<id> → GC 租约
			mdb := metadata.NewDB(db, cs.(content.Store), snapshotters, dbopts...)
			if err := mdb.Init(ic.Context); err != nil {
				return nil, err
			}
			return mdb, nil
		},
	})

	// wy: Step 4: 注册 Proxy Plugin（远程代理插件）
	// 允许将 Content Store 或 Snapshotter 指向远程服务（通过 gRPC 代理）
	// 配置示例: [proxy_plugins.my-remote-snapshotter]
	//             type = "snapshot"
	//             address = "/tmp/remote-snapshotter.sock"
	clients := &proxyClients{}
	for name, pp := range config.ProxyPlugins {
		var (
			t plugin.Type
			f func(*grpc.ClientConn) interface{}

			address = pp.Address
		)

		switch pp.Type {
		case string(plugin.SnapshotPlugin), "snapshot":
			t = plugin.SnapshotPlugin
			ssname := name
			f = func(conn *grpc.ClientConn) interface{} {
				return ssproxy.NewSnapshotter(ssapi.NewSnapshotsClient(conn), ssname)
			}

		case string(plugin.ContentPlugin), "content":
			t = plugin.ContentPlugin
			f = func(conn *grpc.ClientConn) interface{} {
				return csproxy.NewContentStore(csapi.NewContentClient(conn))
			}
		default:
			log.G(ctx).WithField("type", pp.Type).Warn("unknown proxy plugin type")
		}

		plugin.Register(&plugin.Registration{
			Type: t,
			ID:   name,
			InitFn: func(ic *plugin.InitContext) (interface{}, error) {
				ic.Meta.Exports["address"] = address
				conn, err := clients.getClient(address)
				if err != nil {
					return nil, err
				}
				return f(conn), nil
			},
		})

	}

	// wy: 根据配置文件版本选择禁用过滤器
	// V1/V2 配置格式中禁用插件的写法不同
	filter := srvconfig.V2DisabledFilter
	if config.GetVersion() == 1 {
		filter = srvconfig.V1DisabledFilter
	}

	// wy: 🚀 执行拓扑排序并返回有序列表
	// filter 函数标记被禁用的插件，Graph() 跳过已标记的插件
	// 配置文件中 [disabled_plugins] 列表里的插件将不会被加载
	return plugin.Graph(filter(config.DisabledPlugins)), nil
}

type proxyClients struct {
	m       sync.Mutex
	clients map[string]*grpc.ClientConn
}

func (pc *proxyClients) getClient(address string) (*grpc.ClientConn, error) {
	pc.m.Lock()
	defer pc.m.Unlock()
	if pc.clients == nil {
		pc.clients = map[string]*grpc.ClientConn{}
	} else if c, ok := pc.clients[address]; ok {
		return c, nil
	}

	backoffConfig := backoff.DefaultConfig
	backoffConfig.MaxDelay = 3 * time.Second
	connParams := grpc.ConnectParams{
		Backoff: backoffConfig,
	}
	gopts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithConnectParams(connParams),
		grpc.WithContextDialer(dialer.ContextDialer),

		// TODO(stevvooe): We may need to allow configuration of this on the client.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(defaults.DefaultMaxRecvMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(defaults.DefaultMaxSendMsgSize)),
	}

	conn, err := grpc.Dial(dialer.DialAddress(address), gopts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial %q", address)
	}

	pc.clients[address] = conn

	return conn, nil
}

func trapClosedConnErr(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "use of closed network connection") {
		return nil
	}
	return err
}
