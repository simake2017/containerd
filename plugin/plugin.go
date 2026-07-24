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

package plugin

import (
	"fmt"
	"sync"

	"github.com/containerd/ttrpc"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var (
	// ErrNoType is returned when no type is specified
	ErrNoType = errors.New("plugin: no type")
	// ErrNoPluginID is returned when no id is specified
	ErrNoPluginID = errors.New("plugin: no id")
	// ErrIDRegistered is returned when a duplicate id is already registered
	ErrIDRegistered = errors.New("plugin: id already registered")
	// ErrSkipPlugin is used when a plugin is not initialized and should not be loaded,
	// this allows the plugin loader differentiate between a plugin which is configured
	// not to load and one that fails to load.
	ErrSkipPlugin = errors.New("skip plugin")

	// ErrInvalidRequires will be thrown if the requirements for a plugin are
	// defined in an invalid manner.
	ErrInvalidRequires = errors.New("invalid requires")
)

// IsSkipPlugin returns true if the error is skipping the plugin
func IsSkipPlugin(err error) bool {
	return errors.Is(err, ErrSkipPlugin)
}

// Type is the type of the plugin
type Type string

func (t Type) String() string { return string(t) }

// wy: Plugin Type 常量定义
// containerd 将所有功能模块抽象为 Plugin，每种 Type 代表一类子系统：
//   - ContentPlugin: 内容存储（CAS），存储镜像 blob（layer、config、manifest）
//   - SnapshotPlugin: 文件系统快照器（overlayfs/native/devmapper），负责 rootfs 层叠合并
//   - MetadataPlugin: BoltDB 元数据引擎，所有上层 Service 的数据持久化后端
//   - RuntimePluginV2: v2 Runtime，管理 shim 进程的创建/连接/生命周期
//   - ServicePlugin: Daemon 内部 Service（如 TaskService、ImageService），是 gRPC 的底层实现
//   - GRPCPlugin: 将 ServicePlugin 包装为对外暴露的 gRPC 服务端点
//   - DiffPlugin: 文件系统 diff 的生成与应用（用于镜像解包、checkpoint）
//   - GCPlugin: 垃圾回收策略（标记-清除，基于 BoltDB label 引用关系）
const (
	// InternalPlugin implements an internal plugin to containerd
	// wy: 内部插件，不直接对外暴露 gRPC，用于中间组件（如 events exchange）
	InternalPlugin Type = "io.containerd.internal.v1"
	// RuntimePlugin implements a runtime
	// wy: v1 Runtime 插件（已废弃，保留向后兼容）
	RuntimePlugin Type = "io.containerd.runtime.v1"
	// RuntimePluginV2 implements a runtime v2
	// wy: 🚀 核心——v2 Runtime 插件，管理 containerd-shim-runc-v2 进程
	// 默认实现: runtime/v2.TaskManager，注册 ID 为 "task"
	RuntimePluginV2 Type = "io.containerd.runtime.v2"
	// ServicePlugin implements a internal service
	// wy: Daemon 内部 Service 插件，如 tasks、images、containers 等
	// 它们是 gRPC handler 的直接实现者，但不处理网络协议
	ServicePlugin Type = "io.containerd.service.v1"
	// GRPCPlugin implements a grpc service
	// wy: gRPC 服务插件——将 ServicePlugin 包装并注册到 gRPC Server
	// Client 端实际调用的就是这些插件暴露的 API
	GRPCPlugin Type = "io.containerd.grpc.v1"
	// SnapshotPlugin implements a snapshotter
	// wy: 🚀 快照器插件，Linux 默认注册的是 overlayfs（snapshots/overlay）
	// 还有 native（直接拷贝）和 devmapper（块设备 thin-provisioning）
	SnapshotPlugin Type = "io.containerd.snapshotter.v1"
	// TaskMonitorPlugin implements a task monitor
	// wy: Task 监控器插件，负责监控 Task 状态变化（如容器退出事件）
	TaskMonitorPlugin Type = "io.containerd.monitor.v1"
	// DiffPlugin implements a differ
	// wy: Diff 插件，负责 tar 包的生成（Commit）和应用（Apply）
	// 默认实现: walking differ（diff/walking）
	DiffPlugin Type = "io.containerd.differ.v1"
	// MetadataPlugin implements a metadata store
	// wy: 🚀 元数据插件——BoltDB 引擎，是 containerd 的数据中枢
	// 所有 Container/Image/Content/Snapshot/Lease 记录都存储在 meta.db 中
	// 默认实现: metadata.DB，注册 ID 为 "bolt"
	MetadataPlugin Type = "io.containerd.metadata.v1"
	// ContentPlugin implements a content store
	// wy: 内容存储插件——基于文件系统的 CAS（Content Addressable Storage）
	// 默认实现: content/local.Store，blob 存储在 blobs/sha256/<digest> 文件中
	ContentPlugin Type = "io.containerd.content.v1"
	// GCPlugin implements garbage collection policy
	// wy: GC 插件——负责清理不再被引用的 content/snapshot/image
	// 默认实现: gc/scheduler，基于 label 引用关系做标记-清除
	GCPlugin Type = "io.containerd.gc.v1"
)

// wy: 预定义的 Runtime 名称常量
// 这些名称决定了容器创建时使用哪个 shim 二进制：
//   "io.containerd.runc.v2" → 查找 containerd-shim-runc-v2 二进制
//   "io.containerd.runc.v1" → 查找 containerd-shim-runc-v1 二进制（废弃）
// 命名规则: "io.containerd.<runtime-type>.<version>"
const (
	// RuntimeLinuxV1 is the legacy linux runtime
	// wy: v1 时代的 runtime，使用旧的 shim 协议（每个容器一个 shim，v1 API）
	RuntimeLinuxV1 = "io.containerd.runtime.v1.linux"
	// RuntimeRuncV1 is the runc runtime that supports a single container
	// wy: runc v1 shim——每个容器独占一个 shim 进程，已废弃
	RuntimeRuncV1 = "io.containerd.runc.v1"
	// RuntimeRuncV2 is the runc runtime that supports multiple containers per shim
	// wy: 🚀 生产默认——runc v2 shim，支持同一个 shim 管理多个容器（Pod 场景）
	// 通过 annotation "io.containerd.runc.v2.group" 或 "io.kubernetes.cri.sandbox-id" 分组
	RuntimeRuncV2 = "io.containerd.runc.v2"
)

// Registration 描述一个插件的注册信息——在 Go init() 阶段通过 plugin.Register() 注册到全局列表
// wy: 🚀 关键设计：
//   - Type + ID 构成插件的唯一标识（URI），如 "io.containerd.snapshotter.v1.overlayfs"
//   - Requires 声明依赖的其他 Plugin Type，用于拓扑排序确定初始化顺序
//   - InitFn 是延迟初始化函数——在 Server.New() 按拓扑序调用时才真正创建插件实例
//   - Config 是插件的默认配置，可被 TOML 配置文件覆盖
type Registration struct {
	// Type of the plugin
	Type Type
	// ID of the plugin
	ID string
	// Config specific to the plugin
	// wy: 插件特定的配置结构体指针，在 InitFn 中通过 ic.Config 获取
	Config interface{}
	// Requires is a list of plugins that the registered plugin requires to be available
	// wy: 依赖的 Plugin Type 列表。"*" 表示依赖所有已注册插件（如 metadata 插件需要等所有 snapshotter 就绪）
	// Graph() 函数根据此字段做拓扑排序，确保被依赖者先初始化
	Requires []Type

	// InitFn is called when initializing a plugin. The registration and
	// context are passed in. The init function may modify the registration to
	// add exports, capabilities and platform support declarations.
	// wy: 🚀 初始化函数——返回的 interface{} 是插件实例
	// 实例可以实现多个接口：Service（gRPC）、TTRPCService、TCPService、io.Closer 等
	InitFn func(*InitContext) (interface{}, error)
	// Disable the plugin from loading
	Disable bool
}

// Init 调用注册函数的 InitFn 初始化插件
// wy: 在 Server.New() 的插件初始化循环中被调用，按拓扑序依次初始化
// InitFn 返回的 instance 可能是：
//   - content.Store（Content 插件）
//   - snapshots.Snapshotter（Snapshotter 插件）
//   - *metadata.DB（Metadata 插件）
//   - *v2.TaskManager（Runtime V2 插件）
//   - plugin.Service 实现（Service 插件）
//   - plugin.TTRPCService 实现（TTRPC 插件）
func (r *Registration) Init(ic *InitContext) *Plugin {
	p, err := r.InitFn(ic)
	return &Plugin{
		Registration: r,
		Config:       ic.Config,
		Meta:         ic.Meta,
		instance:     p,
		err:          err,
	}
}

// URI returns the full plugin URI
func (r *Registration) URI() string {
	return fmt.Sprintf("%s.%s", r.Type, r.ID)
}

// wy: 🚀 三种服务注册接口——插件实例实现了哪个接口，就被注册到对应的 Server 上
// Server.New() 在初始化完所有插件后，遍历实例检查它们实现了哪些接口：
//   instance.(plugin.Service)      → 注册到 gRPC Unix Socket Server
//   instance.(plugin.TTRPCService) → 注册到 TTRPC Server（给 shim 用的轻量 RPC）
//   instance.(plugin.TCPService)   → 注册到 gRPC TCP Server（远程访问）

// Service allows GRPC services to be registered with the underlying server
// wy: gRPC 服务接口——实现此接口的插件会被注册到 containerd 的 Unix Socket gRPC Server
// Client 端通过 grpc.Dial("unix:///run/containerd/containerd.sock") 调用的就是这些服务
type Service interface {
	Register(*grpc.Server) error
}

// TTRPCService allows TTRPC services to be registered with the underlying server
// wy: TTRPC 服务接口——TTRPC 是 containerd 自研的轻量 RPC 协议（基于 Unix Socket）
// 用于 Daemon ↔ Shim 之间的通信，比 gRPC 开销更低（无 HTTP/2、无 TLS）
type TTRPCService interface {
	RegisterTTRPC(*ttrpc.Server) error
}

// TCPService allows GRPC services to be registered with the underlying tcp server
// wy: TCP 服务接口——实现此接口的插件会被注册到 TCP 端口的 gRPC Server
// 用于远程访问 containerd API（需配置 TLS 证书）
type TCPService interface {
	RegisterTCP(*grpc.Server) error
}

var register = struct {
	sync.RWMutex
	r []*Registration
}{}

// Load loads all plugins at the provided path into containerd
func Load(path string) (err error) {
	defer func() {
		if v := recover(); v != nil {
			rerr, ok := v.(error)
			if !ok {
				rerr = fmt.Errorf("%s", v)
			}
			err = rerr
		}
	}()
	return loadPlugins(path)
}

// Register 将插件注册到全局注册表
// wy: 🚀 在所有插件包的 Go init() 函数中被调用
// 例如: snapshots/overlay/plugin/plugin.go 的 init() 调用 Register(overlayfs)
//       runtime/v2/manager.go 的 init() 调用 Register(TaskManager)
//       cmd/containerd/builtins_linux.go 通过 blank import 触发这些 init()
//
// 注册时机：Go 程序启动时，所有被 import 的包的 init() 按依赖顺序执行
// 注册结果：全局 register.r 切片中积累了所有 Plugin 的 Registration
func Register(r *Registration) {
	register.Lock()
	defer register.Unlock()

	if r.Type == "" {
		panic(ErrNoType)
	}
	if r.ID == "" {
		panic(ErrNoPluginID)
	}
	if err := checkUnique(r); err != nil {
		panic(err) // wy: Type+ID 组合必须全局唯一，否则 panic
	}

	// wy: "*" 表示依赖所有插件类型，此时不允许再声明其他依赖
	for _, requires := range r.Requires {
		if requires == "*" && len(r.Requires) != 1 {
			panic(ErrInvalidRequires)
		}
	}

	register.r = append(register.r, r) // wy: 追加到全局注册列表
}

func checkUnique(r *Registration) error {
	for _, registered := range register.r {
		if r.URI() == registered.URI() {
			return errors.Wrap(ErrIDRegistered, r.URI())
		}
	}
	return nil
}

// DisableFilter filters out disabled plugins
type DisableFilter func(r *Registration) bool

// Graph 返回按依赖关系拓扑排序后的插件初始化顺序列表
// wy: 🚀 核心算法——DFS 拓扑排序，保证被依赖的插件排在依赖者之前初始化
// 调用时机: Server.New() → LoadPlugins() → plugin.Graph(filter)
//
// 排序结果示例（简化）:
//   [ContentPlugin/content, SnapshotPlugin/overlayfs, MetadataPlugin/bolt,
//    RuntimePluginV2/task, ServicePlugin/tasks, GRPCPlugin/tasks, ...]
//
// 这意味着:
//   1. Content Store 最先初始化（无依赖）
//   2. Snapshotter 紧随其后（无依赖）
//   3. Metadata(BoltDB) 在 Content+Snapshot 之后初始化（它依赖这两者）
//   4. Runtime V2 在 Metadata 之后（它依赖 Metadata 获取容器信息）
//   5. TaskService 在 Runtime V2 之后（它依赖 Runtime 创建/管理 Task）
//   6. gRPC 包装器最后（它将 ServicePlugin 注册到 gRPC Server）
func Graph(filter DisableFilter) (ordered []*Registration) {
	register.RLock()
	defer register.RUnlock()

	// wy: Step 1: 根据配置文件的 disabled_plugins 列表标记被禁用的插件
	for _, r := range register.r {
		if filter(r) {
			r.Disable = true
		}
	}

	// wy: Step 2: DFS 遍历，递归地将依赖排在前
	added := map[*Registration]bool{}
	for _, r := range register.r {
		if r.Disable {
			continue
		}
		// wy: 递归处理 r 的所有依赖——确保依赖先加入 ordered
		children(r, added, &ordered)
		if !added[r] {
			ordered = append(ordered, r)
			added[r] = true
		}
	}
	return ordered
}

// children 递归地将 reg 的所有依赖插件加入 ordered 列表
// wy: 算法逻辑:
//   1. 遍历 reg.Requires 中声明的每个 Plugin Type
//   2. 在全局注册表中找到匹配该 Type 的所有插件
//   3. 对每个匹配的插件递归调用 children()（先确保它的依赖也在列表中）
//   4. 最后将该插件加入 ordered
//
// 特殊情况: Requires 为 "*" 时，匹配所有非自身的已注册插件
// 例如: MetadataPlugin 声明 Requires: [ContentPlugin, SnapshotPlugin]
//       → children 会先递归初始化所有 ContentPlugin 和 SnapshotPlugin
func children(reg *Registration, added map[*Registration]bool, ordered *[]*Registration) {
	for _, t := range reg.Requires {
		for _, r := range register.r {
			if !r.Disable &&
				r.URI() != reg.URI() &&             // wy: 排除自身，避免循环
				(t == "*" || r.Type == t) {          // wy: Type 匹配（或 "*" 全匹配）
				children(r, added, ordered)           // wy: 递归：先把 r 的依赖排好
				if !added[r] {
					*ordered = append(*ordered, r)    // wy: 依赖者排在被依赖者之后
					added[r] = true
				}
			}
		}
	}
}
