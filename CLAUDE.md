# containerd v1.5.x 源码剖析记忆文件

## 项目基本信息

- **版本**: containerd v1.5.x (commit 5280530a0)
- **语言**: Go
- **仓库路径**: `/Users/wangyang/goproject/src/containerd`
- **注释规范**: 所有中文注释带 `wy:` 标记，核心底层交互用 `🚀` 标记

## 架构总览（5 层）

```
┌─ Client 层 ─────────────────────────────────────────────────┐
│  ctr / Docker / kubelet 通过 gRPC 调用                       │
│  client.go → container.go → task.go → pull.go               │
└──────────────────────┬──────────────────────────────────────┘
                       │ gRPC (Unix Socket: /run/containerd/containerd.sock)
┌─ Daemon Services 层 ─┴──────────────────────────────────────┐
│  services/tasks/local.go → services/images/ → services/...  │
│  各 ServicePlugin 实现 gRPC handler                          │
└──────────────────────┬──────────────────────────────────────┘
                       │ 内部调用
┌─ Runtime V2 层 ──────┴──────────────────────────────────────┐
│  runtime/v2/manager.go (TaskManager)                         │
│  管理 shim 进程: 启动/连接/删除                               │
└──────────────────────┬──────────────────────────────────────┘
                       │ TTRPC (Unix Socket) + fork/exec
┌─ Shim 层 ────────────┴──────────────────────────────────────┐
│  containerd-shim-runc-v2 (独立进程)                           │
│  runtime/v2/runc/v2/service.go → runc create/start/kill     │
└──────────────────────┬──────────────────────────────────────┘
                       │ fork/exec
┌─ runc ───────────────┴──────────────────────────────────────┐
│  OCI Runtime: 设置 namespaces/cgroups/seccomp → execve      │
└─────────────────────────────────────────────────────────────┘
```

## 已注释的核心文件清单 (16 个)

### 第 1 层: Plugin 机制
| 文件 | 关键内容 |
|---|---|
| `plugin/plugin.go` | Type 常量(10种)、Registration、Register()、Graph() 拓扑排序、Service/TTRPCService/TCPService 接口 |

### 第 2 层: Daemon 启动
| 文件 | 关键内容 |
|---|---|
| `cmd/containerd/builtins_linux.go` | blank import 触发插件 init() 注册 |
| `cmd/containerd/command/main.go` | 8 步启动: config → dirs → signals → server.New → listeners |
| `services/server/server.go` | New() 初始化全部插件、LoadPlugins() 注册 Content/Metadata Plugin、BoltDB 打开 |

### 第 3 层: Client 层
| 文件 | 关键内容 |
|---|---|
| `client.go` | New() gRPC 连接、Namespace 拦截器、NewContainer() 函数式选项、各 XxxService() 双路径(proxy/直连) |
| `container.go` | NewTask() 完整流程: IO创建 → rootfs mounts → gRPC Create |
| `task.go` | Task 接口(状态机 Created→Running→Stopped)、Start/Kill/Wait/Delete/Exec |
| `pull.go` | Pull() 完整流程: Resolve→Dispatch→边下载边解包、fetch() handler 链 |

### 第 4 层: Runtime/Shim 层
| 文件 | 关键内容 |
|---|---|
| `runtime/v2/manager.go` | TaskManager: Create() 3步(Bundle→startShim→shim.Create)、两次调用协议 |
| `runtime/v2/runc/v2/service.go` | shim 内 service: New()(OOM/Reaper/Platform)、Create(runc create)、Start(runc start+cgroup OOM) |
| `runtime/v2/shim/shim.go` | Shim 接口、run() 3种action(delete/start/server)、Serve() TTRPC注册、subreaper |
| `services/tasks/local.go` | local 结构体、Create() 5步(getContainer→buildOpts→getRuntime→runtime.Create→monitor) |

### 第 5 层: 核心子系统
| 文件 | 关键内容 |
|---|---|
| `oci/spec.go` | Spec=specs.Spec、GenerateSpec 平台默认值、ApplyOpts 函数式选项模式 |
| `snapshots/snapshotter.go` | Kind 三态(Active/Committed/View)、Snapshotter 接口(Prepare/Commit/Mounts/Remove) |
| `content/content.go` | Provider/Ingester/Store 接口、CAS 文件布局(blobs/sha256/<digest>) |
| `rootfs/apply.go` | Layer 结构、ApplyLayers 逐层解包(Prepare→Apply→Commit)、chainID 计算 |

## 核心调用链路

### 容器创建 (NewContainer + NewTask)
```
Client.NewContainer()
  → gRPC: ContainerService.Create → Daemon → BoltDB 写入容器记录

Client.NewTask()
  → cio.Create() → mkfifo 创建 FIFO
  → Snapshotter.Mounts() → 获取 overlay mount 参数
  → gRPC: TaskService.Create
    → services/tasks/local.go: Create()
      → runtime/v2/manager.go: TaskManager.Create()
        → NewBundle() → 创建 bundle 目录 + 写入 config.json
        → startShim() → fork/exec containerd-shim-runc-v2
          → shim 输出 TTRPC socket 地址到 stdout
        → TTRPC Connect → shim.Task.Create()
          → runc.NewContainer() → mount rootfs → runc create <id>
```

### 镜像拉取 (Pull)
```
Client.Pull("docker.io/library/nginx:latest")
  → WithLease() 防 GC
  → Resolver.Resolve() → HTTP 认证 + manifest 解析
  → images.Dispatch() 递归分发:
      [manifest list] → FetchHandler → ChildrenHandler(平台过滤)
      [manifest]      → FetchHandler → ChildrenHandler(获取 config+layers)
      [config]        → FetchHandler
      [layer N]       → FetchHandler → UnpackHandler(并行解包)
  → Unpacker:
      Prepare("extract-1", "") → diff.Apply(tar) → Commit("chain-1")
      Prepare("extract-2", "chain-1") → diff.Apply(tar) → Commit("chain-2")
      ...
  → imageService.Create() → BoltDB 写入 Image 记录
```

## 关键数据路径

```
持久化 (/var/lib/containerd):
  ├── io.containerd.content.v1.content/blobs/sha256/<digest>  ← Content Store
  ├── io.containerd.metadata.v1.bolt/meta.db                  ← BoltDB 元数据
  ├── io.containerd.snapshotter.v1.overlayfs/snapshots/       ← overlayfs 快照数据
  └── io.containerd.runtime.v2.task/<ns>/<id>/                ← 容器 working 目录

运行时 (/run/containerd):
  ├── containerd.sock                                          ← gRPC 主端口
  ├── containerd.sock.ttrpc                                    ← TTRPC 端口
  ├── io.containerd.runtime.v2.task/<ns>/<id>/
  │   ├── config.json                                          ← OCI runtime spec
  │   ├── rootfs/                                              ← 容器 rootfs 挂载点
  │   └── address                                              ← shim TTRPC socket 地址
  └── fifos/<id>-stdin/stdout/stderr                           ← IO FIFO 管道
```

## BoltDB 元数据架构 (meta.db)

```
v1/
├── content/<namespace>/blobs/<digest>  → Info{Size, Labels}
├── images/<namespace>/<name>          → Image{Name, Target(Descriptor)}
├── containers/<namespace>/<id>        → Container{Image, Spec, SnapshotKey, Runtime}
├── snapshots/<namespace>/<snapshotter>/<key> → Info{Kind, Parent, Labels}
├── leases/<namespace>/<id>            → GC 租约
└── namespaces/<name>                  → Labels
```

## Plugin 初始化顺序

```
Layer 0: ContentPlugin/content, SnapshotPlugin/overlayfs, SnapshotPlugin/native
Layer 1: MetadataPlugin/bolt (依赖 Content + Snapshot)
Layer 2: RuntimePluginV2/task (依赖 Metadata), DiffPlugin/walking
Layer 3: ServicePlugin/tasks, images, containers, snapshots, content (依赖 Runtime + Metadata)
Layer 4: GRPCPlugin/* (包装 ServicePlugin 为 gRPC endpoint)
```

## 未注释但重要的文件 (后续可扩展)

| 文件 | 用途 |
|---|---|
| `runtime/v2/shim.go` | Daemon 侧 shim 对象(TTRPC 调用封装) |
| `runtime/v2/bundle.go` | OCI bundle 目录准备 |
| `runtime/v2/runc/container.go` | runc.Container(init+exec 进程管理) |
| `pkg/process/init.go` | InitProcess(runc create 执行者) |
| `pkg/process/exec.go` | ExecProcess(runc exec 执行者) |
| `sys/reaper/reaper.go` | 僵尸进程回收器(wait4+SIGCHLD) |
| `metadata/db.go` | BoltDB 元数据引擎实现 |
| `snapshots/overlay/overlay.go` | overlayfs 实现 |
| `content/local/store.go` | 本地 CAS Store 实现 |
| `images/image.go` | 镜像对象与 handler 分发 |
| `gc/scheduler/scheduler.go` | GC 调度器 |

## OCI 标准映射

| containerd 组件 | OCI 规范 | 具体映射 |
|---|---|---|
| `oci/spec.go` | Runtime Spec | 生成 config.json (namespaces/cgroups/mounts/process) |
| `content/content.go` | Image Spec | 存储 manifest/config/layer blob (CAS by digest) |
| `images/image.go` | Image Spec | 镜像记录: name → Descriptor(manifest digest) |
| `snapshots/snapshotter.go` | Runtime Spec (rootfs) | 将各 layer 叠合为 rootfs 挂载 |
| `rootfs/apply.go` | Image Spec + Runtime Spec | 将 Image layers 解包为 Snapshot 链 |
