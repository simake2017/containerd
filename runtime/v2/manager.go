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

package v2

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/events/exchange"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/metadata"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/pkg/timeout"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/runtime"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// Config for the v2 runtime
type Config struct {
	// Supported platforms
	Platforms []string `toml:"platforms"`
}

func init() {
	// wy: 🚀 注册 v2 Runtime Plugin——这是 containerd 管理容器的核心插件
	// ID: "task"，完整 URI: "io.containerd.runtime.v2.task"
	// 职责: 管理 shim 进程的生命周期（创建/连接/删除 shim）
	// 依赖: MetadataPlugin（从 BoltDB 读取容器元数据）
	plugin.Register(&plugin.Registration{
		Type: plugin.RuntimePluginV2,
		ID:   "task",
		Requires: []plugin.Type{
			plugin.MetadataPlugin, // wy: 依赖 BoltDB 元数据（获取容器记录）
		},
		Config: &Config{
			Platforms: defaultPlatforms(),
		},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			supportedPlatforms, err := parsePlatforms(ic.Config.(*Config).Platforms)
			if err != nil {
				return nil, err
			}

			ic.Meta.Platforms = supportedPlatforms
			// wy: 创建 runtime 的 root 和 state 目录
			// Root:  /var/lib/containerd/io.containerd.runtime.v2.task/  (持久化: 容器 working dir)
			// State: /run/containerd/io.containerd.runtime.v2.task/      (运行时: OCI bundle, shim socket)
			if err := os.MkdirAll(ic.Root, 0711); err != nil {
				return nil, err
			}
			if err := os.MkdirAll(ic.State, 0711); err != nil {
				return nil, err
			}

			// wy: 从 Metadata Plugin 获取容器存储接口
			m, err := ic.Get(plugin.MetadataPlugin)
			if err != nil {
				return nil, err
			}
			cs := metadata.NewContainerStore(m.(*metadata.DB))

			// wy: 🚀 创建 TaskManager——v2 runtime 的核心管理器
			// 它负责: 启动 shim、连接 shim、管理 task 列表、daemon 重启时恢复 shim
			return New(ic.Context, ic.Root, ic.State, ic.Address, ic.TTRPCAddress, ic.Events, cs)
		},
	})
}

// New task manager for v2 shims
func New(ctx context.Context, root, state, containerdAddress, containerdTTRPCAddress string, events *exchange.Exchange, cs containers.Store) (*TaskManager, error) {
	for _, d := range []string{root, state} {
		if err := os.MkdirAll(d, 0711); err != nil {
			return nil, err
		}
	}
	m := &TaskManager{
		root:                   root,
		state:                  state,
		containerdAddress:      containerdAddress,
		containerdTTRPCAddress: containerdTTRPCAddress,
		tasks:                  runtime.NewTaskList(),
		events:                 events,
		containers:             cs,
	}
	if err := m.loadExistingTasks(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

// TaskManager 管理所有 v2 shim 进程及其 task
// wy: 🚀 核心职责:
//   - Create: 为新容器启动 shim 进程，创建 task
//   - Get/Delete/Tasks: 查询和管理 task
//   - loadExistingTasks: daemon 重启时恢复之前存活的 shim 连接
//
// 数据结构:
//   root:  /var/lib/containerd/io.containerd.runtime.v2.task/<namespace>/<container-id>/
//          存放容器的 working 目录（overlayfs 的 upperdir/workdir）
//   state: /run/containerd/io.containerd.runtime.v2.task/<namespace>/<container-id>/
//          存放 OCI bundle（config.json, rootfs/）和 shim 的 TTRPC socket 地址
type TaskManager struct {
	root                   string // wy: 持久化目录（容器 working 目录）
	state                  string // wy: 运行时状态目录（bundle + shim address）
	containerdAddress      string // wy: containerd gRPC 地址（传递给 shim）
	containerdTTRPCAddress string // wy: containerd TTRPC 地址（shim 通过此地址发布事件）

	tasks      *runtime.TaskList  // wy: 当前所有运行中 task 的列表
	events     *exchange.Exchange // wy: 全局事件总线（shim 退出事件通过此发布）
	containers containers.Store   // wy: 容器元数据存储（BoltDB）
}

// ID of the task manager
func (m *TaskManager) ID() string {
	return fmt.Sprintf("%s.%s", plugin.RuntimePluginV2, "task")
}

// Create 为新容器创建 task（启动 shim + 在 shim 中创建容器）
// wy: 🚀 完整的创建流程:
//   1. NewBundle: 创建 OCI bundle 目录，写入 config.json
//   2. startShim: fork/exec containerd-shim-runc-v2 进程
//   3. shim.Create: 通过 TTRPC 调用 shim 的 Create → runc create
//
// 任何步骤失败都会清理 bundle 和 shim（defer 回滚）
func (m *TaskManager) Create(ctx context.Context, id string, opts runtime.CreateOpts) (_ runtime.Task, retErr error) {
	// wy: Step 1: 创建 OCI bundle 目录
	// 目录结构: /run/containerd/io.containerd.runtime.v2.task/<namespace>/<id>/
	//   ├── config.json  ← OCI runtime spec（从 opts.Spec 解码写入）
	//   └── rootfs/      ← 由 shim 挂载 overlayfs 到此目录
	bundle, err := NewBundle(ctx, m.root, m.state, id, opts.Spec.Value)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			bundle.Delete() // wy: 失败时清理 bundle 目录
		}
	}()

	// wy: Step 2: 启动 shim 进程
	shim, err := m.startShim(ctx, bundle, id, opts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			m.deleteShim(shim) // wy: 失败时删除 shim
		}
	}()

	// wy: Step 3: 通过 TTRPC 调用 shim 的 Task.Create
	// shim 内部: runc.NewContainer() → 挂载 rootfs → runc create
	t, err := shim.Create(ctx, opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create shim")
	}

	// wy: 将 task 加入运行时 task 列表
	if err := m.tasks.Add(ctx, t); err != nil {
		return nil, errors.Wrap(err, "failed to add task")
	}

	return t, nil
}

// startShim 启动 shim 进程并建立 TTRPC 连接
// wy: 🚀 Shim 启动协议（两次调用模式）:
//
//   第 1 次调用: containerd-shim-runc-v2 start
//     → shim fork 自身，子进程作为 shim server 运行
//     → 父进程将 TTRPC socket 地址打印到 stdout 后退出
//     → containerd 从 stdout 读取地址
//
//   第 2 次调用: containerd-shim-runc-v2（无参数，作为 shim server 运行）
//     → shim server 在 TTRPC socket 上监听
//     → 注册 TaskService（Create/Start/Kill/Delete/Exec 等 RPC）
//     → containerd 通过读取的地址建立 TTRPC 连接
//
// opts.Runtime 决定使用哪个 shim 二进制:
//   "io.containerd.runc.v2" → containerd-shim-runc-v2
//   "io.containerd.kata.v2" → containerd-shim-kata-v2（如果安装了的话）
func (m *TaskManager) startShim(ctx context.Context, bundle *Bundle, id string, opts runtime.CreateOpts) (*shim, error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, err
	}

	topts := opts.TaskOptions
	if topts == nil {
		topts = opts.RuntimeOptions
	}

	// wy: 🚀 构建 shim 二进制调用器
	// shimBinary 根据 opts.Runtime 名称解析 shim 二进制路径
	// 例如: "io.containerd.runc.v2" → "containerd-shim-runc-v2"
	b := shimBinary(ctx, bundle, opts.Runtime, m.containerdAddress, m.containerdTTRPCAddress, m.events, m.tasks)

	// wy: 启动 shim 进程
	shim, err := b.Start(ctx, topts, func() {
		// wy: 这是 shim 断开连接的回调——当 shim 进程异常退出时触发
		log.G(ctx).WithField("id", id).Info("shim disconnected")

		// wy: 🚀 清理死掉的 shim: 杀掉容器进程、发布退出事件
		cleanupAfterDeadShim(context.Background(), id, ns, m.tasks, m.events, b)
		// wy: 从 task 列表中移除（因为 TTRPC 已断开，无法再调用 shim.Delete()）
		m.tasks.Delete(ctx, id)
	})
	if err != nil {
		return nil, errors.Wrap(err, "start failed")
	}

	return shim, nil
}

// deleteShim attempts to properly delete and cleanup shim after error
func (m *TaskManager) deleteShim(shim *shim) {
	dctx, cancel := timeout.WithContext(context.Background(), cleanupTimeout)
	defer cancel()

	_, errShim := shim.Delete(dctx)
	if errShim != nil {
		if errdefs.IsDeadlineExceeded(errShim) {
			dctx, cancel = timeout.WithContext(context.Background(), cleanupTimeout)
			defer cancel()
		}
		shim.Shutdown(dctx)
		shim.Close()
	}
}

// Get a specific task
func (m *TaskManager) Get(ctx context.Context, id string) (runtime.Task, error) {
	return m.tasks.Get(ctx, id)
}

// Add a runtime task
func (m *TaskManager) Add(ctx context.Context, task runtime.Task) error {
	return m.tasks.Add(ctx, task)
}

// Delete a runtime task
func (m *TaskManager) Delete(ctx context.Context, id string) {
	m.tasks.Delete(ctx, id)
}

// Tasks lists all tasks
func (m *TaskManager) Tasks(ctx context.Context, all bool) ([]runtime.Task, error) {
	return m.tasks.GetAll(ctx, all)
}

func (m *TaskManager) loadExistingTasks(ctx context.Context) error {
	nsDirs, err := ioutil.ReadDir(m.state)
	if err != nil {
		return err
	}
	for _, nsd := range nsDirs {
		if !nsd.IsDir() {
			continue
		}
		ns := nsd.Name()
		// skip hidden directories
		if len(ns) > 0 && ns[0] == '.' {
			continue
		}
		log.G(ctx).WithField("namespace", ns).Debug("loading tasks in namespace")
		if err := m.loadTasks(namespaces.WithNamespace(ctx, ns)); err != nil {
			log.G(ctx).WithField("namespace", ns).WithError(err).Error("loading tasks in namespace")
			continue
		}
		if err := m.cleanupWorkDirs(namespaces.WithNamespace(ctx, ns)); err != nil {
			log.G(ctx).WithField("namespace", ns).WithError(err).Error("cleanup working directory in namespace")
			continue
		}
	}
	return nil
}

func (m *TaskManager) loadTasks(ctx context.Context) error {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return err
	}
	shimDirs, err := ioutil.ReadDir(filepath.Join(m.state, ns))
	if err != nil {
		return err
	}
	for _, sd := range shimDirs {
		if !sd.IsDir() {
			continue
		}
		id := sd.Name()
		// skip hidden directories
		if len(id) > 0 && id[0] == '.' {
			continue
		}
		bundle, err := LoadBundle(ctx, m.state, id)
		if err != nil {
			// fine to return error here, it is a programmer error if the context
			// does not have a namespace
			return err
		}
		// fast path
		bf, err := ioutil.ReadDir(bundle.Path)
		if err != nil {
			bundle.Delete()
			log.G(ctx).WithError(err).Errorf("fast path read bundle path for %s", bundle.Path)
			continue
		}
		if len(bf) == 0 {
			bundle.Delete()
			continue
		}
		container, err := m.container(ctx, id)
		if err != nil {
			log.G(ctx).WithError(err).Errorf("loading container %s", id)
			if err := mount.UnmountAll(filepath.Join(bundle.Path, "rootfs"), 0); err != nil {
				log.G(ctx).WithError(err).Errorf("forceful unmount of rootfs %s", id)
			}
			bundle.Delete()
			continue
		}
		binaryCall := shimBinary(ctx, bundle, container.Runtime.Name, m.containerdAddress, m.containerdTTRPCAddress, m.events, m.tasks)
		shim, err := loadShim(ctx, bundle, m.events, m.tasks, func() {
			log.G(ctx).WithField("id", id).Info("shim disconnected")

			cleanupAfterDeadShim(context.Background(), id, ns, m.tasks, m.events, binaryCall)
			// Remove self from the runtime task list.
			m.tasks.Delete(ctx, id)
		})
		if err != nil {
			cleanupAfterDeadShim(ctx, id, ns, m.tasks, m.events, binaryCall)
			continue
		}
		m.tasks.Add(ctx, shim)
	}
	return nil
}

func (m *TaskManager) container(ctx context.Context, id string) (*containers.Container, error) {
	container, err := m.containers.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return &container, nil
}

func (m *TaskManager) cleanupWorkDirs(ctx context.Context) error {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return err
	}
	dirs, err := ioutil.ReadDir(filepath.Join(m.root, ns))
	if err != nil {
		return err
	}
	for _, d := range dirs {
		// if the task was not loaded, cleanup and empty working directory
		// this can happen on a reboot where /run for the bundle state is cleaned up
		// but that persistent working dir is left
		if _, err := m.tasks.Get(ctx, d.Name()); err != nil {
			path := filepath.Join(m.root, ns, d.Name())
			if err := os.RemoveAll(path); err != nil {
				log.G(ctx).WithError(err).Errorf("cleanup working dir %s", path)
			}
		}
	}
	return nil
}

func parsePlatforms(platformStr []string) ([]ocispec.Platform, error) {
	p := make([]ocispec.Platform, len(platformStr))
	for i, v := range platformStr {
		parsed, err := platforms.Parse(v)
		if err != nil {
			return nil, err
		}
		p[i] = parsed
	}
	return p, nil
}
