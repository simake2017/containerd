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

package oci

import (
	"context"
	"path/filepath"
	"runtime"

	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/platforms"

	"github.com/containerd/containerd/containers"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const (
	rwm               = "rwm"
	defaultRootfsPath = "rootfs"
)

var (
	defaultUnixEnv = []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	}
)

// Spec 是 OCI runtime spec 的类型别名
// wy: 🚀 这就是 OCI Runtime Specification 中的 config.json 对应的 Go 结构体
// 它定义了容器的完整运行配置:
//   - Root: rootfs 路径和只读标志
//   - Process: 进程配置（args, env, cwd, capabilities, rlimits）
//   - Linux: Linux 特有配置（namespaces, cgroups, seccomp, devices）
//   - Mounts: 挂载列表（proc, sys, tmpfs, cgroup 等）
//   - Hostname: 容器主机名
//   - Annotations: 注释键值对
//
// 最终这个 Spec 会被序列化为 JSON，写入 OCI bundle 的 config.json 文件
// runc 读取 config.json 来创建容器
type Spec = specs.Spec

// GenerateSpec 从镜像生成默认的 OCI runtime spec
// wy: 🚀 调用时机: client.NewContainer() 中的 WithNewSpec() 选项
// 生成流程:
//   1. generateDefaultSpecWithPlatform: 填充平台默认值（namespaces, mounts, devices）
//   2. ApplyOpts: 按顺序应用所有 SpecOpts（从镜像提取 entrypoint、设置资源限制等）
func GenerateSpec(ctx context.Context, client Client, c *containers.Container, opts ...SpecOpts) (*Spec, error) {
	return GenerateSpecWithPlatform(ctx, client, platforms.DefaultString(), c, opts...)
}

// GenerateSpecWithPlatform 按指定平台生成 OCI spec
// wy: 支持跨平台生成（如为 linux/arm64 生成 spec，即使在 amd64 机器上）
func GenerateSpecWithPlatform(ctx context.Context, client Client, platform string, c *containers.Container, opts ...SpecOpts) (*Spec, error) {
	var s Spec
	// wy: Step 1: 填充平台默认值
	if err := generateDefaultSpecWithPlatform(ctx, platform, c.ID, &s); err != nil {
		return nil, err
	}

	// wy: Step 2: 应用所有 SpecOpts（函数式选项模式）
	return &s, ApplyOpts(ctx, client, c, &s, opts...)
}

// generateDefaultSpecWithPlatform 根据目标平台填充默认的 OCI spec
// wy: Linux 默认值包括:
//   - Namespaces: PID, IPC, UTS, Mount, Network（5 种隔离）
//   - Mounts: proc, tmpfs(/dev, /dev/shm, /dev/mqueue, /dev/pts), sysfs, cgroup
//   - Root: {Path: "rootfs", Readonly: false}
//   - Process: {Cwd: "/", Capabilities: [默认 caps], Rlimits: [RLIMIT_NOFILE]}
//   - Devices: /dev/null, /dev/zero, /dev/full, /dev/random, /dev/urandom, /dev/tty
func generateDefaultSpecWithPlatform(ctx context.Context, platform, id string, s *Spec) error {
	plat, err := platforms.Parse(platform)
	if err != nil {
		return err
	}

	if plat.OS == "windows" {
		err = populateDefaultWindowsSpec(ctx, s, id)
	} else {
		err = populateDefaultUnixSpec(ctx, s, id)
		if err == nil && runtime.GOOS == "windows" {
			// LCOW (Linux Containers on Windows): 同时填充 Linux 和 Windows 段
			s.Windows = &specs.Windows{}
		}
	}
	return err
}

// ApplyOpts 按顺序应用所有 SpecOpts 到 spec 上
// wy: 🚀 SpecOpts 是函数式选项模式的核心:
//   每个 SpecOpts 是一个 func(ctx, client, container, spec) error
//   它可以读取镜像配置、修改 spec 字段、注入客户端数据
//
// 常用的 SpecOpts:
//   WithImageConfig(img)  → 从镜像提取 entrypoint/cmd/env/user
//   WithProcessArgs(...)  → 覆盖进程启动参数
//   WithHostNamespace(ns) → 与宿主机共享某个 namespace
//   WithResources(res)    → 设置 cgroup 资源限制
//   WithAnnotations(map)  → 添加 OCI annotations
//   WithCgroup(path)      → 设置 cgroup 路径
func ApplyOpts(ctx context.Context, client Client, c *containers.Container, s *Spec, opts ...SpecOpts) error {
	for _, o := range opts {
		if err := o(ctx, client, c, s); err != nil {
			return err
		}
	}

	return nil
}

func defaultUnixCaps() []string {
	return []string{
		"CAP_CHOWN",
		"CAP_DAC_OVERRIDE",
		"CAP_FSETID",
		"CAP_FOWNER",
		"CAP_MKNOD",
		"CAP_NET_RAW",
		"CAP_SETGID",
		"CAP_SETUID",
		"CAP_SETFCAP",
		"CAP_SETPCAP",
		"CAP_NET_BIND_SERVICE",
		"CAP_SYS_CHROOT",
		"CAP_KILL",
		"CAP_AUDIT_WRITE",
	}
}

func defaultUnixNamespaces() []specs.LinuxNamespace {
	return []specs.LinuxNamespace{
		{
			Type: specs.PIDNamespace,
		},
		{
			Type: specs.IPCNamespace,
		},
		{
			Type: specs.UTSNamespace,
		},
		{
			Type: specs.MountNamespace,
		},
		{
			Type: specs.NetworkNamespace,
		},
	}
}

func populateDefaultUnixSpec(ctx context.Context, s *Spec, id string) error {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return err
	}

	*s = Spec{
		Version: specs.Version,
		Root: &specs.Root{
			Path: defaultRootfsPath,
		},
		Process: &specs.Process{
			Cwd:             "/",
			NoNewPrivileges: true,
			User: specs.User{
				UID: 0,
				GID: 0,
			},
			Capabilities: &specs.LinuxCapabilities{
				Bounding:    defaultUnixCaps(),
				Permitted:   defaultUnixCaps(),
				Inheritable: defaultUnixCaps(),
				Effective:   defaultUnixCaps(),
			},
			Rlimits: []specs.POSIXRlimit{
				{
					Type: "RLIMIT_NOFILE",
					Hard: uint64(1024),
					Soft: uint64(1024),
				},
			},
		},
		Linux: &specs.Linux{
			MaskedPaths: []string{
				"/proc/acpi",
				"/proc/asound",
				"/proc/kcore",
				"/proc/keys",
				"/proc/latency_stats",
				"/proc/timer_list",
				"/proc/timer_stats",
				"/proc/sched_debug",
				"/sys/firmware",
				"/proc/scsi",
			},
			ReadonlyPaths: []string{
				"/proc/bus",
				"/proc/fs",
				"/proc/irq",
				"/proc/sys",
				"/proc/sysrq-trigger",
			},
			CgroupsPath: filepath.Join("/", ns, id),
			Resources: &specs.LinuxResources{
				Devices: []specs.LinuxDeviceCgroup{
					{
						Allow:  false,
						Access: rwm,
					},
				},
			},
			Namespaces: defaultUnixNamespaces(),
		},
	}
	s.Mounts = defaultMounts()
	return nil
}

func populateDefaultWindowsSpec(ctx context.Context, s *Spec, id string) error {
	*s = Spec{
		Version: specs.Version,
		Root:    &specs.Root{},
		Process: &specs.Process{
			Cwd: `C:\`,
		},
		Windows: &specs.Windows{},
	}
	return nil
}
