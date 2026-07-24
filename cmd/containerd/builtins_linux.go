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

// wy: 🚀 Linux 平台专用插件导入文件
// 通过 Go blank import（_ "pkg"）机制，在编译期将各插件包纳入程序
// 每个被 import 的包都有 init() 函数，会调用 plugin.Register() 注册自己
//
// 这意味着: 如果要从 containerd 中移除某个功能，只需删除对应的 import 行
// 如果要用自定义的 snapshotter 替换默认的，在这里 import 自己的插件包即可
package main

import (
	// wy: cgroups 指标采集——监控容器的 CPU/内存/IO 使用量
	_ "github.com/containerd/containerd/metrics/cgroups"    // cgroups v1 (legacy)
	_ "github.com/containerd/containerd/metrics/cgroups/v2" // cgroups v2 (unified)

	// wy: Runtime 实现
	_ "github.com/containerd/containerd/runtime/v1/linux" // v1 shim（废弃，保留兼容）
	_ "github.com/containerd/containerd/runtime/v2"       // 🚀 v2 runtime（TaskManager）——管理 shim 进程
	_ "github.com/containerd/containerd/runtime/v2/runc/options" // runc 运行时配置选项类型

	// wy: 文件系统快照器
	_ "github.com/containerd/containerd/snapshots/native/plugin" // native: 直接文件拷贝（性能差但兼容性好）
	_ "github.com/containerd/containerd/snapshots/overlay/plugin" // 🚀 overlayfs: 联合文件系统（Linux 默认，高性能）
)
