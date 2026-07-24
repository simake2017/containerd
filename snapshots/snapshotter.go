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

package snapshots

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/containerd/containerd/mount"
)

const (
	// UnpackKeyPrefix is the beginning of the key format used for snapshots that will have
	// image content unpacked into them.
	UnpackKeyPrefix = "extract"
	// UnpackKeyFormat is the format for the snapshotter keys used for extraction
	UnpackKeyFormat       = UnpackKeyPrefix + "-%s %s"
	inheritedLabelsPrefix = "containerd.io/snapshot/"
	labelSnapshotRef      = "containerd.io/snapshot.ref"
)

// Kind 标识快照的类型
// wy: 🚀 三种快照类型的含义:
//
//   KindActive: 可写快照——通过 Prepare() 创建
//     用于: 容器的可写层（容器运行时的读写操作都在此层）
//     特点: 不能作为 parent，必须先 Commit 为 Committed 后才能被引用
//
//   KindCommitted: 只读快照——通过 Commit() 创建
//     用于: 镜像层（每个 image layer 对应一个 Committed 快照）
//     特点: 可以作为 parent，被 Active 快照或 View 快照引用
//
//   KindView: 只读视图——通过 View() 创建
//     用于: 挂载某个 Committed 快照进行只读访问（如查看镜像内容）
//
// 生命周期:
//   镜像拉取: Prepare(active) → 解压 tar → Commit(committed)
//   容器创建: Prepare(active, parent=最后一层 committed) → 容器可写层
//   镜像查看: View(view, parent=committed) → 只读挂载
type Kind uint8

// definitions of snapshot kinds
const (
	KindUnknown Kind = iota
	KindView       // wy: 只读视图
	KindActive     // wy: 可写快照（容器的可写层）
	KindCommitted  // wy: 只读快照（镜像层）
)

// ParseKind parses the provided string into a Kind
//
// If the string cannot be parsed KindUnknown is returned
func ParseKind(s string) Kind {
	s = strings.ToLower(s)
	switch s {
	case "view":
		return KindView
	case "active":
		return KindActive
	case "committed":
		return KindCommitted
	}

	return KindUnknown
}

// String returns the string representation of the Kind
func (k Kind) String() string {
	switch k {
	case KindView:
		return "View"
	case KindActive:
		return "Active"
	case KindCommitted:
		return "Committed"
	}

	return "Unknown"
}

// MarshalJSON the Kind to JSON
func (k Kind) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.String())
}

// UnmarshalJSON the Kind from JSON
func (k *Kind) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	*k = ParseKind(s)
	return nil
}

// Info provides information about a particular snapshot.
// JSON marshallability is supported for interactive with tools like ctr,
type Info struct {
	Kind   Kind   // active or committed snapshot
	Name   string // name or key of snapshot
	Parent string `json:",omitempty"` // name of parent snapshot

	// Labels for a snapshot.
	//
	// Note: only labels prefixed with `containerd.io/snapshot/` will be inherited by the
	// snapshotter's `Prepare`, `View`, or `Commit` calls.
	Labels  map[string]string `json:",omitempty"`
	Created time.Time         `json:",omitempty"` // Created time
	Updated time.Time         `json:",omitempty"` // Last update time
}

// Usage defines statistics for disk resources consumed by the snapshot.
//
// These resources only include the resources consumed by the snapshot itself
// and does not include resources usage by the parent.
type Usage struct {
	Inodes int64 // number of inodes in use.
	Size   int64 // provides usage, in bytes, of snapshot
}

// Add the provided usage to the current usage
func (u *Usage) Add(other Usage) {
	u.Size += other.Size

	// TODO(stevvooe): assumes independent inodes, but provides and upper
	// bound. This should be pretty close, assuming the inodes for a
	// snapshot are roughly unique to it. Don't trust this assumption.
	u.Inodes += other.Inodes
}

// WalkFunc defines the callback for a snapshot walk.
type WalkFunc func(context.Context, Info) error

// Snapshotter defines the methods required to implement a snapshot snapshotter for
// allocating, snapshotting and mounting filesystem changesets. The model works
// by building up sets of changes with parent-child relationships.
//
// A snapshot represents a filesystem state. Every snapshot has a parent, where
// the empty parent is represented by the empty string. A diff can be taken
// between a parent and its snapshot to generate a classic layer.
//
// An active snapshot is created by calling `Prepare`. After mounting, changes
// can be made to the snapshot. The act of committing creates a committed
// snapshot. The committed snapshot will get the parent of active snapshot. The
// committed snapshot can then be used as a parent. Active snapshots can never
// act as a parent.
//
// Snapshots are best understood by their lifecycle. Active snapshots are
// always created with Prepare or View. Committed snapshots are always created
// with Commit.  Active snapshots never become committed snapshots and vice
// versa. All snapshots may be removed.
//
// For consistency, we define the following terms to be used throughout this
// interface for snapshotter implementations:
//
// 	`ctx` - refers to a context.Context
// 	`key` - refers to an active snapshot
// 	`name` - refers to a committed snapshot
// 	`parent` - refers to the parent in relation
//
// Most methods take various combinations of these identifiers. Typically,
// `name` and `parent` will be used in cases where a method *only* takes
// committed snapshots. `key` will be used to refer to active snapshots in most
// cases, except where noted. All variables used to access snapshots use the
// same key space. For example, an active snapshot may not share the same key
// with a committed snapshot.
//
// We cover several examples below to demonstrate the utility of a snapshot
// snapshotter.
//
// Importing a Layer
//
// To import a layer, we simply have the Snapshotter provide a list of
// mounts to be applied such that our dst will capture a changeset. We start
// out by getting a path to the layer tar file and creating a temp location to
// unpack it to:
//
//	layerPath, tmpDir := getLayerPath(), mkTmpDir() // just a path to layer tar file.
//
// We start by using a Snapshotter to Prepare a new snapshot transaction, using a
// key and descending from the empty parent "". To prevent our layer from being
// garbage collected during unpacking, we add the `containerd.io/gc.root` label:
//
//	noGcOpt := snapshots.WithLabels(map[string]string{
//		"containerd.io/gc.root": time.Now().UTC().Format(time.RFC3339),
//	})
//	mounts, err := snapshotter.Prepare(ctx, key, "", noGcOpt)
// 	if err != nil { ... }
//
// We get back a list of mounts from Snapshotter.Prepare, with the key identifying
// the active snapshot. Mount this to the temporary location with the
// following:
//
//	if err := mount.All(mounts, tmpDir); err != nil { ... }
//
// Once the mounts are performed, our temporary location is ready to capture
// a diff. In practice, this works similar to a filesystem transaction. The
// next step is to unpack the layer. We have a special function unpackLayer
// that applies the contents of the layer to target location and calculates the
// DiffID of the unpacked layer (this is a requirement for docker
// implementation):
//
//	layer, err := os.Open(layerPath)
//	if err != nil { ... }
// 	digest, err := unpackLayer(tmpLocation, layer) // unpack into layer location
// 	if err != nil { ... }
//
// When the above completes, we should have a filesystem the represents the
// contents of the layer. Careful implementations should verify that digest
// matches the expected DiffID. When completed, we unmount the mounts:
//
//	unmount(mounts) // optional, for now
//
// Now that we've verified and unpacked our layer, we commit the active
// snapshot to a name. For this example, we are just going to use the layer
// digest, but in practice, this will probably be the ChainID. This also removes
// the active snapshot:
//
//	if err := snapshotter.Commit(ctx, digest.String(), key, noGcOpt); err != nil { ... }
//
// Now, we have a layer in the Snapshotter that can be accessed with the digest
// provided during commit.
//
// Importing the Next Layer
//
// Making a layer depend on the above is identical to the process described
// above except that the parent is provided as parent when calling
// Manager.Prepare, assuming a clean, unique key identifier:
//
// 	mounts, err := snapshotter.Prepare(ctx, key, parentDigest, noGcOpt)
//
// We then mount, apply and commit, as we did above. The new snapshot will be
// based on the content of the previous one.
//
// Running a Container
//
// To run a container, we simply provide Snapshotter.Prepare the committed image
// snapshot as the parent. After mounting, the prepared path can
// be used directly as the container's filesystem:
//
// 	mounts, err := snapshotter.Prepare(ctx, containerKey, imageRootFSChainID)
//
// The returned mounts can then be passed directly to the container runtime. If
// one would like to create a new image from the filesystem, Manager.Commit is
// called:
//
// 	if err := snapshotter.Commit(ctx, newImageSnapshot, containerKey); err != nil { ... }
//
// Alternatively, for most container runs, Snapshotter.Remove will be called to
// signal the Snapshotter to abandon the changes.
// Snapshotter 定义文件系统快照器的接口
// wy: 🚀 这是 containerd 文件系统层叠管理的核心抽象
// 默认实现: overlayfs（snapshots/overlay）
// 其他实现: native（直接拷贝）、devmapper（块设备 thin-provisioning）、stargz（延迟加载）
//
// 快照的生命周期:
//   镜像拉取时:
//     Prepare(active, parent="") → tar 解压 → Commit(committed, "sha256-xxx")
//     Prepare(active, parent=layer1) → tar 解压 → Commit(committed, "sha256-yyy")
//     ... 逐层构建
//
//   容器创建时:
//     Prepare(active, parent=最后一层committed) → 返回 overlay mounts → 作为容器可写层
//
//   容器删除时:
//     Remove(active key) → 删除 upperdir + workdir
//
// overlayfs 实现细节:
//   每个 Committed 快照 = 一个只读目录（lowerdir 的一层）
//   每个 Active 快照 = upperdir + workdir（可写层）
//   Mounts() 返回的 overlay mount 将所有层合并为统一视图:
//     mount -t overlay overlay -o lowerdir=L3:L2:L1,upperdir=U,workdir=W /rootfs
type Snapshotter interface {
	// Stat 查询快照信息
	Stat(ctx context.Context, key string) (Info, error)

	// Update 更新快照的可变属性（如 labels）
	Update(ctx context.Context, info Info, fieldpaths ...string) (Info, error)

	// Usage 返回快照的磁盘使用量（不包含 parent 的使用量）
	Usage(ctx context.Context, key string) (Usage, error)

	// Mounts 返回 Active 快照的挂载参数
	// wy: 🚀 关键方法——返回的 mount.Mount 可直接传给 mount.All() 执行挂载
	// overlayfs 返回: {Type: "overlay", Source: "overlay", Options: ["lowerdir=...", "upperdir=...", "workdir=..."]}
	Mounts(ctx context.Context, key string) ([]mount.Mount, error)

	// Prepare 创建 Active 快照（可写层）
	// wy: 🚀 最常用的方法:
	//   - 镜像拉取时: Prepare("extract-xxx", parentLayer) → 准备解压目标
	//   - 容器创建时: Prepare("container-key", imageTopLayer) → 创建容器可写层
	// 返回的 mounts 用于将快照挂载到临时目录进行操作
	Prepare(ctx context.Context, key, parent string, opts ...Opt) ([]mount.Mount, error)

	// View 创建只读视图（类似 Prepare 但不可写、不可 Commit）
	View(ctx context.Context, key, parent string, opts ...Opt) ([]mount.Mount, error)

	// Commit 将 Active 快照提交为 Committed 快照
	// wy: 🚀 镜像拉取时的关键步骤:
	//   1. Prepare("extract-1", "") → 解压 layer1.tar
	//   2. Commit("sha256-layer1", "extract-1") → 将解压结果标记为只读层
	//   3. Prepare("extract-2", "sha256-layer1") → 基于第 1 层准备第 2 层
	//   4. Commit("sha256-layer2", "extract-2") → 提交第 2 层
	Commit(ctx context.Context, name, key string, opts ...Opt) error

	// Remove 删除快照
	// wy: 容器删除时调用，清理 upperdir/workdir
	Remove(ctx context.Context, key string) error

	// Walk will call the provided function for each snapshot in the
	// snapshotter which match the provided filters. If no filters are
	// given all items will be walked.
	// Filters:
	//  name
	//  parent
	//  kind (active,view,committed)
	//  labels.(label)
	Walk(ctx context.Context, fn WalkFunc, filters ...string) error

	// Close releases the internal resources.
	//
	// Close is expected to be called on the end of the lifecycle of the snapshotter,
	// but not mandatory.
	//
	// Close returns nil when it is already closed.
	Close() error
}

// Cleaner defines a type capable of performing asynchronous resource cleanup.
// The Cleaner interface should be used by snapshotters which implement fast
// removal and deferred resource cleanup. This prevents snapshots from needing
// to perform lengthy resource cleanup before acknowledging a snapshot key
// has been removed and available for re-use. This is also useful when
// performing multi-key removal with the intent of cleaning up all the
// resources after each snapshot key has been removed.
type Cleaner interface {
	Cleanup(ctx context.Context) error
}

// Opt allows setting mutable snapshot properties on creation
type Opt func(info *Info) error

// WithLabels appends labels to a created snapshot
func WithLabels(labels map[string]string) Opt {
	return func(info *Info) error {
		if info.Labels == nil {
			info.Labels = make(map[string]string)
		}

		for k, v := range labels {
			info.Labels[k] = v
		}

		return nil
	}
}

// FilterInheritedLabels filters the provided labels by removing any key which
// isn't a snapshot label. Snapshot labels have a prefix of "containerd.io/snapshot/"
// or are the "containerd.io/snapshot.ref" label.
func FilterInheritedLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}

	filtered := make(map[string]string)
	for k, v := range labels {
		if k == labelSnapshotRef || strings.HasPrefix(k, inheritedLabelsPrefix) {
			filtered[k] = v
		}
	}
	return filtered
}
