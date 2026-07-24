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

package rootfs

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"time"

	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// Layer 表示镜像一层的描述信息
// wy: 🚀 每层包含两个 descriptor:
//   Diff: 解压后的 tar diff 描述符（用于计算 chain ID）
//   Blob: 原始压缩 blob 描述符（实际从 Content Store 读取的数据）
// 例如: Blob 是 layer.tar.gz（compressed），Diff 是 layer.tar（uncompressed）
type Layer struct {
	Diff ocispec.Descriptor // wy: 解压后的 diff descriptor（digest 用于 chain ID 计算）
	Blob ocispec.Descriptor // wy: 原始压缩 blob descriptor（从 Content Store 读取）
}

// ApplyLayers 将所有镜像层依次应用到 Snapshotter，生成完整的 rootfs
// wy: 🚀 这是镜像解包的核心函数——将 Docker/OCI 镜像的各层 tar 包解压为文件系统快照
//
// 工作原理（以 3 层镜像为例）:
//   Layer 1 (base): Prepare("extract-1", "") → tar 解压 → Commit("chain-1", "extract-1")
//   Layer 2 (app):  Prepare("extract-2", "chain-1") → tar 解压 → Commit("chain-2", "extract-2")
//   Layer 3 (cfg):  Prepare("extract-3", "chain-2") → tar 解压 → Commit("chain-3", "extract-3")
//
// 最终 chain-3 包含了所有层的合并视图（通过 overlayfs lowerdir 链实现）
// chain ID 的计算: sha256(sha256(layer1) + sha256(layer2) + sha256(layer3))
func ApplyLayers(ctx context.Context, layers []Layer, sn snapshots.Snapshotter, a diff.Applier) (digest.Digest, error) {
	return ApplyLayersWithOpts(ctx, layers, sn, a, nil)
}

// ApplyLayersWithOpts 带选项的逐层应用
func ApplyLayersWithOpts(ctx context.Context, layers []Layer, sn snapshots.Snapshotter, a diff.Applier, applyOpts []diff.ApplyOpt) (digest.Digest, error) {
	// wy: 构建 chain: 每层的 Diff digest 组成有序数组
	chain := make([]digest.Digest, len(layers))
	for i, layer := range layers {
		chain[i] = layer.Diff.Digest
	}
	// wy: 计算最终的 Chain ID = sha256(digest1 + digest2 + ... + digestN)
	// Chain ID 是唯一标识一组层叠序列的指纹
	chainID := identity.ChainID(chain)

	// wy: 幂等性检查: 先检查最终层的 snapshot 是否已存在
	// 如果存在说明整个镜像已经解包过了，直接返回（避免重复解压）
	_, err := sn.Stat(ctx, chainID.String())
	if err != nil {
		if !errdefs.IsNotFound(err) {
			return "", errors.Wrapf(err, "failed to stat snapshot %s", chainID)
		}

		// wy: 不存在，开始逐层解包
		if err := applyLayers(ctx, layers, chain, sn, a, nil, applyOpts); err != nil && !errdefs.IsAlreadyExists(err) {
			return "", err
		}
	}

	return chainID, nil
}

// ApplyLayer applies a single layer on top of the given provided layer chain,
// using the provided snapshotter and applier. If the layer was unpacked true
// is returned, if the layer already exists false is returned.
func ApplyLayer(ctx context.Context, layer Layer, chain []digest.Digest, sn snapshots.Snapshotter, a diff.Applier, opts ...snapshots.Opt) (bool, error) {
	return ApplyLayerWithOpts(ctx, layer, chain, sn, a, opts, nil)
}

// ApplyLayerWithOpts applies a single layer on top of the given provided layer chain,
// using the provided snapshotter, applier, and apply opts. If the layer was unpacked true
// is returned, if the layer already exists false is returned.
func ApplyLayerWithOpts(ctx context.Context, layer Layer, chain []digest.Digest, sn snapshots.Snapshotter, a diff.Applier, opts []snapshots.Opt, applyOpts []diff.ApplyOpt) (bool, error) {
	var (
		chainID = identity.ChainID(append(chain, layer.Diff.Digest)).String()
		applied bool
	)
	if _, err := sn.Stat(ctx, chainID); err != nil {
		if !errdefs.IsNotFound(err) {
			return false, errors.Wrapf(err, "failed to stat snapshot %s", chainID)
		}

		if err := applyLayers(ctx, []Layer{layer}, append(chain, layer.Diff.Digest), sn, a, opts, applyOpts); err != nil {
			if !errdefs.IsAlreadyExists(err) {
				return false, err
			}
		} else {
			applied = true
		}
	}
	return applied, nil

}

// applyLayers 逐层应用镜像层到 Snapshotter 的内部实现
// wy: 🚀 核心逻辑（递归 + 迭代）:
//   1. 递归确保 parent 层已存在（如果 parent 不存在，先递归应用前面的层）
//   2. Prepare 一个 Active 快照（基于 parent）
//   3. 将 layer 的 tar 包通过 diff.Applier 解压到快照的挂载点
//   4. Commit 为 Committed 快照（以 chainID 为名称）
//
// overlayfs 视角:
//   Layer 1: lowerdir=/var/lib/.../snapshots/1/fs (base layer)
//   Layer 2: lowerdir=/var/lib/.../snapshots/2/fs:/var/lib/.../snapshots/1/fs
//   Layer 3: lowerdir=/var/lib/.../snapshots/3/fs:.../2/fs:.../1/fs
//   容器:    lowerdir=3:2:1, upperdir=/var/lib/.../snapshots/4/fs, workdir=...
func applyLayers(ctx context.Context, layers []Layer, chain []digest.Digest, sn snapshots.Snapshotter, a diff.Applier, opts []snapshots.Opt, applyOpts []diff.ApplyOpt) error {
	var (
		parent  = identity.ChainID(chain[:len(chain)-1]) // wy: 前一层的 chain ID
		chainID = identity.ChainID(chain)                 // wy: 当前层的 chain ID
		layer   = layers[len(layers)-1]                   // wy: 当前要应用的层
		diff    ocispec.Descriptor
		key     string
		mounts  []mount.Mount
		err     error
	)

	for {
		// wy: 生成唯一的快照 key（格式: "extract-<random> <chainID>"）
		key = fmt.Sprintf(snapshots.UnpackKeyFormat, uniquePart(), chainID)

		// wy: 🚀 Step 1: 从 parent 创建 Active 快照
		// 对于 overlayfs: 创建 upperdir + workdir，挂载时 lowerdir 指向 parent
		mounts, err = sn.Prepare(ctx, key, parent.String(), opts...)
		if err != nil {
			if errdefs.IsNotFound(err) && len(layers) > 1 {
				// wy: parent 不存在 → 递归应用前面的层（确保依赖链完整）
				if err := applyLayers(ctx, layers[:len(layers)-1], chain[:len(chain)-1], sn, a, opts, applyOpts); err != nil {
					if !errdefs.IsAlreadyExists(err) {
						return err
					}
				}
				layers = nil // wy: 标记前面的层已应用，不再重试
				continue
			} else if errdefs.IsAlreadyExists(err) {
				// wy: key 冲突（极低概率），换一个随机 key 重试
				continue
			}

			return errors.Wrapf(err, "failed to prepare extraction snapshot %q", key)

		}
		break
	}
	defer func() {
		if err != nil {
			if !errdefs.IsAlreadyExists(err) {
				log.G(ctx).WithError(err).WithField("key", key).Infof("apply failure, attempting cleanup")
			}

			if rerr := sn.Remove(ctx, key); rerr != nil {
				log.G(ctx).WithError(rerr).WithField("key", key).Warnf("extraction snapshot removal failed")
			}
		}
	}()

	diff, err = a.Apply(ctx, layer.Blob, mounts, applyOpts...)
	if err != nil {
		err = errors.Wrapf(err, "failed to extract layer %s", layer.Diff.Digest)
		return err
	}
	if diff.Digest != layer.Diff.Digest {
		err = errors.Errorf("wrong diff id calculated on extraction %q", diff.Digest)
		return err
	}

	if err = sn.Commit(ctx, chainID.String(), key, opts...); err != nil {
		err = errors.Wrapf(err, "failed to commit snapshot %s", key)
		return err
	}

	return nil
}

func uniquePart() string {
	t := time.Now()
	var b [3]byte
	// Ignore read failures, just decreases uniqueness
	rand.Read(b[:])
	return fmt.Sprintf("%d-%s", t.Nanosecond(), base64.URLEncoding.EncodeToString(b[:]))
}
