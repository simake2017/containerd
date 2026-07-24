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

package containerd

import (
	"context"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/containerd/remotes/docker/schema1"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Pull 从远程仓库拉取镜像到 containerd 的 Content Store 并解包到 Snapshotter
// wy: 🚀 这是 containerd 最复杂的 Client 方法之一，完整流程:
//
//   1. 解析镜像引用（如 "docker.io/library/nginx:latest"）
//   2. 认证: 向 registry 获取 Bearer Token
//   3. 下载 manifest list（多平台索引）
//   4. 按平台过滤，选择目标 manifest
//   5. 下载 manifest + image config + 所有 layer blobs
//   6. 并行解包: 边下载边将 layer tar 解压到 Snapshotter（overlayfs）
//   7. 创建 Image 记录到 ImageService（BoltDB）
//
// 典型用法:
//   image, err := client.Pull(ctx, "docker.io/library/nginx:latest",
//       containerd.WithPullUnpack,              // 拉取后解包
//       containerd.WithPullSnapshotter("overlayfs"), // 指定快照器
//   )
func (c *Client) Pull(ctx context.Context, ref string, opts ...RemoteOpt) (_ Image, retErr error) {
	pullCtx := defaultRemoteContext()
	// wy: 应用拉取选项（平台、并发数、解包等）
	for _, o := range opts {
		if err := o(c, pullCtx); err != nil {
			return nil, err
		}
	}

	// wy: 设置平台匹配器——决定拉取哪个平台的镜像
	// 多平台镜像（如 nginx:latest 有 amd64/arm64/arm/v7）只拉取匹配的
	if pullCtx.PlatformMatcher == nil {
		if len(pullCtx.Platforms) > 1 {
			return nil, errors.New("cannot pull multiplatform image locally, try Fetch")
		} else if len(pullCtx.Platforms) == 0 {
			pullCtx.PlatformMatcher = c.platform // wy: 默认: 当前机器的 OS/Arch
		} else {
			p, err := platforms.Parse(pullCtx.Platforms[0])
			if err != nil {
				return nil, errors.Wrapf(err, "invalid platform %s", pullCtx.Platforms[0])
			}
			pullCtx.PlatformMatcher = platforms.Only(p)
		}
	}

	// wy: 创建 Lease 防止拉取过程中 GC 回收临时 content
	ctx, done, err := c.WithLease(ctx)
	if err != nil {
		return nil, err
	}
	defer done(ctx)

	var unpacks int32
	var unpackEg *errgroup.Group
	var unpackWrapper func(f images.Handler) images.Handler

	if pullCtx.Unpack {
		// wy: 🚀 创建解包器——实现"边下载边解包"的流水线
		// 解包器的 handler 包裹在 fetch handler 外层:
		//   下载完一个 layer blob → 立即触发解包（tar 解压到 snapshotter）
		//   同时继续下载下一个 layer
		u, err := c.newUnpacker(ctx, pullCtx)
		if err != nil {
			return nil, errors.Wrap(err, "create unpacker")
		}
		unpackWrapper, unpackEg = u.handlerWrapper(ctx, pullCtx, &unpacks)
		defer func() {
			if err := unpackEg.Wait(); err != nil {
				if retErr == nil {
					retErr = errors.Wrap(err, "unpack")
				}
			}
		}()
		wrapper := pullCtx.HandlerWrapper
		pullCtx.HandlerWrapper = func(h images.Handler) images.Handler {
			if wrapper == nil {
				return unpackWrapper(h)
			}
			return unpackWrapper(wrapper(h))
		}
	}

	// wy: 🚀 核心下载流程——递归分发下载所有 descriptor
	img, err := c.fetch(ctx, pullCtx, ref, 1)
	if err != nil {
		return nil, err
	}

	// wy: 等待所有解包操作完成（包括 blob 下载 + tar 解压）
	if pullCtx.Unpack {
		if unpackEg != nil {
			if err := unpackEg.Wait(); err != nil {
				return nil, err
			}
		}
	}

	// wy: 创建 Image 记录到 ImageService（写入 BoltDB）
	img, err = c.createNewImage(ctx, img)
	if err != nil {
		return nil, err
	}

	i := NewImageWithPlatform(c, img, pullCtx.PlatformMatcher)

	if pullCtx.Unpack {
		if unpacks == 0 {
			// wy: 兼容 schema1 镜像——之前没有触发解包，补一次
			if err := i.Unpack(ctx, pullCtx.Snapshotter, pullCtx.UnpackOpts...); err != nil {
				return nil, errors.Wrapf(err, "failed to unpack image on snapshotter %s", pullCtx.Snapshotter)
			}
		}
	}

	return i, nil
}

// fetch 执行实际的镜像下载
// wy: 🚀 下载流程（以 docker.io/library/nginx:latest 为例）:
//
//   1. Resolve: HTTP GET registry → 获取 manifest list descriptor
//   2. Fetcher: 获取 HTTP fetcher 客户端
//   3. Dispatch: 递归遍历 descriptor 树，对每个执行 handler 链:
//
//      [manifest list] → FetchHandler(下载到 ContentStore)
//                      → ChildrenHandler(解析子 descriptor，按平台过滤)
//      [manifest]      → FetchHandler → ChildrenHandler(获取 config + layers)
//      [config JSON]   → FetchHandler
//      [layer 1 blob]  → FetchHandler → UnpackHandler(tar 解压到 snapshotter)
//      [layer 2 blob]  → FetchHandler → UnpackHandler
//      ...
//
//   每个 blob 写入路径: /var/lib/containerd/.../blobs/sha256/<digest>
func (c *Client) fetch(ctx context.Context, rCtx *RemoteContext, ref string, limit int) (images.Image, error) {
	store := c.ContentStore()

	// wy: Step 1: 解析镜像引用
	// "docker.io/library/nginx:latest" → (name="docker.io/library/nginx", descriptor)
	// 底层: HTTP GET registry 的 manifest 端点
	name, desc, err := rCtx.Resolver.Resolve(ctx, ref)
	if err != nil {
		return images.Image{}, errors.Wrapf(err, "failed to resolve reference %q", ref)
	}

	// wy: Step 2: 获取 Fetcher（HTTP 下载客户端）
	fetcher, err := rCtx.Resolver.Fetcher(ctx, name)
	if err != nil {
		return images.Image{}, errors.Wrapf(err, "failed to get fetcher for %q", name)
	}

	var (
		handler images.Handler

		isConvertible bool
		converterFunc func(context.Context, ocispec.Descriptor) (ocispec.Descriptor, error)
		limiter       *semaphore.Weighted
	)

	if desc.MediaType == images.MediaTypeDockerSchema1Manifest && rCtx.ConvertSchema1 {
		schema1Converter := schema1.NewConverter(store, fetcher)

		handler = images.Handlers(append(rCtx.BaseHandlers, schema1Converter)...)

		isConvertible = true

		converterFunc = func(ctx context.Context, _ ocispec.Descriptor) (ocispec.Descriptor, error) {
			return schema1Converter.Convert(ctx)
		}
	} else {
		// Get all the children for a descriptor
		childrenHandler := images.ChildrenHandler(store)
		// Set any children labels for that content
		childrenHandler = images.SetChildrenMappedLabels(store, childrenHandler, rCtx.ChildLabelMap)
		if rCtx.AllMetadata {
			// Filter manifests by platforms but allow to handle manifest
			// and configuration for not-target platforms
			childrenHandler = remotes.FilterManifestByPlatformHandler(childrenHandler, rCtx.PlatformMatcher)
		} else {
			// Filter children by platforms if specified.
			childrenHandler = images.FilterPlatforms(childrenHandler, rCtx.PlatformMatcher)
		}
		// Sort and limit manifests if a finite number is needed
		if limit > 0 {
			childrenHandler = images.LimitManifests(childrenHandler, rCtx.PlatformMatcher, limit)
		}

		// set isConvertible to true if there is application/octet-stream media type
		convertibleHandler := images.HandlerFunc(
			func(_ context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
				if desc.MediaType == docker.LegacyConfigMediaType {
					isConvertible = true
				}

				return []ocispec.Descriptor{}, nil
			},
		)

		appendDistSrcLabelHandler, err := docker.AppendDistributionSourceLabel(store, ref)
		if err != nil {
			return images.Image{}, err
		}

		handlers := append(rCtx.BaseHandlers,
			remotes.FetchHandler(store, fetcher),
			convertibleHandler,
			childrenHandler,
			appendDistSrcLabelHandler,
		)

		handler = images.Handlers(handlers...)

		converterFunc = func(ctx context.Context, desc ocispec.Descriptor) (ocispec.Descriptor, error) {
			return docker.ConvertManifest(ctx, store, desc)
		}
	}

	if rCtx.HandlerWrapper != nil {
		handler = rCtx.HandlerWrapper(handler)
	}

	if rCtx.MaxConcurrentDownloads > 0 {
		limiter = semaphore.NewWeighted(int64(rCtx.MaxConcurrentDownloads))
	}

	if err := images.Dispatch(ctx, handler, limiter, desc); err != nil {
		return images.Image{}, err
	}

	if isConvertible {
		if desc, err = converterFunc(ctx, desc); err != nil {
			return images.Image{}, err
		}
	}

	return images.Image{
		Name:   name,
		Target: desc,
		Labels: rCtx.Labels,
	}, nil
}

func (c *Client) createNewImage(ctx context.Context, img images.Image) (images.Image, error) {
	is := c.ImageService()
	for {
		if created, err := is.Create(ctx, img); err != nil {
			if !errdefs.IsAlreadyExists(err) {
				return images.Image{}, err
			}

			updated, err := is.Update(ctx, img)
			if err != nil {
				// if image was removed, try create again
				if errdefs.IsNotFound(err) {
					continue
				}
				return images.Image{}, err
			}

			img = updated
		} else {
			img = created
		}

		return img, nil
	}
}
