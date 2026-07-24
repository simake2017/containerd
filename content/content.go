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

package content

import (
	"context"
	"io"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// ReaderAt 扩展了标准 io.ReaderAt，增加了 Size 和 Close
// wy: Content Store 的读接口——支持随机读取 blob 的任意偏移
type ReaderAt interface {
	io.ReaderAt
	io.Closer
	Size() int64
}

// Provider 提供内容读取能力
// wy: 🚀 Content Store 的读侧接口
// 通过 OCI Descriptor（包含 digest + size）定位并读取 blob
// 默认实现: content/local.Store（从本地文件系统读取）
type Provider interface {
	// ReaderAt 通过 digest 获取 blob 的随机读取器
	// 底层实现: 打开 /var/lib/containerd/.../blobs/sha256/<hex> 文件
	ReaderAt(ctx context.Context, desc ocispec.Descriptor) (ReaderAt, error)
}

// Ingester 提供内容写入能力
// wy: 🚀 Content Store 的写侧接口
// 写入流程: Writer() 获取 writer → Write(data) → Commit(digest)
// 默认实现: content/local.Store
type Ingester interface {
	// Writer 创建一个写入器
	// WithRef 指定写入引用名（用于跟踪写入进度和断点续传）
	Writer(ctx context.Context, opts ...WriterOpt) (Writer, error)
}

// Info holds content specific information
//
// TODO(stevvooe): Consider a very different name for this struct. Info is way
// to general. It also reads very weird in certain context, like pluralization.
type Info struct {
	Digest    digest.Digest
	Size      int64
	CreatedAt time.Time
	UpdatedAt time.Time
	Labels    map[string]string
}

// Status of a content operation
type Status struct {
	Ref       string
	Offset    int64
	Total     int64
	Expected  digest.Digest
	StartedAt time.Time
	UpdatedAt time.Time
}

// WalkFunc defines the callback for a blob walk.
type WalkFunc func(Info) error

// Manager provides methods for inspecting, listing and removing content.
type Manager interface {
	// Info will return metadata about content available in the content store.
	//
	// If the content is not present, ErrNotFound will be returned.
	Info(ctx context.Context, dgst digest.Digest) (Info, error)

	// Update updates mutable information related to content.
	// If one or more fieldpaths are provided, only those
	// fields will be updated.
	// Mutable fields:
	//  labels.*
	Update(ctx context.Context, info Info, fieldpaths ...string) (Info, error)

	// Walk will call fn for each item in the content store which
	// match the provided filters. If no filters are given all
	// items will be walked.
	Walk(ctx context.Context, fn WalkFunc, filters ...string) error

	// Delete removes the content from the store.
	Delete(ctx context.Context, dgst digest.Digest) error
}

// IngestManager provides methods for managing ingests.
type IngestManager interface {
	// Status returns the status of the provided ref.
	Status(ctx context.Context, ref string) (Status, error)

	// ListStatuses returns the status of any active ingestions whose ref match the
	// provided regular expression. If empty, all active ingestions will be
	// returned.
	ListStatuses(ctx context.Context, filters ...string) ([]Status, error)

	// Abort completely cancels the ingest operation targeted by ref.
	Abort(ctx context.Context, ref string) error
}

// Writer handles the write of content into a content store
type Writer interface {
	// Close closes the writer, if the writer has not been
	// committed this allows resuming or aborting.
	// Calling Close on a closed writer will not error.
	io.WriteCloser

	// Digest may return empty digest or panics until committed.
	Digest() digest.Digest

	// Commit commits the blob (but no roll-back is guaranteed on an error).
	// size and expected can be zero-value when unknown.
	// Commit always closes the writer, even on error.
	// ErrAlreadyExists aborts the writer.
	Commit(ctx context.Context, size int64, expected digest.Digest, opts ...Opt) error

	// Status returns the current state of write
	Status() (Status, error)

	// Truncate updates the size of the target blob
	Truncate(size int64) error
}

// Store 是内容存储的完整接口——组合了读、写、管理、 ingest 四大能力
// wy: 🚀 这是 containerd 的 CAS（Content Addressable Storage）核心接口
// 默认实现: content/local.Store
//
// 存储结构（文件系统布局）:
//   /var/lib/containerd/io.containerd.content.v1.content/
//     ├── blobs/
//     │   └── sha256/
//     │       ├── <hex-digest-1>  ← 镜像 layer tar.gz
//     │       ├── <hex-digest-2>  ← image config JSON
//     │       └── <hex-digest-3>  ← manifest JSON
//     └── ingest/
//         └── <ref>/              ← 进行中的写入（commit 后移动到 blobs/）
//
// 所有 blob 以 digest 为文件名，天然去重：
// 同一个 layer 被多个镜像引用时，磁盘上只存一份
type Store interface {
	Manager      // wy: Info/Update/Walk/Delete（管理已有内容）
	Provider     // wy: ReaderAt（读取内容）
	IngestManager // wy: Status/ListStatuses/Abort（管理进行中的写入）
	Ingester     // wy: Writer（开始新写入）
}

// Opt is used to alter the mutable properties of content
type Opt func(*Info) error

// WithLabels allows labels to be set on content
func WithLabels(labels map[string]string) Opt {
	return func(info *Info) error {
		info.Labels = labels
		return nil
	}
}

// WriterOpts is internally used by WriterOpt.
type WriterOpts struct {
	Ref  string
	Desc ocispec.Descriptor
}

// WriterOpt is used for passing options to Ingester.Writer.
type WriterOpt func(*WriterOpts) error

// WithDescriptor specifies an OCI descriptor.
// Writer may optionally use the descriptor internally for resolving
// the location of the actual data.
// Write does not require any field of desc to be set.
// If the data size is unknown, desc.Size should be set to 0.
// Some implementations may also accept negative values as "unknown".
func WithDescriptor(desc ocispec.Descriptor) WriterOpt {
	return func(opts *WriterOpts) error {
		opts.Desc = desc
		return nil
	}
}

// WithRef specifies a ref string.
func WithRef(ref string) WriterOpt {
	return func(opts *WriterOpts) error {
		opts.Ref = ref
		return nil
	}
}
