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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	goruntime "runtime"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/containerd/api/services/tasks/v1"
	"github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/oci"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/rootfs"
	"github.com/containerd/containerd/runtime/linux/runctypes"
	"github.com/containerd/containerd/runtime/v2/runc/options"
	"github.com/containerd/typeurl"
	google_protobuf "github.com/gogo/protobuf/types"
	digest "github.com/opencontainers/go-digest"
	is "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/pkg/errors"
)

// UnknownExitStatus is returned when containerd is unable to
// determine the exit status of a process. This can happen if the process never starts
// or if an error was encountered when obtaining the exit status, it is set to 255.
const UnknownExitStatus = 255

const (
	checkpointDateFormat = "01-02-2006-15:04:05"
	checkpointNameFormat = "containerd.io/checkpoint/%s:%s"
)

// Status returns process status and exit information
type Status struct {
	// Status of the process
	Status ProcessStatus
	// ExitStatus returned by the process
	ExitStatus uint32
	// ExitedTime is the time at which the process died
	ExitTime time.Time
}

// ProcessInfo provides platform specific process information
type ProcessInfo struct {
	// Pid is the process ID
	Pid uint32
	// Info includes additional process information
	// Info varies by platform
	Info *google_protobuf.Any
}

// ProcessStatus returns a human readable status for the Process representing its current status
type ProcessStatus string

const (
	// Running indicates the process is currently executing
	Running ProcessStatus = "running"
	// Created indicates the process has been created within containerd but the
	// user's defined process has not started
	Created ProcessStatus = "created"
	// Stopped indicates that the process has ran and exited
	Stopped ProcessStatus = "stopped"
	// Paused indicates that the process is currently paused
	Paused ProcessStatus = "paused"
	// Pausing indicates that the process is currently switching from a
	// running state into a paused state
	Pausing ProcessStatus = "pausing"
	// Unknown indicates that we could not determine the status from the runtime
	Unknown ProcessStatus = "unknown"
)

// IOCloseInfo allows specific io pipes to be closed on a process
type IOCloseInfo struct {
	Stdin bool
}

// IOCloserOpts allows the caller to set specific pipes as closed on a process
type IOCloserOpts func(*IOCloseInfo)

// WithStdinCloser closes the stdin of a process
func WithStdinCloser(r *IOCloseInfo) {
	r.Stdin = true
}

// CheckpointTaskInfo allows specific checkpoint information to be set for the task
type CheckpointTaskInfo struct {
	Name string
	// ParentCheckpoint is the digest of a parent checkpoint
	ParentCheckpoint digest.Digest
	// Options hold runtime specific settings for checkpointing a task
	Options interface{}

	runtime string
}

// Runtime name for the container
func (i *CheckpointTaskInfo) Runtime() string {
	return i.runtime
}

// CheckpointTaskOpts allows the caller to set checkpoint options
type CheckpointTaskOpts func(*CheckpointTaskInfo) error

// TaskInfo sets options for task creation
type TaskInfo struct {
	// Checkpoint is the Descriptor for an existing checkpoint that can be used
	// to restore a task's runtime and memory state
	Checkpoint *types.Descriptor
	// RootFS is a list of mounts to use as the task's root filesystem
	RootFS []mount.Mount
	// Options hold runtime specific settings for task creation
	Options interface{}
	runtime string
}

// Runtime name for the container
func (i *TaskInfo) Runtime() string {
	return i.runtime
}

// Task 是容器的运行实例——封装了进程生命周期管理
// wy: 🚀 Task 与 Container 的关系:
//   - Container = 元数据对象（ID、Image、Spec、SnapshotKey），不运行进程
//   - Task = 运行实例（有 PID、状态、IO），对应 shim 进程管理的容器
//   - 一个 Container 同一时刻最多有一个 Task
//
// Task 的生命周期状态机:
//   Created → Running → Stopped
//              ↕
//            Paused ↔ Pausing
//
// 所有 Task 方法最终都转化为 gRPC 调用:
//   Client Task → gRPC → Daemon TaskService → TTRPC → Shim → runc
type Task interface {
	Process // wy: 继承基础进程接口: Start, Kill, Wait, Delete, CloseIO, Resize, IO, Status

	// Pause suspends the execution of the task
	// wy: 暂停容器 → 底层: runc pause（通过 cgroup freezer 冻结所有进程）
	Pause(context.Context) error
	// Resume the execution of the task
	// wy: 恢复容器 → 底层: runc resume（解冻 cgroup freezer）
	Resume(context.Context) error
	// Exec creates a new process inside the task
	// wy: 在运行中的容器内创建新进程 → 底层: runc exec --process <spec.json>
	Exec(context.Context, string, *specs.Process, cio.Creator) (Process, error)
	// Pids returns a list of system specific process ids inside the task
	// wy: 列出容器内所有进程 PID → 底层: 读取 cgroup 的 cgroup.procs 文件
	Pids(context.Context) ([]ProcessInfo, error)
	// Checkpoint serializes the runtime and memory information of a task into an
	// OCI Index that can be pushed and pulled from a remote resource.
	//
	// Additional software like CRIU maybe required to checkpoint and restore tasks
	// NOTE: Checkpoint supports to dump task information to a directory, in this way,
	// an empty OCI Index will be returned.
	// wy: 容器检查点（热迁移）→ 底层: CRIU 序列化进程内存状态到文件
	Checkpoint(context.Context, ...CheckpointTaskOpts) (Image, error)
	// Update modifies executing tasks with updated settings
	// wy: 运行时更新容器资源限制 → 底层: 写入 cgroup 文件（如 memory.limit_in_bytes）
	Update(context.Context, ...UpdateTaskOpts) error
	// LoadProcess loads a previously created exec'd process
	LoadProcess(context.Context, string, cio.Attach) (Process, error)
	// Metrics returns task metrics for runtime specific metrics
	//
	// The metric types are generic to containerd and change depending on the runtime
	// For the built in Linux runtime, github.com/containerd/cgroups.Metrics
	// are returned in protobuf format
	// wy: 获取容器指标 → 底层: 读取 cgroup 统计文件（cpu.stat, memory.stat 等）
	Metrics(context.Context) (*types.Metric, error)
	// Spec returns the current OCI specification for the task
	Spec(context.Context) (*oci.Spec, error)
}

var _ = (Task)(&task{})

type task struct {
	client *Client
	c      Container

	io  cio.IO
	id  string
	pid uint32
}

// Spec returns the current OCI specification for the task
func (t *task) Spec(ctx context.Context) (*oci.Spec, error) {
	return t.c.Spec(ctx)
}

// ID of the task
func (t *task) ID() string {
	return t.id
}

// Pid returns the pid or process id for the task
func (t *task) Pid() uint32 {
	return t.pid
}

// Start 启动容器的 init 进程
// wy: 🚀 调用链: Client → gRPC → Daemon → TTRPC → Shim → runc start <id>
// 此前 NewTask() 已执行了 runc create（设置了 namespaces/cgroups/rootfs），
// init 进程处于阻塞状态（等待 start 信号）
// Start 就是通知 init 进程开始执行
func (t *task) Start(ctx context.Context) error {
	r, err := t.client.TaskService().Start(ctx, &tasks.StartRequest{
		ContainerID: t.id,
	})
	if err != nil {
		if t.io != nil {
			t.io.Cancel()
			t.io.Close()
		}
		return errdefs.FromGRPC(err)
	}
	t.pid = r.Pid // wy: 更新 PID（Start 后 runc 返回真实的 init 进程 PID）
	return nil
}

// Kill 向容器发送信号
// wy: 🚀 调用链: Client → gRPC → Daemon → TTRPC → Shim → runc kill [signal] [--all]
// 底层实现: runc 通过 cgroup 找到容器内所有 PID，调用 kill() 系统调用
// All=true 时发送信号给容器内所有进程（不仅仅是 init 进程）
func (t *task) Kill(ctx context.Context, s syscall.Signal, opts ...KillOpts) error {
	var i KillInfo
	for _, o := range opts {
		if err := o(ctx, &i); err != nil {
			return err
		}
	}
	_, err := t.client.TaskService().Kill(ctx, &tasks.KillRequest{
		Signal:      uint32(s),     // wy: 如 syscall.SIGTERM(15), syscall.SIGKILL(9)
		ContainerID: t.id,
		ExecID:      i.ExecID,     // wy: 空="" 表示 init 进程，非空表示特定 exec 进程
		All:         i.All,        // wy: true = 发送给容器内所有进程
	})
	if err != nil {
		return errdefs.FromGRPC(err)
	}
	return nil
}

func (t *task) Pause(ctx context.Context) error {
	_, err := t.client.TaskService().Pause(ctx, &tasks.PauseTaskRequest{
		ContainerID: t.id,
	})
	return errdefs.FromGRPC(err)
}

func (t *task) Resume(ctx context.Context) error {
	_, err := t.client.TaskService().Resume(ctx, &tasks.ResumeTaskRequest{
		ContainerID: t.id,
	})
	return errdefs.FromGRPC(err)
}

func (t *task) Status(ctx context.Context) (Status, error) {
	r, err := t.client.TaskService().Get(ctx, &tasks.GetRequest{
		ContainerID: t.id,
	})
	if err != nil {
		return Status{}, errdefs.FromGRPC(err)
	}
	return Status{
		Status:     ProcessStatus(strings.ToLower(r.Process.Status.String())),
		ExitStatus: r.Process.ExitStatus,
		ExitTime:   r.Process.ExitedAt,
	}, nil
}

func (t *task) Wait(ctx context.Context) (<-chan ExitStatus, error) {
	c := make(chan ExitStatus, 1)
	go func() {
		defer close(c)
		r, err := t.client.TaskService().Wait(ctx, &tasks.WaitRequest{
			ContainerID: t.id,
		})
		if err != nil {
			c <- ExitStatus{
				code: UnknownExitStatus,
				err:  err,
			}
			return
		}
		c <- ExitStatus{
			code:     r.ExitStatus,
			exitedAt: r.ExitedAt,
		}
	}()
	return c, nil
}

// Delete deletes the task and its runtime state
// it returns the exit status of the task and any errors that were encountered
// during cleanup
func (t *task) Delete(ctx context.Context, opts ...ProcessDeleteOpts) (*ExitStatus, error) {
	for _, o := range opts {
		if err := o(ctx, t); err != nil {
			return nil, err
		}
	}
	status, err := t.Status(ctx)
	if err != nil && errdefs.IsNotFound(err) {
		return nil, err
	}
	switch status.Status {
	case Stopped, Unknown, "":
	case Created:
		if t.client.runtime == fmt.Sprintf("%s.%s", plugin.RuntimePlugin, "windows") {
			// On windows Created is akin to Stopped
			break
		}
		fallthrough
	default:
		return nil, errors.Wrapf(errdefs.ErrFailedPrecondition, "task must be stopped before deletion: %s", status.Status)
	}
	if t.io != nil {
		t.io.Cancel()
		t.io.Wait()
	}
	r, err := t.client.TaskService().Delete(ctx, &tasks.DeleteTaskRequest{
		ContainerID: t.id,
	})
	if err != nil {
		return nil, errdefs.FromGRPC(err)
	}
	// Only cleanup the IO after a successful Delete
	if t.io != nil {
		t.io.Close()
	}
	return &ExitStatus{code: r.ExitStatus, exitedAt: r.ExitedAt}, nil
}

// Exec 在运行中的容器内创建新进程（类似 docker exec）
// wy: 🚀 调用链: Client → gRPC → Daemon → TTRPC → Shim → runc exec --process <spec.json>
// 两步操作:
//   1. task.Exec() 创建 exec 进程（runc exec 准备阶段），返回 Process 对象
//   2. process.Start() 启动 exec 进程（解除阻塞）
//
// spec 参数是 OCI Process Spec，包含: args, env, cwd, capabilities, user 等
func (t *task) Exec(ctx context.Context, id string, spec *specs.Process, ioCreate cio.Creator) (_ Process, err error) {
	if id == "" {
		return nil, errors.Wrapf(errdefs.ErrInvalidArgument, "exec id must not be empty")
	}
	// wy: 为 exec 进程创建独立的 IO 管道（与 init 进程的 IO 隔离）
	i, err := ioCreate(id)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil && i != nil {
			i.Cancel()
			i.Close()
		}
	}()

	// wy: 🚀 将 OCI Process Spec 序列化为 protobuf Any
	// 这是跨 gRPC 边界传递复杂对象的标准方式
	any, err := typeurl.MarshalAny(spec)
	if err != nil {
		return nil, err
	}

	cfg := i.Config()
	request := &tasks.ExecProcessRequest{
		ContainerID: t.id,
		ExecID:      id,        // wy: exec 进程的唯一标识（如 "exec-1"）
		Terminal:    cfg.Terminal,
		Stdin:       cfg.Stdin, // wy: exec 进程专用的 FIFO 路径
		Stdout:      cfg.Stdout,
		Stderr:      cfg.Stderr,
		Spec:        any,       // wy: OCI Process Spec (protobuf Any 编码)
	}
	// wy: gRPC 调用: 在 shim 中创建 exec 进程（但不启动）
	if _, err := t.client.TaskService().Exec(ctx, request); err != nil {
		i.Cancel()
		i.Wait()
		i.Close()
		return nil, errdefs.FromGRPC(err)
	}
	return &process{
		id:   id,
		task: t,
		io:   i,
	}, nil
}

func (t *task) Pids(ctx context.Context) ([]ProcessInfo, error) {
	response, err := t.client.TaskService().ListPids(ctx, &tasks.ListPidsRequest{
		ContainerID: t.id,
	})
	if err != nil {
		return nil, errdefs.FromGRPC(err)
	}
	var processList []ProcessInfo
	for _, p := range response.Processes {
		processList = append(processList, ProcessInfo{
			Pid:  p.Pid,
			Info: p.Info,
		})
	}
	return processList, nil
}

func (t *task) CloseIO(ctx context.Context, opts ...IOCloserOpts) error {
	r := &tasks.CloseIORequest{
		ContainerID: t.id,
	}
	var i IOCloseInfo
	for _, o := range opts {
		o(&i)
	}
	r.Stdin = i.Stdin
	_, err := t.client.TaskService().CloseIO(ctx, r)
	return errdefs.FromGRPC(err)
}

func (t *task) IO() cio.IO {
	return t.io
}

func (t *task) Resize(ctx context.Context, w, h uint32) error {
	_, err := t.client.TaskService().ResizePty(ctx, &tasks.ResizePtyRequest{
		ContainerID: t.id,
		Width:       w,
		Height:      h,
	})
	return errdefs.FromGRPC(err)
}

// NOTE: Checkpoint supports to dump task information to a directory, in this way, an empty
// OCI Index will be returned.
func (t *task) Checkpoint(ctx context.Context, opts ...CheckpointTaskOpts) (Image, error) {
	ctx, done, err := t.client.WithLease(ctx)
	if err != nil {
		return nil, err
	}
	defer done(ctx)
	cr, err := t.client.ContainerService().Get(ctx, t.id)
	if err != nil {
		return nil, err
	}

	request := &tasks.CheckpointTaskRequest{
		ContainerID: t.id,
	}
	i := CheckpointTaskInfo{
		runtime: cr.Runtime.Name,
	}
	for _, o := range opts {
		if err := o(&i); err != nil {
			return nil, err
		}
	}
	// set a default name
	if i.Name == "" {
		i.Name = fmt.Sprintf(checkpointNameFormat, t.id, time.Now().Format(checkpointDateFormat))
	}
	request.ParentCheckpoint = i.ParentCheckpoint
	if i.Options != nil {
		any, err := typeurl.MarshalAny(i.Options)
		if err != nil {
			return nil, err
		}
		request.Options = any
	}

	status, err := t.Status(ctx)
	if err != nil {
		return nil, err
	}

	if status.Status != Paused {
		// make sure we pause it and resume after all other filesystem operations are completed
		if err := t.Pause(ctx); err != nil {
			return nil, err
		}
		defer t.Resume(ctx)
	}

	index := v1.Index{
		Versioned: is.Versioned{
			SchemaVersion: 2,
		},
		Annotations: make(map[string]string),
	}
	if err := t.checkpointTask(ctx, &index, request); err != nil {
		return nil, err
	}
	// if checkpoint image path passed, jump checkpoint image,
	// return an empty image
	if isCheckpointPathExist(cr.Runtime.Name, i.Options) {
		return NewImage(t.client, images.Image{}), nil
	}

	if cr.Image != "" {
		if err := t.checkpointImage(ctx, &index, cr.Image); err != nil {
			return nil, err
		}
		index.Annotations["image.name"] = cr.Image
	}
	if cr.SnapshotKey != "" {
		if err := t.checkpointRWSnapshot(ctx, &index, cr.Snapshotter, cr.SnapshotKey); err != nil {
			return nil, err
		}
	}
	desc, err := t.writeIndex(ctx, &index)
	if err != nil {
		return nil, err
	}
	im := images.Image{
		Name:   i.Name,
		Target: desc,
		Labels: map[string]string{
			"containerd.io/checkpoint": "true",
		},
	}
	if im, err = t.client.ImageService().Create(ctx, im); err != nil {
		return nil, err
	}
	return NewImage(t.client, im), nil
}

// UpdateTaskInfo allows updated specific settings to be changed on a task
type UpdateTaskInfo struct {
	// Resources updates a tasks resource constraints
	Resources interface{}
	// Annotations allows arbitrary and/or experimental resource constraints for task update
	Annotations map[string]string
}

// UpdateTaskOpts allows a caller to update task settings
type UpdateTaskOpts func(context.Context, *Client, *UpdateTaskInfo) error

func (t *task) Update(ctx context.Context, opts ...UpdateTaskOpts) error {
	request := &tasks.UpdateTaskRequest{
		ContainerID: t.id,
	}
	var i UpdateTaskInfo
	for _, o := range opts {
		if err := o(ctx, t.client, &i); err != nil {
			return err
		}
	}
	if i.Resources != nil {
		any, err := typeurl.MarshalAny(i.Resources)
		if err != nil {
			return err
		}
		request.Resources = any
	}
	if i.Annotations != nil {
		request.Annotations = i.Annotations
	}
	_, err := t.client.TaskService().Update(ctx, request)
	return errdefs.FromGRPC(err)
}

func (t *task) LoadProcess(ctx context.Context, id string, ioAttach cio.Attach) (Process, error) {
	if id == t.id && ioAttach == nil {
		return t, nil
	}
	response, err := t.client.TaskService().Get(ctx, &tasks.GetRequest{
		ContainerID: t.id,
		ExecID:      id,
	})
	if err != nil {
		err = errdefs.FromGRPC(err)
		if errdefs.IsNotFound(err) {
			return nil, errors.Wrapf(err, "no running process found")
		}
		return nil, err
	}
	var i cio.IO
	if ioAttach != nil {
		if i, err = attachExistingIO(response, ioAttach); err != nil {
			return nil, err
		}
	}
	return &process{
		id:   id,
		task: t,
		io:   i,
	}, nil
}

func (t *task) Metrics(ctx context.Context) (*types.Metric, error) {
	response, err := t.client.TaskService().Metrics(ctx, &tasks.MetricsRequest{
		Filters: []string{
			"id==" + t.id,
		},
	})
	if err != nil {
		return nil, errdefs.FromGRPC(err)
	}

	if response.Metrics == nil {
		_, err := t.Status(ctx)
		if err != nil && errdefs.IsNotFound(err) {
			return nil, err
		}
		return nil, errors.New("no metrics received")
	}

	return response.Metrics[0], nil
}

func (t *task) checkpointTask(ctx context.Context, index *v1.Index, request *tasks.CheckpointTaskRequest) error {
	response, err := t.client.TaskService().Checkpoint(ctx, request)
	if err != nil {
		return errdefs.FromGRPC(err)
	}
	// NOTE: response.Descriptors can be an empty slice if checkpoint image is jumped
	// add the checkpoint descriptors to the index
	for _, d := range response.Descriptors {
		index.Manifests = append(index.Manifests, v1.Descriptor{
			MediaType: d.MediaType,
			Size:      d.Size_,
			Digest:    d.Digest,
			Platform: &v1.Platform{
				OS:           goruntime.GOOS,
				Architecture: goruntime.GOARCH,
			},
			Annotations: d.Annotations,
		})
	}
	return nil
}

func (t *task) checkpointRWSnapshot(ctx context.Context, index *v1.Index, snapshotterName string, id string) error {
	opts := []diff.Opt{
		diff.WithReference(fmt.Sprintf("checkpoint-rw-%s", id)),
	}
	rw, err := rootfs.CreateDiff(ctx, id, t.client.SnapshotService(snapshotterName), t.client.DiffService(), opts...)
	if err != nil {
		return err
	}
	rw.Platform = &v1.Platform{
		OS:           goruntime.GOOS,
		Architecture: goruntime.GOARCH,
	}
	index.Manifests = append(index.Manifests, rw)
	return nil
}

func (t *task) checkpointImage(ctx context.Context, index *v1.Index, image string) error {
	if image == "" {
		return fmt.Errorf("cannot checkpoint image with empty name")
	}
	ir, err := t.client.ImageService().Get(ctx, image)
	if err != nil {
		return err
	}
	index.Manifests = append(index.Manifests, ir.Target)
	return nil
}

func (t *task) writeIndex(ctx context.Context, index *v1.Index) (d v1.Descriptor, err error) {
	labels := map[string]string{}
	for i, m := range index.Manifests {
		labels[fmt.Sprintf("containerd.io/gc.ref.content.%d", i)] = m.Digest.String()
	}
	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(index); err != nil {
		return v1.Descriptor{}, err
	}
	return writeContent(ctx, t.client.ContentStore(), v1.MediaTypeImageIndex, t.id, buf, content.WithLabels(labels))
}

func writeContent(ctx context.Context, store content.Ingester, mediaType, ref string, r io.Reader, opts ...content.Opt) (d v1.Descriptor, err error) {
	writer, err := store.Writer(ctx, content.WithRef(ref))
	if err != nil {
		return d, err
	}
	defer writer.Close()
	size, err := io.Copy(writer, r)
	if err != nil {
		return d, err
	}

	if err := writer.Commit(ctx, size, "", opts...); err != nil {
		if !errdefs.IsAlreadyExists(err) {
			return d, err
		}
	}
	return v1.Descriptor{
		MediaType: mediaType,
		Digest:    writer.Digest(),
		Size:      size,
	}, nil
}

// isCheckpointPathExist only suitable for runc runtime now
func isCheckpointPathExist(runtime string, v interface{}) bool {
	if v == nil {
		return false
	}

	switch runtime {
	case plugin.RuntimeRuncV1, plugin.RuntimeRuncV2:
		if opts, ok := v.(*options.CheckpointOptions); ok && opts.ImagePath != "" {
			return true
		}

	case plugin.RuntimeLinuxV1:
		if opts, ok := v.(*runctypes.CheckpointOptions); ok && opts.ImagePath != "" {
			return true
		}
	}

	return false
}
