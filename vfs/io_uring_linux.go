// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build linux

package vfs

import (
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

const (
	syscallIOURingEnter    = 426
	syscallIOURingRegister = 427
)

// ioUringParams is the structure passed to io_uring_setup.
//
//	struct io_uring_params {
//	    __u32 sq_entries;
//	    __u32 cq_entries;
//	    __u32 flags;
//	    __u32 sq_thread_cpu;
//	    __u32 sq_thread_idle;
//	    __u32 features;
//	    __u32 wq_fd;
//	    __u32 resv[3];
//	    struct io_sqring_offsets sq_off;
//	    struct io_cqring_offsets cq_off;
//	};
type ioUringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFD         uint32
	resv         [3]uint32
	sqOff        sqRingOffsets
	cqOff        cqRingOffsets
}

// sqRingOffsets is a structure that contains the offsets for the submission
// queue.
//
//	struct io_sqring_offsets {
//	 	__u32 head;
//	 	__u32 tail;
//	 	__u32 ring_mask;
//	 	__u32 ring_entries;
//	 	__u32 flags;
//	 	__u32 dropped;
//	 	__u32 array;
//	 	__u32 resv1;
//	 	__u64 user_addr;
//	};
type sqRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	userAddr    uint64
}

// cqRingOffsets is a structure that contains the offsets for the completion
// queue.
//
//	struct io_cqring_offsets {
//		__u32 head;
//		__u32 tail;
//		__u32 ring_mask;
//		__u32 ring_entries;
//		__u32 overflow;
//		__u32 cqes;
//		__u32 flags;
//		__u32 resv1;
//		__u64 user_addr;
//	};
type cqRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	userAddr    uint64
}

func newIOUring(entries uint32) (*ioUring, error) {
	const (
		syscallIOUringSetup = 425

		mmapOffsetSubmissionQueueRing    = 0
		mmapOffsetCompletionQueueRing    = 0x8000000
		mmapOffsetSubmissionQueueEntries = 0x10000000
	)

	r := &ioUring{
		fileOffsets: make(map[uintptr]uint64),
	}
	// Create the ring.
	fd, _, errno := unix.Syscall(syscallIOUringSetup, uintptr(entries), uintptr(unsafe.Pointer(&r.params)), 0)
	if errno != 0 {
		return nil, errors.Wrap(errno, "io_uring_setup")
	}
	r.fd = fd

	// We need to mmap the submission and completion queues.
	//
	// In total there are three regions of memory that we need to mmap:
	// - The completion queue ring
	// - The submission queue ring
	// - The submission queue entries
	//
	// The submission queue ring stores pointers into the submission queue entries.
	//
	// If we encounter an error, we'll Close() the ioUring before propagating
	// the error. Care must be taken to ensure we unmap any memory regions we've
	// successfully mapped before returning.

	// First mmap the completion queue ring.
	r.cq.ringSize = uint(uintptr(r.params.cqOff.cqes) +
		uintptr(r.params.cqEntries)*unsafe.Sizeof(completionQueueEvent{}))
	cqRingPointer, err := mmap(0, uintptr(r.cq.ringSize), syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED|syscall.MAP_POPULATE, int(r.fd), mmapOffsetCompletionQueueRing)
	if err != nil {
		err = errors.CombineErrors(err, r.Close())
		return nil, err
	}
	r.cq.ringPtr = unsafe.Pointer(cqRingPointer)

	// Next mmap the submission queue ring.
	r.sq.ringSize = uint(uintptr(r.params.sqOff.array) +
		uintptr(r.params.sqEntries)*unsafe.Sizeof(uint32(0)))
	sqRingPointer, err := mmap(0, uintptr(r.sq.ringSize), syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED|syscall.MAP_POPULATE, int(r.fd), mmapOffsetSubmissionQueueRing)
	if err != nil {
		err = errors.CombineErrors(err, r.Close())
		return nil, err
	}
	r.sq.ringPtr = unsafe.Pointer(sqRingPointer)

	// Finally mmap the submission queue entries.
	//
	// When mapping the submission queue entries, we use the count of entries
	// (params.sqEntries) that the kernel populated when we called
	// io_uring_setup.
	sqEntriesPointer, err := mmap(0, unsafe.Sizeof(submissionQueueEntry{})*uintptr(r.params.sqEntries),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE, int(fd), mmapOffsetSubmissionQueueEntries)
	if err != nil {
		err = errors.CombineErrors(err, r.Close())
		return nil, err
	}
	r.sq.sqesPtr = (*submissionQueueEntry)(unsafe.Pointer(sqEntriesPointer))

	// Set up pointers based on the mmap'd regions.
	r.sq.head = (*uint32)(unsafe.Pointer(uintptr(r.sq.ringPtr) + uintptr(r.params.sqOff.head)))
	r.sq.tail = (*uint32)(unsafe.Pointer(uintptr(r.sq.ringPtr) + uintptr(r.params.sqOff.tail)))
	r.sq.ringMask = (*uint32)(unsafe.Pointer(uintptr(r.sq.ringPtr) + uintptr(r.params.sqOff.ringMask)))
	r.sq.ringEntries = (*uint32)(unsafe.Pointer(uintptr(r.sq.ringPtr) + uintptr(r.params.sqOff.ringEntries)))
	r.sq.flags = (*uint32)(unsafe.Pointer(uintptr(r.sq.ringPtr) + uintptr(r.params.sqOff.flags)))
	r.sq.dropped = (*uint32)(unsafe.Pointer(uintptr(r.sq.ringPtr) + uintptr(r.params.sqOff.dropped)))
	r.sq.array = (*uint32)(unsafe.Pointer(uintptr(r.sq.ringPtr) + uintptr(r.params.sqOff.array)))
	for i := range *r.sq.ringEntries {
		*(*uint32)(unsafe.Add(unsafe.Pointer(r.sq.array), i*uint32(unsafe.Sizeof(uint32(0))))) = i
	}

	r.cq.head = (*uint32)(unsafe.Pointer(uintptr(r.cq.ringPtr) + uintptr(r.params.cqOff.head)))
	r.cq.tail = (*uint32)(unsafe.Pointer(uintptr(r.cq.ringPtr) + uintptr(r.params.cqOff.tail)))
	r.cq.ringMask = (*uint32)(unsafe.Pointer(uintptr(r.cq.ringPtr) + uintptr(r.params.cqOff.ringMask)))
	r.cq.ringEntries = (*uint32)(unsafe.Pointer(uintptr(r.cq.ringPtr) + uintptr(r.params.cqOff.ringEntries)))
	r.cq.overflow = (*uint32)(unsafe.Pointer(uintptr(r.cq.ringPtr) + uintptr(r.params.cqOff.overflow)))
	r.cq.cqes = (*completionQueueEvent)(unsafe.Pointer(uintptr(r.cq.ringPtr) + uintptr(r.params.cqOff.cqes)))
	if r.params.cqOff.flags != 0 {
		r.cq.flags = (*uint32)(unsafe.Pointer(uintptr(r.cq.ringPtr) + uintptr(r.params.cqOff.flags)))
	}

	return r, nil
}

// TODO(jackson): Avoid the linkname directive. We can likely just use unix.MmapPtr.

//go:linkname mmap syscall.mmap
func mmap(
	addr uintptr, length uintptr, prot int, flags int, fd int, offset int64,
) (xaddr uintptr, err error)

type ioUring struct {
	opID OpID
	fd   uintptr
	sq   submissionQueue
	cq   completionQueue

	fileOffsets map[uintptr]uint64

	// params holds the parameters used while setting up the ring.
	params ioUringParams
}

func (r *ioUring) Submit(op RingOp) (OpID, error) {
	r.opID++
	id := r.opID
	op.ID = id
	err := r.addOp(op)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (r *ioUring) addOp(op RingOp) error {
	const (
		ioUringOpCodeWrite = 23

		ioUringSubmitFlagHardLink    = 8
		ioUringSubmitFlagSkipSuccess = 64
	)

	sqe := r.sq.getEntry()
	if sqe == nil {
		// TODO(jackson): Block until an entry is free.
		return errors.New("no free entries")
	}
	switch op.Type {
	case RingOpAppend:
		*sqe = submissionQueueEntry{
			OpCode: ioUringOpCodeWrite,
			Fd:     int32(op.Fd),
			Addr:   uint64(uintptr(unsafe.Pointer(&op.Data[0]))),
			Len:    uint32(len(op.Data)),
			Off:    r.fileOffsets[op.Fd],
		}
		r.fileOffsets[op.Fd] += uint64(len(op.Data))
	}
	return nil
}

func (r *ioUring) Sync() error {
	panic("TODO")
}

func (r *ioUring) Close() error {
	var err error
	if r.sq.ringPtr != nil {
		err = errors.CombineErrors(err, unix.MunmapPtr(r.sq.ringPtr, uintptr(r.sq.ringSize)))
	}
	if r.cq.ringPtr != nil {
		err = errors.CombineErrors(err, unix.MunmapPtr(r.cq.ringPtr, uintptr(r.cq.ringSize)))
	}
	if r.sq.sqesPtr != nil {
		err = errors.CombineErrors(err, unix.MunmapPtr(unsafe.Pointer(r.sq.sqesPtr),
			uintptr(r.params.sqEntries)*unsafe.Sizeof(submissionQueueEntry{})))
	}
	err = errors.CombineErrors(err, unix.Close(int(r.fd)))
	return err
}

type submissionQueue struct {
	head        *uint32
	tail        *uint32
	ringMask    *uint32
	ringEntries *uint32
	flags       *uint32
	dropped     *uint32
	array       *uint32
	sqesPtr     *submissionQueueEntry

	ringSize uint
	ringPtr  unsafe.Pointer

	sqeHead uint32
	sqeTail uint32

	// nolint: unused
	pad [2]uint32
}

func (sq *submissionQueue) pending() int {
	tail := sq.sqeTail
	if sq.sqeHead != tail {
		sq.sqeHead = tail
		atomic.StoreUint32(sq.tail, tail)
	}
	return int(tail - atomic.LoadUint32(sq.head))
}

func (sq *submissionQueue) getEntry() *submissionQueueEntry {
	head := atomic.LoadUint32(sq.head)
	next := sq.sqeTail + 1
	if next-head <= *sq.ringEntries {
		sqe := (*submissionQueueEntry)(
			unsafe.Add(unsafe.Pointer(sq.sqesPtr),
				uintptr((sq.sqeTail&*sq.ringMask))*unsafe.Sizeof(submissionQueueEntry{})),
		)
		sq.sqeTail = next

		return sqe
	}
	// No free entries.
	return nil
}

type submissionQueueEntry struct {
	OpCode uint8
	Flags  uint8
	IoPrio uint16
	Fd     int32
	// union {
	// 	__u64	off;	/* offset into file */
	// 	__u64	addr2;
	// 	struct {
	// 		__u32	cmd_op;
	// 		__u32	__pad1;
	// 	};
	// };
	Off uint64
	// union {
	// 	__u64	addr;	/* pointer to buffer or iovecs */
	// 	__u64	splice_off_in;
	// };
	Addr uint64
	Len  uint32
	// union {
	// 	__kernel_rwf_t	rw_flags;
	// 	__u32		fsync_flags;
	// 	__u16		poll_events;	/* compatibility */
	// 	__u32		poll32_events;	/* word-reversed for BE */
	// 	__u32		sync_range_flags;
	// 	__u32		msg_flags;
	// 	__u32		timeout_flags;
	// 	__u32		accept_flags;
	// 	__u32		cancel_flags;
	// 	__u32		open_flags;
	// 	__u32		statx_flags;
	// 	__u32		fadvise_advice;
	// 	__u32		splice_flags;
	// 	__u32		rename_flags;
	// 	__u32		unlink_flags;
	// 	__u32		hardlink_flags;
	// 	__u32		xattr_flags;
	// 	__u32		msg_ring_flags;
	// 	__u32		uring_cmd_flags;
	// };
	OpcodeFlags uint32
	UserData    uint64
	// union {
	// 	/* index into fixed buffers, if used */
	// 	__u16	buf_index;
	// 	/* for grouped buffer selection */
	// 	__u16	buf_group;
	// } __attribute__((packed));
	BufIG       uint16
	Personality uint16
	// union {
	// 	__s32	splice_fd_in;
	// 	__u32	file_index;
	// 	struct {
	// 		__u16	addr_len;
	// 		__u16	__pad3[1];
	// 	};
	// };
	SpliceFdIn int32
	Addr3      uint64
	_pad2      [1]uint64
	// TODO: add __u8	cmd[0];
}

type completionQueue struct {
	head        *uint32
	tail        *uint32
	ringMask    *uint32
	ringEntries *uint32
	flags       *uint32
	overflow    *uint32
	cqes        *completionQueueEvent

	ringSize uint
	ringPtr  unsafe.Pointer

	// nolint: unused
	pad [2]uint32
}

type completionQueueEvent struct {
	UserData uint64
	Res      int32
	Flags    uint32
	Res64    [8]uint8 // big_cqe[];
}
