//! By convention, root.zig is the root source file when making a library. If
//! you are making an executable, the convention is to delete this file and
//! start with main.zig instead.
const std = @import("std");
const atomic = std.atomic;
const testing = std.testing;

// From align.h: #define CACHE_LINE_SIZE 64
const CACHE_LINE_SIZE = 64;

/// Queue structure with cache-aligned atomic producer and consumer counters.
/// Handle type, typically representing a thread identifier or similar.
pub const Handle = usize;

pub const Queue = struct {
    P: atomic.Value(usize) align(CACHE_LINE_SIZE),
    C: atomic.Value(usize) align(CACHE_LINE_SIZE),

    /// Initializes the queue.
    /// Sets producer (P) and consumer (C) counters to 0.
    pub fn init(self: *Queue) void {
        self.P.store(0, .monotonic);
        self.C.store(0, .monotonic);
    }

    /// Registers a handle. In this implementation, it sets the handle to `id + 1`.
    /// The `q` parameter is unused, matching the C implementation.
    pub fn register(q: *Queue, hd: *Handle, id: i32) void {
        _ = q; // Mark as unused
        // Assuming id is non-negative or id + 1 fits in usize.
        hd.* = @intCast(id + 1);
    }

    pub fn enqueue(q: *Queue, th: *const Handle, val: ?*anyopaque) void {
        _ = th; // Mark as unused
        _ = val; // Mark as unused
        _ = q.P.fetchAdd(1, .monotonic);
    }

    pub fn dequeue(q: *Queue, th: *const Handle) ?*anyopaque {
        // Corresponds to FAA(&q->C, 1) with __ATOMIC_RELAXED
        _ = q.C.fetchAdd(1, .monotonic); // Return value (old C) is ignored

        // Returns the thread's handle, cast to an opaque pointer.
        return @ptrFromInt(th.*);
    }
};

test "Queue operations: init, register, enqueue, dequeue" {
    var q_mem: Queue = undefined; // Memory for the queue

    // Initialize queue
    q_mem.init(); // nprocs = 0 (unused)
    try testing.expectEqual(@as(usize, 0), q_mem.P.load(.seq_cst));
    try testing.expectEqual(@as(usize, 0), q_mem.C.load(.seq_cst));

    var handle: Handle = 0;
    const test_id: i32 = 10;
    const expected_handle_val: Handle = @intCast(test_id + 1);

    // Test register
    q_mem.register(&handle, test_id);
    try testing.expectEqual(expected_handle_val, handle);

    // Test enqueue
    q_mem.enqueue(&handle, null); // val can be null
    try testing.expectEqual(@as(usize, 1), q_mem.P.load(.seq_cst));
    try testing.expectEqual(@as(usize, 0), q_mem.C.load(.seq_cst)); // C should not be affected

    q_mem.enqueue(&handle, null);
    try testing.expectEqual(@as(usize, 2), q_mem.P.load(.seq_cst));

    // Test dequeue
    const ret_val_ptr = q_mem.dequeue(&handle);
    try testing.expectEqual(@as(usize, 1), q_mem.C.load(.seq_cst));
    try testing.expectEqual(@as(usize, 2), q_mem.P.load(.seq_cst)); // P should not be affected

    // Check returned value from dequeue (should be the handle cast to a pointer)
    const expected_ptr_val: *anyopaque = @ptrFromInt(expected_handle_val);
    try testing.expectEqual(expected_ptr_val, ret_val_ptr);

    // Dequeue again
    _ = q_mem.dequeue(&handle);
    try testing.expectEqual(@as(usize, 2), q_mem.C.load(.seq_cst));
}

test "Queue field alignment" {
    // Test that P and C are cache aligned.
    // Offset of P should be 0 (or a multiple of CACHE_LINE_SIZE if struct itself is further aligned).
    // Offset of C should be CACHE_LINE_SIZE (or P_offset + CACHE_LINE_SIZE).
    try testing.expectEqual(0, @offsetOf(Queue, "P") % CACHE_LINE_SIZE);
    try testing.expectEqual(0, @offsetOf(Queue, "C") % CACHE_LINE_SIZE);

    // Check that C is on a different cache line than P, assuming usize is smaller than CACHE_LINE_SIZE
    if (@sizeOf(usize) < CACHE_LINE_SIZE) {
        try testing.expect(@offsetOf(Queue, "C") >= @offsetOf(Queue, "P") + CACHE_LINE_SIZE);
    }
    // More directly, C's offset should be a multiple of cache line size, and P's offset too.
    // If P is at 0, C should be at 64 (or 128 etc.)
    // This also implicitly tests P and C are not overlapping if CACHE_LINE_SIZE > 0
    try testing.expect(@offsetOf(Queue, "C") != @offsetOf(Queue, "P"));
}
