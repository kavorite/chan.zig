const std = @import("std");
const atomic = std.atomic;
const testing = std.testing;

// From align.h: #define CACHE_LINE_SIZE 64
const CACHE_LINE_SIZE = 64;

/// Channel with cache-aligned atomic producer and consumer counters.
pub const Chan = struct {
    dummy_node: Node align(CACHE_LINE_SIZE),
    tail: atomic.Value(*Node) align(CACHE_LINE_SIZE),

    pub const Node = struct {
        next: atomic.Value(?*Node),
    };

    /// Initializes the queue.
    /// Sets up the dummy node and initializes the tail pointer.
    pub fn init(self: *Chan) void {
        self.dummy_node.next.store(null, .monotonic); // No data depends on this yet
        self.tail.store(&self.dummy_node, .release); // Ensure dummy_node is initialized before tail is published
    }

    pub fn make() Chan {
        var self: Chan = undefined;
        self.init();
        return self;
    }

    /// Enqueues a node into the lock-free queue.
    /// The `node_to_add` must not be null and its `next` field will be set to null.
    pub fn send(self: *Chan, node_to_add: *Node) void {
        node_to_add.next.store(null, .monotonic); // Initialize node's next pointer

        while (true) {
            std.atomic.spinLoopHint();

            const local_tail = self.tail.load(.acquire);
            const local_tail_next = local_tail.next.load(.acquire);

            // Check if tail is still consistent.
            if (local_tail == self.tail.load(.acquire)) {
                if (local_tail_next == null) { // If tail is indeed the last node
                    // Try to link the new node.
                    // cmpxchgStrong returns null on success.
                    if (local_tail.next.cmpxchgStrong(null, node_to_add, .release, .monotonic) == null) {
                        // Successfully linked. Try to swing the tail pointer.
                        // It's okay if this CAS fails, another enqueuer or dequeuer might fix it.
                        _ = self.tail.cmpxchgStrong(local_tail, node_to_add, .release, .monotonic);
                        return; // Enqueue successful
                    }
                } else { // Tail was not pointing to the last node (another enqueue is in progress).
                    // Try to help by swinging the tail to the next node.
                    // local_tail_next is guaranteed non-null here.
                    _ = self.tail.cmpxchgStrong(local_tail, local_tail_next.?, .release, .monotonic);
                }
            }
        }
    }

    pub fn wait(self: *Chan) void {
        var node: ?*Node = null;
        while (node == null) node = self.recv();
        return node.?;
    }

    /// Dequeues a node from the lock-free queue.
    /// Returns a pointer to the dequeued node, or null if the queue is empty.
    /// The caller is responsible for using `@fieldParentPtr` to get the containing data structure.
    pub fn recv(self: *Chan) ?*Node {
        while (true) {
            std.atomic.spinLoopHint();

            const head_is_dummy = &self.dummy_node;
            const first_potential_node = head_is_dummy.next.load(.acquire);
            const local_tail = self.tail.load(.acquire);

            // Re-read head_is_dummy.next to protect against ABA, though head_is_dummy is stable.
            // This mainly ensures first_potential_node is consistent with the current state of dummy_node.next
            if (head_is_dummy.next.load(.acquire) != first_potential_node) {
                continue; // state changed, retry
            }

            if (first_potential_node == null) { // Queue is empty
                return null;
            }

            // first_potential_node is not null here
            const first_node_actual = first_potential_node.?;

            if (head_is_dummy == local_tail) {
                // Head is tail. This means either the queue is empty (checked above)
                // or tail is lagging. If first_potential_node is not null, tail is lagging.
                // Help advance the tail.
                _ = self.tail.cmpxchgStrong(local_tail, first_node_actual, .release, .monotonic);
                // Continue to retry, as the state is changing or we need to re-evaluate.
            } else {
                // Queue is not empty and head is not tail.
                // Try to dequeue first_potential_node.
                const node_after_first = first_node_actual.next.load(.acquire);
                // cmpxchgStrong returns null on success.
                if (head_is_dummy.next.cmpxchgStrong(first_potential_node, node_after_first, .acq_rel, .acquire) == null) {
                    // Successfully unlinked first_potential_node.
                    // If tail was pointing to the dequeued node, it might need an update.
                    // Specifically, if the queue becomes empty, tail should point back to dummy.
                    if (local_tail == first_potential_node) {
                        // local_tail == first_potential_node implies first_potential_node is not null.
                        _ = self.tail.cmpxchgStrong(first_potential_node.?, head_is_dummy, .release, .monotonic);
                    }
                    return first_potential_node; // Return the dequeued node
                }
            }
        }
    }
};

test "Chan operations: init, put, pop" {
    var q: Chan = undefined;
    q.init();

    // We need a data structure to embed the Node
    const MyData = struct {
        id: i32,
        node: Chan.Node,
    };

    var item1_mem = MyData{ .id = 10, .node = .{ .next = atomic.Value(?*Chan.Node).init(null) } };
    var item2_mem = MyData{ .id = 20, .node = .{ .next = atomic.Value(?*Chan.Node).init(null) } };

    // Test init state
    try testing.expect(q.dummy_node.next.load(.monotonic) == null);
    try testing.expect(q.tail.load(.monotonic) == &q.dummy_node);

    // Test enqueue one item
    q.send(&item1_mem.node);
    try testing.expect(q.dummy_node.next.load(.acquire) == &item1_mem.node);
    try testing.expect(q.tail.load(.acquire) == &item1_mem.node);
    try testing.expect(item1_mem.node.next.load(.monotonic) == null);

    // Test enqueue second item
    q.send(&item2_mem.node);
    try testing.expect(q.dummy_node.next.load(.acquire) == &item1_mem.node); // item1 is still first
    try testing.expect(item1_mem.node.next.load(.acquire) == &item2_mem.node); // item1 points to item2
    try testing.expect(q.tail.load(.acquire) == &item2_mem.node); // tail is item2
    try testing.expect(item2_mem.node.next.load(.monotonic) == null);

    // Test dequeue first item
    var dequeued_node_ptr = q.recv();
    try testing.expect(dequeued_node_ptr != null);
    const dequeued_node = dequeued_node_ptr.?; // we know it's not null
    const dequeued_item1: *MyData = @fieldParentPtr("node", dequeued_node);
    try testing.expectEqual(@as(i32, 10), dequeued_item1.id);
    try testing.expect(q.dummy_node.next.load(.acquire) == &item2_mem.node); // item2 is now first
    try testing.expect(q.tail.load(.acquire) == &item2_mem.node); // tail is item2

    // Test dequeue second item
    dequeued_node_ptr = q.recv();
    try testing.expect(dequeued_node_ptr != null);
    const dequeued_node2 = dequeued_node_ptr.?; // we know it's not null
    const dequeued_item2: *MyData = @fieldParentPtr("node", dequeued_node2);
    try testing.expectEqual(@as(i32, 20), dequeued_item2.id);
    try testing.expect(q.dummy_node.next.load(.acquire) == null); // Queue is empty
    try testing.expect(q.tail.load(.acquire) == &q.dummy_node); // Tail back to dummy

    // Test dequeue from empty queue
    dequeued_node_ptr = q.recv();
    try testing.expect(dequeued_node_ptr == null);
}

test "Chan field alignment" {
    // Test that dummy_node and tail are cache aligned.
    try testing.expectEqual(0, @offsetOf(Chan, "dummy_node") % CACHE_LINE_SIZE);
    try testing.expectEqual(0, @offsetOf(Chan, "tail") % CACHE_LINE_SIZE);

    // Check that tail is on a different cache line than dummy_node's fields,
    // assuming Node (dummy_node) is smaller than CACHE_LINE_SIZE.
    if (@sizeOf(Chan.Node) < CACHE_LINE_SIZE) {
        try testing.expect(@offsetOf(Chan, "tail") >= @offsetOf(Chan, "dummy_node") + CACHE_LINE_SIZE or
            @offsetOf(Chan, "tail") < @offsetOf(Chan, "dummy_node")); // Could be before if packed differently
        // A stronger test: their start addresses relative to struct start should differ by a multiple of cache line size
        // if they are on different cache lines.
        // Or, more simply, ensure they are not in the same cache line if they are distinct.
        const dummy_node_start = @offsetOf(Chan, "dummy_node");
        const tail_start = @offsetOf(Chan, "tail");
        if (dummy_node_start / CACHE_LINE_SIZE != tail_start / CACHE_LINE_SIZE) {
            // They are in different cache lines, good.
        } else {
            // They are in the same cache line. Ensure dummy_node + its size doesn't overlap tail
            // or tail + its size doesn't overlap dummy_node.
            // This is generally ensured by distinct field offsets.
            // The primary concern is false sharing if they are very close on the *same* line.
            // The align(CACHE_LINE_SIZE) on both fields should segregate them.
            try testing.expect(tail_start >= dummy_node_start + @sizeOf(Chan.Node) or
                dummy_node_start >= tail_start + @sizeOf(atomic.Value(*Chan.Node)));
        }
    }
    try testing.expect(@offsetOf(Chan, "tail") != @offsetOf(Chan, "dummy_node"));
}
