const std = @import("std");

const testing = @import("testing.zig");

// This is largely based on gocql: https://github.com/gocql/gocql/blob/master/internal/streams/streams.go

const BucketType = u64;
const BucketBits = @sizeOf(BucketType) * 8;

pub const IDGenerator = struct {
    const Self = @This();
    /// MaxStreams is the maximum number of streams allowed by Cassandra.
    /// Since Protocol v2 this is the limit, and we only support Protocol v3+
    // pub const MaxStreams = 32768;
    pub const MaxStreams = 128;

    const Buckets = MaxStreams / BucketBits;

    inuse: u32,

    /// streams is a bitset where each bit represents a stream, a 1 implies in use.
    streams: [Buckets]BucketType,
    offset: u32,

    pub fn init(self: *Self) void {
        self.inuse = 1;
        self.streams = [_]BucketType{0} ** Buckets;
        self.offset = Buckets - 1;
        // Reserve stream 0
        self.streams[0] = (1 << 63);
    }

    pub fn getInUse(self: *Self) usize {
        return @intCast(usize, @atomicLoad(u32, &self.inuse, .SeqCst));
    }

    pub fn get(self: *Self) ?i16 {
        // Acquire the next offset.
        const offset = blk: {
            var tmp = @atomicLoad(u32, &self.offset, .SeqCst);

            while (@atomicRmw(u32, &self.offset, .Xchg, (tmp + 1) % Buckets, .SeqCst) != tmp) {
                tmp = @atomicLoad(u32, &self.offset, .SeqCst);
            }

            tmp = (tmp + 1) % Buckets;

            break :blk tmp;
        };

        // Find a bucket with an empty slot.

        var i: usize = 0;
        while (i < Buckets) : (i += 1) {
            const pos = (i + offset) % Buckets;

            var bucket = @atomicLoad(BucketType, &self.streams[pos], .SeqCst);
            if (bucket == ((1 << 64) - 1)) {
                // All streams in use
                continue;
            }

            // Find a stream to use.

            var j: usize = 0;
            while (j < BucketBits) : (j += 1) {
                const mask = @as(u64, 1) << streamOffset(j);
                while ((bucket & mask) == 0) {
                    if (@atomicRmw(u64, &self.streams[pos], .Xchg, bucket | mask, .SeqCst) == bucket) {
                        _ = @atomicRmw(u32, &self.inuse, .Add, 1, .SeqCst);

                        return streamFromBucket(pos, j);
                    }
                    bucket = @atomicLoad(u64, &self.streams[pos], .SeqCst);
                }
            }
        }

        return null;
    }

    pub fn clear(self: *Self, stream: i16) void {
        const pos = @intCast(usize, stream) / @as(usize, BucketBits);

        var bucket = @atomicLoad(u64, &self.streams[pos], .SeqCst);

        const mask = @as(u64, 1) << streamOffset(@intCast(usize, stream));
        if ((bucket & mask) != mask) {
            // Already cleared.
            return;
        }

        while (@atomicRmw(u64, &self.streams[pos], .Xchg, bucket & ~mask, .SeqCst) != bucket) {
            bucket = @atomicLoad(u64, &self.streams[pos], .SeqCst);
            if ((bucket & mask) != mask) {
                // Already cleared.
                return;
            }
        }

        _ = @atomicRmw(u32, &self.inuse, .Sub, 1, .SeqCst);
    }
};

fn streamFromBucket(bucket: usize, streamInBucket: usize) i16 {
    return @intCast(i16, (bucket * BucketBits) + streamInBucket);
}

test "generator: streamFromBucket" {
    testing.expectEqual(@intCast(i16, 20), streamFromBucket(0, 20));
    testing.expectEqual(@intCast(i16, 148), streamFromBucket(2, 20));
}

fn streamOffset(stream: usize) u6 {
    return @intCast(u6, BucketBits - (stream % BucketBits) - 1);
}

test "generator: streamOffset" {
    testing.expectEqual(@intCast(u6, 63), streamOffset(0));
    testing.expectEqual(@intCast(u6, 43), streamOffset(20));
}

test "generator: fill all" {
    var generator: IDGenerator = undefined;
    IDGenerator.init(&generator);

    // Expect to get all streams from 1 to 32767

    var seen = std.AutoHashMap(i16, void).init(testing.allocator);
    defer seen.deinit();

    var i: usize = 1;
    while (i < IDGenerator.MaxStreams) : (i += 1) {
        const stream = generator.get();
        // std.log.warn(.main, "got stream: {}={}\n", .{ i, stream });
        try seen.putNoClobber(stream.?, .{});
    }

    i = 1;
    while (i < IDGenerator.MaxStreams) : (i += 1) {
        const v = seen.get(@intCast(i16, i));
        testing.expect(v != null);
    }

    // Expect to not use stream 0
    testing.expect(seen.get(@intCast(i16, 0)) == null);

    // Expect to not get another stream now.
    const stream = generator.get();
    testing.expect(stream == null);

    // Expect inuse to equal the maximum number of streams.
    testing.expectEqual(@intCast(usize, IDGenerator.MaxStreams), generator.getInUse());

    // Expect all buckets to be filled.
    for (generator.streams) |bucket| {
        testing.expectEqual(@intCast(u64, (1 << 64) - 1), bucket);
    }
}

test "generator: clear all" {
    var generator: IDGenerator = undefined;
    IDGenerator.init(&generator);

    // Expect to get all streams from 1 to 32767

    var seen = std.AutoHashMap(i16, void).init(testing.allocator);
    defer seen.deinit();

    var i: usize = 1;
    while (i < IDGenerator.MaxStreams) : (i += 1) {
        _ = generator.get();
    }

    // Expect to not get another stream now.
    const stream = generator.get();
    testing.expect(stream == null);

    // Expect inuse to equal the maximum number of streams.
    testing.expectEqual(@intCast(usize, IDGenerator.MaxStreams), generator.getInUse());

    // Clear everything

    i = 0;
    while (i < IDGenerator.MaxStreams) : (i += 1) {
        generator.clear(@intCast(i16, i));
    }

    // Expect inuse to be 0
    testing.expectEqual(@intCast(usize, 0), generator.getInUse());

    // Expect all buckets to be cleared.
    for (generator.streams) |bucket| {
        testing.expectEqual(@intCast(u64, 0), bucket);
    }
}
