const std = @import("std");
const io = std.io;

pub const allocator = std.testing.allocator;
pub const expect = std.testing.expect;
pub const expectError = std.testing.expectError;
pub const expectEqual = std.testing.expectEqual;
pub const expectEqualSlices = std.testing.expectEqualSlices;

const FrameHeader = @import("frame.zig").FrameHeader;
const RawFrame = @import("frame.zig").RawFrame;
const RawFrameReader = @import("frame.zig").RawFrameReader;
const RawFrameWriter = @import("frame.zig").RawFrameWriter;

// Temporary function while waiting for Zig to have something like this.
pub fn expectEqualString(a: []const u8, b: []const u8) void {
    if (!std.mem.eql(u8, a, b)) {
        std.debug.panic("expected string \"{}\", got \"{}\"", .{ a, b });
    }
}

pub fn expectInDelta(a: var, b: var, delta: @TypeOf(a)) void {
    const dt = a - b;
    if (dt < -delta or dt > delta) {
        std.debug.panic("expected a {e} to be within {e} of b {}, but got {e}", .{ a, delta, b, dt });
    }
}

pub fn printHRBytes(data: []const u8) void {
    const hextable = "0123456789abcdef";

    var buffer = std.testing.allocator.alloc(u8, data.len * 4) catch |err| {
        std.debug.panic("can't allocate buffer. err: {}\n", .{err});
    };
    var j: usize = 0;
    for (data) |c| {
        buffer[j] = '\\';
        buffer[j + 1] = 'x';
        buffer[j + 2] = hextable[(c & 0xF0) >> 4];
        buffer[j + 3] = hextable[c & 0x0F];
        j += 4;
    }

    std.debug.getStderrStream().print("\n", .{}) catch unreachable;
    std.debug.getStderrStream().writeAll(buffer) catch unreachable;
    std.debug.getStderrStream().print("\n", .{}) catch unreachable;
}

/// Creates an arena allocator backed by the testing allocator.
/// Only intended to be used for tests.
pub fn arenaAllocator() std.heap.ArenaAllocator {
    return std.heap.ArenaAllocator.init(std.testing.allocator);
}

/// Reads a raw frame from the provided buffer.
/// Only intended to be used for tests.
pub fn readRawFrame(_allocator: *std.mem.Allocator, data: []const u8) !RawFrame {
    var source = io.StreamSource{ .const_buffer = io.fixedBufferStream(data) };
    var in_stream = source.inStream();

    var fr = RawFrameReader(@TypeOf(in_stream)).init(_allocator, in_stream);

    return fr.read();
}

/// Write a raw frame to a buffer and return it.
/// Only intended to be used for tests.
pub fn writeRawFrame(_allocator: *std.mem.Allocator, header: FrameHeader, body: []const u8) ![]u8 {
    var buf = try _allocator.alloc(u8, @sizeOf(FrameHeader) + body.len);

    var source = io.StreamSource{ .buffer = io.fixedBufferStream(buf) };
    var out_stream = source.outStream();
    var fw = RawFrameWriter(@TypeOf(out_stream)).init(out_stream);

    _ = try fw.write(RawFrame{
        .header = header,
        .body = body,
    });

    return buf;
}
