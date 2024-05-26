const std = @import("std");
const io = std.io;
const mem = std.mem;

const PrimitiveWriter = @import("message.zig").PrimitiveWriter;
const FrameHeader = @import("frame.zig").FrameHeader;
const RawFrame = @import("frame.zig").RawFrame;
const RawFrameReader = @import("frame.zig").RawFrameReader;
const RawFrameWriter = @import("frame.zig").RawFrameWriter;

pub fn printHRBytes(comptime fmt: []const u8, exp: []const u8, args: anytype) void {
    const hextable = "0123456789abcdef";

    var buffer = std.ArrayList(u8).init(std.testing.allocator);
    defer buffer.deinit();

    var column: usize = 0;
    for (exp) |c| {
        if (column % 80 == 0) {
            buffer.append('\n') catch unreachable;
            column = 0;
        }

        if (std.ascii.isAlphanumeric(c) or c == '_') {
            buffer.append(c) catch unreachable;
        } else {
            buffer.appendSlice("\\x") catch unreachable;
            buffer.append(hextable[(c & 0xF0) >> 4]) catch unreachable;
            buffer.append(hextable[(c & 0x0F)]) catch unreachable;
        }

        column += 1;
    }

    std.debug.print(fmt, .{buffer.items} ++ args);
}

/// Creates an arena allocator backed by the testing allocator.
/// Only intended to be used for tests.
pub fn arenaAllocator() std.heap.ArenaAllocator {
    return std.heap.ArenaAllocator.init(std.testing.allocator);
}

/// Reads a raw frame from the provided buffer.
/// Only intended to be used for tests.
pub fn readRawFrame(_allocator: mem.Allocator, data: []const u8) !RawFrame {
    var source = io.StreamSource{ .const_buffer = io.fixedBufferStream(data) };
    const reader = source.reader();

    var fr = RawFrameReader(@TypeOf(reader)).init(reader);

    return fr.read(_allocator);
}

pub fn expectSameRawFrame(comptime T: type, frame: T, header: FrameHeader, exp: []const u8) !void {
    var arena = arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

    // Write frame body
    var pw: PrimitiveWriter = undefined;
    try pw.reset(allocator);

    const write_fn = @typeInfo(@TypeOf(T.write));
    switch (write_fn) {
        .Fn => |info| {
            if (info.params.len == 2) {
                try frame.write(&pw);
            } else if (info.params.len == 3) {
                try frame.write(header.version, &pw);
            }
        },
        else => unreachable,
    }

    // Write raw frame

    const raw_frame = RawFrame{
        .header = header,
        .body = pw.getWritten(),
    };

    var buf2: [1024]u8 = undefined;
    var source = io.StreamSource{ .buffer = io.fixedBufferStream(&buf2) };
    const writer = source.writer();
    var fw = RawFrameWriter(@TypeOf(writer)).init(writer);

    try fw.write(raw_frame);

    if (!std.mem.eql(u8, exp, source.buffer.getWritten())) {
        printHRBytes("\n==> exp   : {s}\n", exp, .{});
        printHRBytes("==> source: {s}\n", source.buffer.getWritten(), .{});
        std.debug.panic("frames are different\n", .{});
    }
}
