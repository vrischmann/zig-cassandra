const std = @import("std");
const io = std.io;

pub const allocator = std.testing.allocator;
pub const expect = std.testing.expect;
pub const expectError = std.testing.expectError;
pub const expectEqual = std.testing.expectEqual;
pub const expectEqualSlices = std.testing.expectEqualSlices;

const PrimitiveWriter = @import("primitive/writer.zig").PrimitiveWriter;
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

pub fn printHRBytes(comptime fmt: []const u8, exp: []const u8, args: var) void {
    const hextable = "0123456789abcdef";

    var buffer = std.ArrayList(u8).init(std.testing.allocator);
    defer buffer.deinit();

    for (exp) |c| {
        if (std.ascii.isAlNum(c) or c == '_') {
            buffer.append(c) catch unreachable;
        } else {
            buffer.appendSlice("\\x") catch unreachable;
            buffer.append(hextable[(c & 0xF0) >> 4]) catch unreachable;
            buffer.append(hextable[(c & 0x0F)]) catch unreachable;
        }
    }

    var span = buffer.span();
    std.debug.warn(fmt, .{span} ++ args);
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

pub fn expectSameRawFrame(frame: var, header: FrameHeader, exp: []const u8) void {
    // Write frame body
    var buf: [1024]u8 = undefined;
    var pw = PrimitiveWriter.init();
    pw.reset(&buf);

    frame.write(&pw) catch |err| {
        std.debug.panic("unable to write frame. err: {}\n", .{err});
    };

    // Write raw frame

    const raw_frame = RawFrame{
        .header = header,
        .body = pw.getWritten(),
    };

    var buf2: [1024]u8 = undefined;
    var source = io.StreamSource{ .buffer = io.fixedBufferStream(&buf2) };
    var out_stream = source.outStream();
    var fw = RawFrameWriter(@TypeOf(out_stream)).init(out_stream);

    fw.write(raw_frame) catch |err| {
        std.debug.panic("unable to write raw frame. err: {}\n", .{err});
    };

    if (!std.mem.eql(u8, exp, source.buffer.getWritten())) {
        printHRBytes("\nexp   : {}\n", exp, .{});
        printHRBytes("source: {}\n", source.buffer.getWritten(), .{});
        std.debug.panic("frames are different\n", .{});
    }
}
