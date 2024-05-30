const std = @import("std");
const io = std.io;
const mem = std.mem;

const MessageWriter = @import("message.zig").MessageWriter;

const frame = @import("frame.zig");
const EnvelopeHeader = frame.EnvelopeHeader;
const Envelope = frame.Envelope;
const EnvelopeReader = frame.EnvelopeReader;
const EnvelopeWriter = frame.EnvelopeWriter;

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
pub fn readEnvelope(_allocator: mem.Allocator, data: []const u8) !Envelope {
    var source = io.StreamSource{ .const_buffer = io.fixedBufferStream(data) };
    const reader = source.reader();

    var fr = EnvelopeReader(@TypeOf(reader)).init(reader);

    return fr.read(_allocator);
}

pub fn expectSameEnvelope(comptime T: type, fr: T, header: EnvelopeHeader, exp: []const u8) !void {
    var arena = arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

    // Write frame body
    var mw = try MessageWriter.init(allocator);

    const write_fn = @typeInfo(@TypeOf(T.write));
    switch (write_fn) {
        .Fn => |info| {
            if (info.params.len == 2) {
                try fr.write(&mw);
            } else if (info.params.len == 3) {
                try fr.write(header.version, &mw);
            }
        },
        else => unreachable,
    }

    // Write raw frame

    const envelope = Envelope{
        .header = header,
        .body = mw.getWritten(),
    };

    var buf2: [1024]u8 = undefined;
    var source = io.StreamSource{ .buffer = io.fixedBufferStream(&buf2) };
    const writer = source.writer();
    var fw = EnvelopeWriter(@TypeOf(writer)).init(writer);

    try fw.write(envelope);

    if (!std.mem.eql(u8, exp, source.buffer.getWritten())) {
        printHRBytes("\n==> exp   : {s}\n", exp, .{});
        printHRBytes("==> source: {s}\n", source.buffer.getWritten(), .{});
        std.debug.panic("envelopes are different\n", .{});
    }
}
