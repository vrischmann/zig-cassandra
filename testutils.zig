const std = @import("std");
const io = std.io;
const mem = std.mem;

const protocol = @import("protocol.zig");
const Iterator = @import("iterator.zig");

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
