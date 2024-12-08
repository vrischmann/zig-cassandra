const std = @import("std");
const mem = std.mem;

const Connection = @import("../Connection.zig");

const Self = @This();

conn: *Connection,

pub fn init(_: mem.Allocator) !Self {
    return Self{ .conn = undefined };
}
