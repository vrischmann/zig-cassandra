const std = @import("std");
const heap = std.heap;
const mem = std.mem;

pub const Blocking = @import("client/Blocking.zig");

const Connection = @import("Connection.zig");

const Self = @This();

// pub const VTable = struct {
// };

gpa: mem.Allocator,
connection: *Connection,

pub fn init(allocator: mem.Allocator) !Self {
    return Self{
        .gpa = allocator,
        .connection = try Connection.init(allocator),
    };
}

pub fn deinit(client: *Self) void {
    client.connection.deinit();
}

test {
    // const allocator = std.testing.allocator;
    // var client = try init(allocator);
    //
    // client.connection.tick();
}
