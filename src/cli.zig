const std = @import("std");
const net = std.net;

const cql = @import("lib.zig");

pub fn main() anyerror!void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = &arena.allocator;

    var address = net.Address.initIp4([_]u8{ 127, 0, 0, 1 }, 9042);

    var client = try cql.Client.init(allocator, address);

    var result = try client.cquery("SELECT * FROM foobar.user", .{});
    var iter = result.Iter;
}
