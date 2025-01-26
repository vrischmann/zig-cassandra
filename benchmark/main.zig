const std = @import("std");
const debug = std.debug;
const heap = std.heap;
const net = std.net;

pub fn main() anyerror!void {
    var gpa: heap.GeneralPurposeAllocator(.{}) = .init;
    defer debug.assert(gpa.deinit() == .ok);

    const allocator = gpa.allocator();

    // const address = try net.Ip4Address.init([4]u8{ 127, 0, 0, 1 }, 9042);
    // const socket = try net.tcpConnectToAddress(address);
}
