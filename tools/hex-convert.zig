const std = @import("std");

pub fn main() anyerror!void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    const allocator = gpa.allocator();

    const base_args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, base_args);

    if (base_args.len < 2) {
        std.debug.print("Usage: hex-convert <hex data>\n", .{});
        std.process.exit(1);
        unreachable;
    }

    //

    const data = base_args[1];

    var builder = std.ArrayList(u8).init(allocator);
    defer builder.deinit();

    var i: usize = 0;
    while (i < data.len) : (i += 2) {
        try builder.writer().print("\\x{s}", .{data[i .. i + 2]});
    }

    std.debug.print("{s}", .{builder.items});
}
