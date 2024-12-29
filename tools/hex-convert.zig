const std = @import("std");
const io = std.io;
const mem = std.mem;

pub fn main() anyerror!void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;

    var arena = std.heap.ArenaAllocator.init(gpa.allocator());
    defer arena.deinit();
    const allocator = arena.allocator();

    const base_args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, base_args);

    if (base_args.len < 2) {
        std.debug.print("Usage: hex-convert <hex data>\n", .{});
        std.process.exit(1);
        unreachable;
    }

    //

    if (mem.eql(u8, "-", base_args[1])) {
        const data = try io.getStdIn().readToEndAlloc(allocator, 10 * 1024 * 1024);

        var builder = std.ArrayList(u8).init(allocator);
        defer builder.deinit();

        for (data) |b| {
            try builder.writer().print("\\x{x:02}", .{b});
        }

        std.debug.print("{s}", .{builder.items});
    } else {
        const data = base_args[1];

        var builder = std.ArrayList(u8).init(allocator);
        defer builder.deinit();

        var i: usize = 0;
        while (i < data.len) : (i += 2) {
            try builder.writer().print("\\x{s}", .{data[i .. i + 2]});
        }

        std.debug.print("{s}", .{builder.items});
    }
}
