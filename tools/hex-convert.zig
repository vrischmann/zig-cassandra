const std = @import("std");
const mem = std.mem;

pub fn main(init: std.process.Init) anyerror!void {
    const io = init.io;

    const base_args = try std.process.Args.toSlice(init.minimal.args, init.arena.allocator());

    if (base_args.len < 2) {
        std.debug.print("Usage: hex-convert <hex data>\n", .{});
        std.process.exit(1);
    }

    if (mem.eql(u8, "-", base_args[1])) {
        // Read all of stdin into memory, then print as hex escape sequences
        var stdin_buf: [4096]u8 = undefined;
        var stdin_reader = std.Io.File.stdin().reader(io, &stdin_buf);
        const data = try stdin_reader.interface.allocRemaining(init.gpa, .unlimited);
        defer init.gpa.free(data);

        var stdout_buf: [4096]u8 = undefined;
        var stdout_file = std.Io.File.stdout().writer(io, &stdout_buf);
        for (data) |b| {
            try stdout_file.interface.print("\\x{x:02}", .{b});
        }
        try stdout_file.flush();
    } else {
        var stdout_buf: [4096]u8 = undefined;
        var stdout_file = std.Io.File.stdout().writer(io, &stdout_buf);

        const data = base_args[1];
        var i: usize = 0;
        while (i < data.len) : (i += 2) {
            try stdout_file.interface.print("\\x{s}", .{data[i..@min(i + 2, data.len)]});
        }
        try stdout_file.flush();
    }
}
