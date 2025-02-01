const std = @import("std");
const io = std.io;
const fs = std.fs;
const debug = std.debug;
const mem = std.mem;
const fmt = std.fmt;
const heap = std.heap;
const net = std.net;
const posix = std.posix;
const builtin = @import("builtin");
const build_options = @import("build_options");

const cassandra = @import("cassandra");
const Connection = cassandra.Connection;
const linenoise = @import("linenoise");

const REPL = @import("REPL.zig");

const log = std.log.scoped(.main);

fn getHint(input: []const u8) mem.Allocator.Error!?[:0]u8 {
    const allocator = hints_arena.allocator();

    var iter = mem.splitScalar(u8, input, ' ');

    const command = iter.next() orelse return null;

    if (mem.startsWith(u8, command, "co")) {
        const endpoint = iter.next() orelse "";
        const port = iter.next() orelse "";

        if (endpoint.len > 0 and port.len == 0) {
            return try allocator.dupeZ(u8, " <port>");
        } else if (endpoint.len == 0 and port.len == 0) {
            return try allocator.dupeZ(u8, " <endpoint> [port]");
        }
    }

    return null;
}

fn replHintsCallback(buf: [*:0]const u8, color: *c_int, bold: *c_int) callconv(.C) ?[*:0]u8 {
    const input = mem.span(buf);

    const hint_opt = getHint(input) catch |err| switch (err) {
        error.OutOfMemory => return null,
    };
    if (hint_opt) |hint| {
        color.* = 90;
        bold.* = 0;
        return @constCast(hint).ptr;
    }

    return null;
}

var hints_arena: heap.ArenaAllocator = undefined;

fn replFreeHintsCallback(ptr: ?*anyopaque) callconv(.C) void {
    const buf: [*c]u8 = @ptrCast(@alignCast(ptr));
    const slice = mem.span(buf);

    hints_arena.allocator().free(slice);
}

fn replCompletionCallback(buf: [*:0]const u8, lc: *linenoise.Completions) callconv(.C) void {
    const input = mem.span(buf);

    if (mem.startsWith(u8, input, "co")) {
        linenoise.addCompletion(lc, "connect ");
    }
}

pub fn main() anyerror!void {
    var gpa: heap.GeneralPurposeAllocator(.{}) = .init;
    defer debug.assert(gpa.deinit() == .ok);

    const allocator = gpa.allocator();

    // Initial setup

    const filenames = blk: {
        const app_data_dir = try std.fs.getAppDataDir(allocator, "cqldebug");
        defer allocator.free(app_data_dir);

        const history_filename = try std.fs.path.join(allocator, &[_][]const u8{ app_data_dir, "history" });
        const trace_filename = try std.fs.path.join(allocator, &[_][]const u8{ app_data_dir, "trace.jsonl" });

        break :blk .{
            .history_filename = history_filename,
            .trace_filename = trace_filename,
        };
    };
    defer {
        allocator.free(filenames.history_filename);
        allocator.free(filenames.trace_filename);
    }

    // Initialize linenoise

    hints_arena = heap.ArenaAllocator.init(allocator);
    defer hints_arena.deinit();

    linenoise.setHintsCallback(replHintsCallback);
    linenoise.setFreeHintsCallback(replFreeHintsCallback);

    linenoise.setCompletionCallback(replCompletionCallback);

    {
        try linenoise.setMaxHistoryLen(128);

        const c_ptr = try allocator.dupeZ(u8, filenames.history_filename);
        defer allocator.free(c_ptr);

        linenoise.loadHistory(c_ptr) catch {
            log.err("history file {s} could not be read", .{filenames.history_filename});
        };
    }

    // Initialize and start the REPL loop

    var repl = try REPL.init(allocator, filenames.history_filename);
    defer repl.deinit();

    // Setup tracing

    const trace_file = try fs.cwd().createFile(filenames.trace_filename, .{
        .truncate = false,
    });
    try trace_file.seekFromEnd(0);

    repl.tracing = .{
        .file = trace_file,
        .tracer = cassandra.tracing.writerTracer(trace_file.writer()),
    };

    try repl.run();
}
