const std = @import("std");
const builtin = @import("builtin");
const io = std.io;
const fs = std.fs;
const debug = std.debug;
const mem = std.mem;
const fmt = std.fmt;
const heap = std.heap;
const net = std.net;
const posix = std.posix;
const build_options = @import("build_options");

const cassandra = @import("cassandra");
const Connection = cassandra.Connection;
const linenoise = @import("linenoise");

const log = std.log.scoped(.main);

const stdin = io.getStdIn();

const ExecuteResult = enum {
    save_history_line,
    do_nothing,
};

const Command = *const fn (repl: *REPL, input: []const u8) anyerror!ExecuteResult;

fn executeHelp(repl: *REPL, input: []const u8) anyerror!ExecuteResult {
    _ = repl;

    if (!mem.startsWith(u8, input, "help")) {
        return error.DoesNoMatch;
    }

    var iter = mem.splitScalar(u8, input, ' ');

    _ = iter.next() orelse "";
    const topic = iter.next() orelse "";

    if (mem.eql(u8, topic, "connect")) {
        print("\x1b[33mUsage\x1b[0m: connect <hostname> [port]", .{});
    } else {
        // TODO
    }

    return .save_history_line;
}

fn executeConnect(repl: *REPL, input: []const u8) anyerror!ExecuteResult {
    // Command: connect <hostname> [port]

    if (!mem.startsWith(u8, input, "connect")) {
        return error.DoesNoMatch;
    }

    // Parse the endpoint to validate it

    var iter = mem.splitScalar(u8, input, ' ');

    const command = iter.next() orelse "";
    const hostname = iter.next() orelse "";
    const port_s = iter.next() orelse "";

    if (!mem.eql(u8, "connect", command) or hostname.len <= 0) {
        print("\x1b[1mUsage\x1b[0m: connect <hostname> [port]", .{});
        return .do_nothing;
    }

    const port: u16 = if (port_s.len > 0)
        try fmt.parseInt(u16, port_s, 10)
    else
        9042;

    // Close and free the current connection if there is one
    if (repl.endpoint) |*endpoint| {
        endpoint.conn.deinit();
        endpoint.socket.close();
        endpoint.address = undefined;
    }

    const address = try resolveSingleAddress(repl.gpa, hostname, port);
    const socket = try net.tcpConnectToAddress(address);

    repl.poll_fds[1].fd = socket.handle;
    repl.poll_fds[1].events = posix.POLL.IN | posix.POLL.OUT;

    var conn = try Connection.init(repl.gpa);
    conn.protocol_version = .v4;
    // TODO(vincent): read this from args
    conn.authentication = .{
        .username = "vincent",
        .password = "vincent",
    };
    if (repl.tracing) |*tracing| {
        conn.tracer = tracing.tracer.tracer();
    }

    // Replace the prompt
    repl.ls.prompt = try fmt.bufPrintZ(&repl.ls.prompt_buf, "{any}> ", .{address});

    repl.endpoint = .{
        .address = address,
        .socket = socket,
        .conn = conn,
    };

    return .save_history_line;
}

fn executeDisconnect(repl: *REPL, input: []const u8) anyerror!ExecuteResult {
    // Command: disconnect

    if (!mem.startsWith(u8, input, "disconnect")) {
        return error.DoesNoMatch;
    }

    if (repl.endpoint) |*endpoint| {
        // Close and free the current connection if there is one
        endpoint.conn.deinit();
        endpoint.socket.close();

        repl.endpoint = null;

        repl.ls.prompt = REPL.default_prompt;
    }

    return .save_history_line;
}

const AllCommands = &[_]Command{
    executeHelp,
    executeConnect,
    executeDisconnect,
};

const REPL = struct {
    const scratch_arena_max_size = 1024;
    const default_prompt = "cqldebug> ";

    gpa: mem.Allocator,
    scratch_arena: heap.ArenaAllocator,

    ls: struct {
        prompt_buf: [1024]u8 = undefined,
        prompt: [:0]const u8 = default_prompt,

        buf: [1024]u8 = undefined,
        edit: linenoise.Edit = undefined,

        history_filename: []const u8 = undefined,
    },

    poll_fds: [2]posix.pollfd,

    endpoint: ?struct {
        address: net.Address,
        socket: net.Stream,
        conn: *Connection,
    } = null,

    // TODO(vincent): stop hardcoding the path
    tracing: ?struct {
        file: fs.File,
        tracer: cassandra.tracing.WriterTracer(fs.File.Writer),
    } = null,

    fn init(gpa: mem.Allocator, history_filename: []const u8) !*REPL {
        var repl = try gpa.create(REPL);
        errdefer gpa.destroy(repl);

        repl.* = .{
            .gpa = gpa,
            .scratch_arena = heap.ArenaAllocator.init(gpa),
            .ls = .{
                .history_filename = history_filename,
            },
            .poll_fds = undefined,
        };

        //

        try repl.linenoiseReset();

        repl.poll_fds[0].fd = stdin.handle;
        repl.poll_fds[0].events = posix.POLL.IN;

        return repl;
    }

    fn deinit(repl: *REPL) void {
        if (repl.endpoint) |endpoint| {
            // TODO(vincent): drain conn
            endpoint.conn.deinit();
            endpoint.socket.close();
        }

        repl.scratch_arena.deinit();
        repl.gpa.destroy(repl);
    }

    fn linenoiseReset(repl: *REPL) !void {
        repl.ls.edit = try linenoise.Edit.start(io.getStdIn(), io.getStdOut(), &repl.ls.buf, repl.ls.prompt);
    }

    fn processLine(repl: *REPL, input: []const u8) !void {
        const line = mem.trim(u8, input, " ");

        const result = for (AllCommands) |func| {
            const result_err = func(repl, line);

            const result = result_err catch |err| switch (err) {
                error.DoesNoMatch => continue,
                else => return err,
            };

            break result;
        } else {
            log.err("no command matching {s}", .{line});
            return;
        };

        if (result == .save_history_line) {
            defer _ = repl.scratch_arena.reset(.{ .retain_with_limit = scratch_arena_max_size });

            const c_line = try repl.scratch_arena.allocator().dupeZ(u8, line);
            const c_history_filename = try repl.scratch_arena.allocator().dupeZ(u8, repl.ls.history_filename);

            linenoise.addHistory(c_line) catch {};

            linenoise.saveHistory(c_history_filename) catch {
                repl.ls.edit.hide();
                defer repl.ls.edit.show();

                log.err("history file {s} could not be written", .{c_history_filename});
            };
        }
    }

    fn processPollFds(repl: *REPL) !void {
        for (&repl.poll_fds) |*poll_fd| {
            if (poll_fd.revents == 0) continue;

            if (poll_fd.fd == stdin.handle) {
                // stdin is ready

                const line_opt = repl.ls.edit.feed() catch |err| switch (err) {
                    error.EditMore => continue,
                    else => return err,
                };

                const line = line_opt orelse return error.Stop;

                repl.ls.edit.stop();
                defer linenoise.free(line);

                try repl.processLine(line);
                try repl.linenoiseReset();

                continue;
            }

            if (repl.endpoint) |endpoint| {
                debug.assert(poll_fd.fd == endpoint.socket.handle);

                const socket = endpoint.socket;
                const conn = endpoint.conn;

                if (poll_fd.revents & posix.POLL.IN == posix.POLL.IN) {
                    var buf: [1024]u8 = undefined;
                    const n = try socket.read(&buf);

                    if (n > 0) {
                        try conn.feedReadable(buf[0..n]);
                    }
                } else if (poll_fd.revents & posix.POLL.OUT == posix.POLL.OUT and conn.write_buffer.readableLength() > 0) {
                    const writable = conn.write_buffer.readableSliceOfLen(conn.write_buffer.readableLength());
                    try endpoint.socket.writeAll(writable);
                    conn.write_buffer.discard(writable.len);
                }

                // Clear the events for the next poll
                poll_fd.revents = 0;
            }
        }
    }

    fn run(repl: *REPL) !void {
        while (true) {
            const ready = try posix.poll(&repl.poll_fds, 1000);

            if (ready > 0) {
                repl.processPollFds() catch |err| switch (err) {
                    error.Stop => return,
                    else => return err,
                };
            }

            // Make some progress with the connection, if possible
            if (repl.endpoint) |endpoint| {
                const conn = endpoint.conn;

                // Disable the linenoise prompt for this call because the connection might log stuff.
                repl.ls.edit.hide();
                var diags = cassandra.Connection.Diagnostics{};
                conn.tick(&diags) catch |err| {
                    log.err("diagnostics: {s}", .{diags});
                    return err;
                };
                repl.ls.edit.show();

                // If we have something to write register for the OUT event on the pollfd
                if (conn.write_buffer.readableLength() > 0) {
                    repl.poll_fds[1].events |= posix.POLL.OUT;
                } else {
                    repl.poll_fds[1].events &= ~@as(i16, posix.POLL.OUT);
                }
            }

            // std.time.sleep(std.time.ns_per_ms * 100);
        }
    }
};

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
            return try allocator.dupeZ(u8, " <endpoint> <port>");
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

fn resolveSingleAddress(allocator: mem.Allocator, hostname: []const u8, port: u16) !net.Address {
    const addresses = try net.getAddressList(allocator, hostname, port);
    defer addresses.deinit();

    debug.assert(addresses.addrs.len > 0);

    for (addresses.addrs) |addr| {
        if (addr.any.family == posix.AF.INET) {
            return addr;
        }
    } else {
        return error.NoAddressResolved;
    }
}

inline fn print(comptime format_string: []const u8, args: anytype) void {
    std.debug.print(format_string ++ "\n", args);
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
