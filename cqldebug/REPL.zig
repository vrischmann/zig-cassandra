const std = @import("std");
const io = std.io;
const debug = std.debug;
const fmt = std.fmt;
const heap = std.heap;
const fs = std.fs;
const mem = std.mem;
const posix = std.posix;
const net = std.net;

const cassandra = @import("cassandra");
const Connection = cassandra.Connection;
const linenoise = @import("linenoise");

const stdin = io.getStdIn();
const log = std.log.scoped(.main);

const scratch_arena_max_size = 1024;
const default_prompt = "cqldebug> ";

const Self = @This();

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

pub fn init(gpa: mem.Allocator, history_filename: []const u8) !*Self {
    var repl = try gpa.create(Self);
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

pub fn deinit(repl: *Self) void {
    if (repl.endpoint) |endpoint| {
        // TODO(vincent): drain conn
        endpoint.conn.deinit();
        endpoint.socket.close();
    }

    repl.scratch_arena.deinit();
    repl.gpa.destroy(repl);
}

fn linenoiseReset(repl: *Self) !void {
    repl.ls.edit = try linenoise.Edit.start(io.getStdIn(), io.getStdOut(), &repl.ls.buf, repl.ls.prompt);
}

fn processLine(repl: *Self, input: []const u8) !void {
    const line = mem.trim(u8, input, " ");
    if (line.len == 0) return;

    const result = for (AllCommands) |cmd| {
        const result_err = cmd.execute(repl, line);

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

fn processPollFds(repl: *Self) !void {
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

pub fn run(repl: *Self) !void {
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

const ExecuteResult = enum {
    save_history_line,
    do_nothing,
};

const Command = struct {
    name: []const u8,
    help: *const fn () []const u8,
    execute: *const fn (repl: *Self, input: []const u8) anyerror!ExecuteResult,
};

const HelpCommand = struct {
    fn help() []const u8 {
        return fmt.comptimePrint(
            \\{s}
            \\
            \\    help {s}
            \\
            \\{s}
            \\
            \\    {s: <40}
            \\    {s: <40}
            \\
            \\{s}
            \\
            \\    {s: <40}
            \\
        , .{
            yellow("Usage"),
            gray("[topic]"),
            yellow("Connection"),
            fmt.comptimePrint("connect {s}", .{gray("<hostname> [port]")}),
            "disconnect",
            yellow("Framing"),
            fmt.comptimePrint("frame dump {s}", .{gray("[enable|disable]")}),
        });
    }

    fn execute(repl: *Self, input: []const u8) anyerror!ExecuteResult {
        if (!mem.startsWith(u8, input, "help")) {
            return error.DoesNoMatch;
        }

        defer _ = repl.scratch_arena.reset(.{ .retain_with_limit = Self.scratch_arena_max_size });
        var out = std.ArrayList(u8).init(repl.scratch_arena.allocator());

        var iter = mem.splitScalar(u8, input, ' ');
        _ = iter.next() orelse "";

        const topic = blk: {
            var parts = std.ArrayList([]const u8).init(repl.scratch_arena.allocator());
            while (iter.next()) |part| {
                try parts.append(part);
            }

            const res = try mem.join(repl.scratch_arena.allocator(), " ", parts.items);

            break :blk res;
        };

        inline for (AllCommands) |cmd| {
            if (@TypeOf(cmd) == HelpCommand) continue;

            if (mem.eql(u8, topic, cmd.name)) {
                try out.appendSlice(cmd.help());
                break;
            }
        } else {
            try out.appendSlice(HelpCommand.help());
        }

        debug.print("{s}", .{out.items});

        return .save_history_line;
    }
};

const ConnectCommand = struct {
    fn help() []const u8 {
        return fmt.comptimePrint(
            \\
            \\    connect {s}
            \\    {s} Connect to a Cassandra node
            \\
            \\
        , .{
            gray("<hostname> [port]"),
            yellow("summary:"),
        });
    }

    fn execute(repl: *Self, input: []const u8) anyerror!ExecuteResult {
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
            print("    connect " ++ gray("<hostname> [port]"), .{});
            print("    " ++ yellow("summary:") ++ " connect to a Cassandra node", .{});
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
};

const DisconnectCommand = struct {
    fn help() []const u8 {
        return fmt.comptimePrint(
            \\
            \\    disconnect
            \\    {s} Disconnect the current connection
            \\
            \\
        , .{
            yellow("summary:"),
        });
    }

    fn execute(repl: *Self, input: []const u8) anyerror!ExecuteResult {
        // Command: disconnect

        if (!mem.startsWith(u8, input, "disconnect")) {
            return error.DoesNoMatch;
        }

        if (repl.endpoint) |*endpoint| {
            // Close and free the current connection if there is one
            endpoint.conn.deinit();
            endpoint.socket.close();

            repl.endpoint = null;

            repl.ls.prompt = Self.default_prompt;
        }

        return .save_history_line;
    }
};

const FrameDumpCommand = struct {
    fn help() []const u8 {
        return fmt.comptimePrint(
            \\
            \\    frame dump {s}
            \\    {s} Enable or disable frame dumping
            \\
            \\
        , .{
            gray("[enable|disable]"),
            yellow("summary:"),
        });
    }

    fn execute(repl: *Self, input: []const u8) anyerror!ExecuteResult {
        _ = repl;

        if (!mem.startsWith(u8, input, "frame dump")) {
            return error.DoesNoMatch;
        }

        var iter = mem.splitScalar(u8, input, ' ');

        _ = iter.next() orelse "";
        _ = iter.next() orelse "";

        if (iter.next()) |action| {
            if (mem.eql(u8, action, "enable")) {
                print("frame dumping enabled", .{});
            } else if (mem.eql(u8, action, "disable")) {
                print("frame dumping disabled ", .{});
            } else {
                print("invalid frame dumping action \"{s}\"", .{action});
            }
        } else {
            print("frame dumping currently disabled", .{});
        }

        return .save_history_line;
    }
};

const AllCommands = &[_]Command{
    .{ .name = "help", .help = HelpCommand.help, .execute = HelpCommand.execute },
    .{ .name = "connect", .help = ConnectCommand.help, .execute = ConnectCommand.execute },
    .{ .name = "disconnect", .help = DisconnectCommand.help, .execute = DisconnectCommand.execute },
    .{ .name = "frame dump", .help = FrameDumpCommand.help, .execute = FrameDumpCommand.execute },
};

const Color = enum(usize) {
    yellow = 33,
    gray = 90,
};

inline fn yellow(comptime format_string: []const u8) []const u8 {
    return colored(format_string, .yellow);
}
inline fn gray(comptime format_string: []const u8) []const u8 {
    return colored(format_string, .gray);
}

inline fn colored(comptime format_string: []const u8, color: Color) []const u8 {
    return fmt.comptimePrint("\x1b[{d}m{s}\x1b[0m", .{ @intFromEnum(color), format_string });
}

inline fn print(comptime format_string: []const u8, args: anytype) void {
    std.debug.print(format_string ++ "\n", args);
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
