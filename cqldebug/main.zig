const std = @import("std");
const io = std.io;
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

const REPL = struct {
    const scratch_arena_max_size = 1024;

    gpa: mem.Allocator,
    scratch_arena: heap.ArenaAllocator,

    ls: struct {
        buf: [1024]u8,
        edit: linenoise.Edit,

        history_filename: []const u8,
    },

    poll_fds: [2]posix.pollfd,

    endpoint: ?struct {
        address: net.Address,
        socket: net.Stream,
        conn: *Connection,
    } = null,

    fn init(gpa: mem.Allocator) !*REPL {
        var repl = try gpa.create(REPL);

        repl.* = .{
            .gpa = gpa,
            .scratch_arena = heap.ArenaAllocator.init(gpa),
            .ls = undefined,
            .poll_fds = undefined,
            .endpoint = null,
        };

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
        repl.ls.edit = try linenoise.Edit.start(io.getStdIn(), io.getStdOut(), &repl.ls.buf, "cqldebug> ");
    }

    fn processLine(repl: *REPL, input: []const u8) !void {
        const line = mem.trim(u8, input, " ");

        var save_line: bool = false;

        if (mem.startsWith(u8, line, "connect")) {
            //
            // Command: connect <hostname> [port>
            //
            // Parse the endpoint to validate it

            var iter = mem.splitScalar(u8, line, ' ');

            const command = iter.next() orelse "";
            const hostname = iter.next() orelse "";
            const port_s = iter.next() orelse "";

            if (!mem.eql(u8, "connect", command) or hostname.len <= 0) {
                print("\x1b[1mUsage\x1b[0m: connect <hostname>", .{});

                return;
            }

            const port: u16 = if (port_s.len > 0)
                try fmt.parseInt(u16, port_s, 10)
            else
                9042;

            save_line = true;

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

            repl.endpoint = .{
                .address = address,
                .socket = socket,
                .conn = conn,
            };
        }

        if (save_line) {
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
                try conn.tick();
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

fn replHintsCallback(buf: [*:0]const u8, color: *c_int, bold: *c_int) callconv(.C) ?[*:0]u8 {
    const input = mem.span(buf);

    if (mem.eql(u8, input, "co")) {
        color.* = 35;
        bold.* = 1;

        const res = " <endpoint>";

        return @constCast(res).ptr;
    }

    return null;
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

    // Initialize linenoise

    linenoise.setHintsCallback(replHintsCallback);
    linenoise.setCompletionCallback(replCompletionCallback);

    const history_filename = blk: {
        const home = std.zig.EnvVar.HOME.getPosix() orelse unreachable;
        const history_filename = try std.fs.path.join(allocator, &[_][]const u8{ home, ".local", "share", "cqldebug", "history" });

        break :blk history_filename;
    };
    defer allocator.free(history_filename);

    {
        try linenoise.setMaxHistoryLen(128);

        const c_ptr = try allocator.dupeZ(u8, history_filename);
        defer allocator.free(c_ptr);

        linenoise.loadHistory(c_ptr) catch {
            log.err("history file {s} could not be read", .{history_filename});
        };
    }

    // Initialize and start the REPL loop

    var repl = try REPL.init(allocator);
    repl.ls.history_filename = history_filename;

    defer repl.deinit();

    try repl.run();
}
