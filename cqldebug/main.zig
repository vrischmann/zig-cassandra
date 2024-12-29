const std = @import("std");
const debug = std.debug;
const fmt = std.fmt;
const heap = std.heap;
const net = std.net;
const posix = std.posix;

const cassandra = @import("cassandra");

const Connection = cassandra.Connection;

const log = std.log.scoped(.main);

const Backend = struct {
    pub fn drive(backend: *Backend, conn: *Connection) !void {
        _ = backend;
        _ = conn;
    }
};

pub fn main() anyerror!void {
    var gpa: heap.GeneralPurposeAllocator(.{}) = .init;
    defer debug.assert(gpa.deinit() == .ok);

    const address = blk: {
        const addresses = try net.getAddressList(gpa.allocator(), "localhost", 9042);
        defer addresses.deinit();
        debug.assert(addresses.addrs.len > 0);

        for (addresses.addrs) |addr| {
            if (addr.any.family == posix.AF.INET) {
                break :blk addr;
            }
        } else {
            return error.NoAddressResolved;
        }
    };

    log.info("address: {any}", .{address});

    const socket = try net.tcpConnectToAddress(address);
    defer socket.close();

    //

    // var backend = Backend{};

    var conn = try Connection.init(gpa.allocator());
    defer conn.deinit();

    conn.protocol_version = .v4;

    // try backend.drive(conn);

    var poll_fds: [4]posix.pollfd = undefined;
    poll_fds[0].fd = socket.handle;
    poll_fds[0].events = posix.POLL.IN | posix.POLL.OUT;

    var count: usize = 0;
    while (true) {
        try conn.tick();

        const ready = try posix.poll(&poll_fds, 1000);
        if (ready <= 0) continue;

        for (&poll_fds) |poll_fd| {
            if (poll_fd.revents == 0) continue;

            if (poll_fd.revents & posix.POLL.IN == posix.POLL.IN) {
                var buf: [1024]u8 = undefined;
                const n = try socket.read(&buf);

                if (n > 0) {
                    log.err("read: {any}, {s}", .{
                        buf[0..n],
                        fmt.fmtSliceHexLower(buf[0..n]),
                    });
                    try conn.feedReadable(buf[0..n]);
                }
            } else if (poll_fd.revents & posix.POLL.OUT == posix.POLL.OUT) {
                if (conn.write_buffer.readableLength() > 0) {
                    const writable = conn.getWritable();
                    try socket.writeAll(writable);
                    conn.write_buffer.discard(writable.len);
                }
            }
        }

        std.time.sleep(std.time.ns_per_ms * 100);

        if (conn.state == .nominal and count == 0) {
            const q1 = "select age from foobar.age_to_ids limit 216;";
            // const q2 = "select count(1) from foobar.age_to_ids;";

            try conn.doQuery(q1, .{
                .consistency_level = .One,
                .values = null,
                .skip_metadata = false,
                .page_size = null,
                .paging_state = null,
                .serial_consistency_level = null,
                .timestamp = null,
                .keyspace = null,
                .now_in_seconds = null,
            });
            count = 1;
        }
    }
}
