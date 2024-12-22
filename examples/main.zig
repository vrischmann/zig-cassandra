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

    // try backend.drive(conn);

    var poll_fds: [4]posix.pollfd = undefined;
    poll_fds[0].fd = socket.handle;
    poll_fds[0].events = posix.POLL.IN | posix.POLL.OUT;

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
                    log.info("read {d} bytes!", .{n});
                    try conn.feedReadable(buf[0..n]);
                }
            } else if (poll_fd.revents & posix.POLL.OUT == posix.POLL.OUT) {
                if (conn.write_buffer.readableLength() > 0) {
                    log.info("writing {d} bytes", .{conn.write_buffer.readableLength()});

                    const writable = try conn.moveWritable();
                    try socket.writeAll(writable);
                    log.info("written {d} bytes", .{writable.len});
                }
            }
        }

        std.time.sleep(std.time.ns_per_ms * 100);
    }

    // try conn.tick(writer, reader);
    // while (conn.tracer.events.readItem()) |event| {
    //     log.info("event: {any}", .{event});
    // }
    // try conn.tick(writer, reader);
    // while (conn.tracer.events.readItem()) |event| {
    //     log.info("event: {any}", .{event});
    // }

    // try client.appendMessage(cassandra.Connection.Message{ .options = {} });
    //
    // const written = try client.connection.write.buffer.toOwnedSlice();
    //
    // log.err("written: {s}", .{fmt.fmtSliceHexLower(written)});
    //
    // try socket.writeAll(written);
    //
    // try client.connection.read.buffer.ensureUnusedCapacity(1 * 1024 * 1024);
    // const read = try socket.read(client.connection.read.buffer.writableSlice(0));
    // debug.assert(read > 0);
    // client.connection.read.buffer.update(read);
    //
    // log.err("read: {d} bytes => {d} {s}", .{ read, client.connection.read.buffer.readableLength(), fmt.fmtSliceHexLower(client.connection.read.buffer.readableSlice(0)) });
    //
    //     const supported_message = client.connection.read.queue.readItem().?.supported;
    //     try client.connection.updateWithSupported(supported_message.compression_algorithms, supported_message.cql_versions, supported_message.protocol_versions);
    //
    // try client.appendMessage(cassandra.Connection.Message{.startup = cassandra.Connection.})
}
