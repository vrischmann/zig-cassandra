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

    var backend = Backend{};

    var conn = try Connection.init(gpa.allocator());
    defer conn.deinit();

    try backend.drive(conn);

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
