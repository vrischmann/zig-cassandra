const std = @import("std");
const mem = std.mem;
const meta = std.meta;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// STARTUP is sent to a node to initialize a connection.
///
/// Described in the protocol spec at §4.1.1.
const StartupFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    cql_version: []const u8,
    compression: ?CompressionAlgorithm,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.cql_version);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .cql_version = undefined,
            .compression = null,
        };

        // TODO(vincent): maybe avoid copying the strings ?

        const map = try framer.readStringMap();
        defer map.deinit();

        // CQL_VERSION is mandatory and the only version supported is 3.0.0 right now.
        if (map.get("CQL_VERSION")) |version| {
            if (!mem.eql(u8, "3.0.0", version.value)) {
                return error.InvalidCQLVersion;
            }
            frame.cql_version = try mem.dupe(allocator, u8, version.value);
        } else {
            return error.InvalidCQLVersion;
        }

        if (map.get("COMPRESSION")) |compression| {
            if (mem.eql(u8, compression.value, "lz4")) {
                frame.compression = CompressionAlgorithm.LZ4;
            } else if (mem.eql(u8, compression.value, "snappy")) {
                frame.compression = CompressionAlgorithm.Snappy;
            } else {
                return error.InvalidCompression;
            }
        }

        return frame;
    }
};

test "startup frame" {
    // from cqlsh exported via Wireshark
    const data = "\x04\x00\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x05\x33\x2e\x30\x2e\x30";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Startup, data.len, framer.header);

    const frame = try StartupFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();
}
