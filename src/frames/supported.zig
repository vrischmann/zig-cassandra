const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// SUPPORTED is sent by a node in response to a OPTIONS frame.
///
/// Described in the protocol spec at §4.2.4.
const SupportedFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    protocol_versions: []ProtocolVersion,
    cql_versions: []CQLVersion,
    compression_algorithms: []CompressionAlgorithm,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.protocol_versions);
        self.allocator.free(self.cql_versions);
        self.allocator.free(self.compression_algorithms);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .protocol_versions = &[_]ProtocolVersion{},
            .cql_versions = &[_]CQLVersion{},
            .compression_algorithms = &[_]CompressionAlgorithm{},
        };

        const options = try framer.readStringMultimap();
        defer options.deinit();

        if (options.get("CQL_VERSION")) |values| {
            var list = std.ArrayList(CQLVersion).init(allocator);

            for (values) |value| {
                const version = try CQLVersion.fromString(value);
                _ = try list.append(version);
            }

            frame.cql_versions = list.toOwnedSlice();
        } else {
            return error.NoCQLVersion;
        }

        if (options.get("COMPRESSION")) |values| {
            var list = std.ArrayList(CompressionAlgorithm).init(allocator);

            for (values) |value| {
                const compression_algorithm = try CompressionAlgorithm.fromString(value);
                _ = try list.append(compression_algorithm);
            }

            frame.compression_algorithms = list.toOwnedSlice();
        }

        if (options.get("PROTOCOL_VERSIONS")) |values| {
            var list = std.ArrayList(ProtocolVersion).init(allocator);

            for (values) |value| {
                const version = try ProtocolVersion.fromString(value);
                _ = try list.append(version);
            }

            frame.protocol_versions = list.toOwnedSlice();
        } else {
            return error.NoProtocolVersions;
        }

        return frame;
    }
};

test "supported frame" {
    const data = "\x84\x00\x00\x09\x06\x00\x00\x00\x60\x00\x03\x00\x11\x50\x52\x4f\x54\x4f\x43\x4f\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x53\x00\x03\x00\x04\x33\x2f\x76\x33\x00\x04\x34\x2f\x76\x34\x00\x09\x35\x2f\x76\x35\x2d\x62\x65\x74\x61\x00\x0b\x43\x4f\x4d\x50\x52\x45\x53\x53\x49\x4f\x4e\x00\x02\x00\x06\x73\x6e\x61\x70\x70\x79\x00\x03\x6c\x7a\x34\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x01\x00\x05\x33\x2e\x34\x2e\x34";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Supported, data.len, framer.header);

    const frame = try SupportedFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(@as(usize, 1), frame.cql_versions.len);
    testing.expectEqual(CQLVersion{ .major = 3, .minor = 4, .patch = 4 }, frame.cql_versions[0]);

    testing.expectEqual(@as(usize, 3), frame.protocol_versions.len);
    testing.expectEqual(ProtocolVersion.V3, frame.protocol_versions[0]);
    testing.expectEqual(ProtocolVersion.V4, frame.protocol_versions[1]);
    testing.expectEqual(ProtocolVersion.V5, frame.protocol_versions[2]);

    testing.expectEqual(@as(usize, 2), frame.compression_algorithms.len);
    testing.expectEqual(CompressionAlgorithm.Snappy, frame.compression_algorithms[0]);
    testing.expectEqual(CompressionAlgorithm.LZ4, frame.compression_algorithms[1]);
}
