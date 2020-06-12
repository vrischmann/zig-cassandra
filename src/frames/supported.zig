const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

usingnamespace @import("../frame.zig");
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// SUPPORTED is sent by a node in response to a OPTIONS frame.
///
/// Described in the protocol spec at §4.2.4.
pub const SupportedFrame = struct {
    const Self = @This();

    pub const opcode: Opcode = .Supported;

    protocol_versions: []ProtocolVersion,
    cql_versions: []CQLVersion,
    compression_algorithms: []CompressionAlgorithm,

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !Self {
        var frame = Self{
            .protocol_versions = &[_]ProtocolVersion{},
            .cql_versions = &[_]CQLVersion{},
            .compression_algorithms = &[_]CompressionAlgorithm{},
        };

        const options = try pr.readStringMultimap(allocator);

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
        }

        return frame;
    }
};

test "supported frame" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x84\x00\x00\x09\x06\x00\x00\x00\x60\x00\x03\x00\x11\x50\x52\x4f\x54\x4f\x43\x4f\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x53\x00\x03\x00\x04\x33\x2f\x76\x33\x00\x04\x34\x2f\x76\x34\x00\x09\x35\x2f\x76\x35\x2d\x62\x65\x74\x61\x00\x0b\x43\x4f\x4d\x50\x52\x45\x53\x53\x49\x4f\x4e\x00\x02\x00\x06\x73\x6e\x61\x70\x70\x79\x00\x03\x6c\x7a\x34\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x01\x00\x05\x33\x2e\x34\x2e\x34";
    const raw_frame = try testing.readRawFrame(&arena.allocator, exp);

    checkHeader(Opcode.Supported, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try SupportedFrame.read(&arena.allocator, &pr);

    testing.expectEqual(@as(usize, 1), frame.cql_versions.len);
    testing.expectEqual(CQLVersion{ .major = 3, .minor = 4, .patch = 4 }, frame.cql_versions[0]);

    testing.expectEqual(@as(usize, 3), frame.protocol_versions.len);
    testing.expect(frame.protocol_versions[0].is(3));
    testing.expect(frame.protocol_versions[1].is(4));
    testing.expect(frame.protocol_versions[2].is(5));

    testing.expectEqual(@as(usize, 2), frame.compression_algorithms.len);
    testing.expectEqual(CompressionAlgorithm.Snappy, frame.compression_algorithms[0]);
    testing.expectEqual(CompressionAlgorithm.LZ4, frame.compression_algorithms[1]);
}
