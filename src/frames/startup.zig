const std = @import("std");
const mem = std.mem;
const meta = std.meta;

usingnamespace @import("../frame.zig");
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// STARTUP is sent to a node to initialize a connection.
///
/// Described in the protocol spec at §4.1.1.
const StartupFrame = struct {
    const Self = @This();

    cql_version: []const u8,
    compression: ?CompressionAlgorithm,

    pub fn write(self: *Self, pw: *PrimitiveWriter) !void {
        if (self.compression) |c| {
            // Always 2 keys
            _ = try pw.startStringMap(2);

            _ = try pw.writeString("CQL_VERSION");
            _ = try pw.writeString("3.0.0");

            _ = try pw.writeString("COMPRESSION");
            switch (c) {
                .LZ4 => _ = try pw.writeString("lz4"),
                .Snappy => _ = try pw.writeString("snappy"),
            }
        } else {
            // Always 1 key
            _ = try pw.startStringMap(2);

            _ = try pw.writeString("CQL_VERSION");
            _ = try pw.writeString("3.0.0");
        }
    }

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !Self {
        var frame = Self{
            .cql_version = undefined,
            .compression = null,
        };

        const map = try pr.readStringMap();

        // CQL_VERSION is mandatory and the only version supported is 3.0.0 right now.
        if (map.get("CQL_VERSION")) |version| {
            if (!mem.eql(u8, "3.0.0", version.value)) {
                return error.InvalidCQLVersion;
            }
            frame.cql_version = version.value;
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

test "startup frame: read" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x04\x00\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x05\x33\x2e\x30\x2e\x30";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    checkHeader(Opcode.Startup, data.len, raw_frame.header);

    var pr = PrimitiveReader.init(&arena.allocator);
    pr.reset(raw_frame.body);

    const frame = try StartupFrame.read(&arena.allocator, &pr);

    testing.expectEqualString("3.0.0", frame.cql_version);
    testing.expect(frame.compression == null);
}

test "startup frame: write" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var frame = StartupFrame{
        .cql_version = "3.0.0",
        .compression = null,
        // .compression = CompressionAlgorithm.LZ4,
    };

    // TODO(vincent): broken

    var buf: [1024]u8 = undefined;
    var pw = PrimitiveWriter.init();
    pw.reset(&buf);

    _ = try StartupFrame.write(&frame, &pw);

    const header = FrameHeader{
        .version = ProtocolVersion.V4,
        .flags = 0,
        .stream = 200,
        .opcode = Opcode.Startup,
        .body_len = @intCast(u32, pw.getWritten().len),
    };

    const out = try testing.writeRawFrame(&arena.allocator, header, pw.getWritten());

    const exp = "\x04\x00\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x05\x33\x2e\x30\x2e\x30";
    testing.expectEqualSlices(u8, exp, out);
}
