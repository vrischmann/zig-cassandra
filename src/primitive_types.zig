const std = @import("std");
const builtin = @import("builtin");
const mem = std.mem;
const testing = std.testing;

pub const CQLVersion = struct {
    major: u16,
    minor: u16,
    patch: u16,

    pub fn fromString(s: []const u8) !CQLVersion {
        var version = CQLVersion{
            .major = 0,
            .minor = 0,
            .patch = 0,
        };

        var it = mem.separate(s, ".");
        var pos: usize = 0;
        while (it.next()) |v| {
            const n = std.fmt.parseInt(u16, v, 10) catch return error.InvalidCQLVersion;

            switch (pos) {
                0 => version.major = n,
                1 => version.minor = n,
                2 => version.patch = n,
                else => break,
            }

            pos += 1;
        }

        if (version.major < 3) {
            return error.InvalidCQLVersion;
        }

        return version;
    }
};

pub const ProtocolVersion = packed enum(u8) {
    V3,
    V4,
    V5,

    pub fn fromString(s: []const u8) !ProtocolVersion {
        // NOTE(vincent): maybe this shouldn't be hardcoded like this but for now it's fine
        if (mem.startsWith(u8, s, "3/")) {
            return ProtocolVersion.V3;
        } else if (mem.startsWith(u8, s, "4/")) {
            return ProtocolVersion.V4;
        } else if (mem.startsWith(u8, s, "5/")) {
            return ProtocolVersion.V5;
        } else {
            return error.InvalidProtocolVersion;
        }
    }

    pub fn deserialize(self: *@This(), deserializer: var) !void {
        const version = try deserializer.deserialize(u8);
        return switch (version & 0x7) {
            3 => self.* = .V3,
            4 => self.* = .V4,
            5 => self.* = .V5,
            else => error.InvalidVersion,
        };
    }
};

pub const FrameFlags = packed enum(u8) {
    Compression = 0x01,
    Tracing = 0x02,
    CustomPayload = 0x04,
    Warning = 0x08,
    UseBeta = 0x10,
};

pub const Opcode = packed enum(u8) {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
};

pub const FrameHeader = packed struct {
    version: ProtocolVersion,
    flags: u8,
    stream: i16,
    opcode: Opcode,
    body_len: u32,

    pub fn read(comptime InStreamType: type, in: InStreamType) !FrameHeader {
        var deserializer = std.io.deserializer(builtin.Endian.Big, std.io.Packing.Byte, in);
        return deserializer.deserialize(FrameHeader);
    }
};

pub const CompressionAlgorithm = enum {
    LZ4,
    Snappy,

    pub fn fromString(s: []const u8) !CompressionAlgorithm {
        if (mem.eql(u8, "lz4", s)) {
            return CompressionAlgorithm.LZ4;
        } else if (mem.eql(u8, "snappy", s)) {
            return CompressionAlgorithm.Snappy;
        } else {
            return error.InvalidCompressionAlgorithm;
        }
    }
};

pub const ValueTag = enum {
    Set,
    NotSet,
};
pub const Value = union(ValueTag) {
    Set: []u8,
    NotSet: void,
};

pub const Consistency = packed enum(u16) {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    Serial = 0x0008,
    LocalSerial = 0x0009,
    LocalOne = 0x000A,
};

test "cql version: fromString" {
    testing.expectEqual(CQLVersion{ .major = 3, .minor = 0, .patch = 0 }, try CQLVersion.fromString("3.0.0"));
    testing.expectEqual(CQLVersion{ .major = 3, .minor = 4, .patch = 0 }, try CQLVersion.fromString("3.4.0"));
    testing.expectEqual(CQLVersion{ .major = 3, .minor = 5, .patch = 4 }, try CQLVersion.fromString("3.5.4"));
    testing.expectError(error.InvalidCQLVersion, CQLVersion.fromString("1.0.0"));
}

test "protocol version: fromString" {
    testing.expectEqual(ProtocolVersion.V3, try ProtocolVersion.fromString("3/v3"));
    testing.expectEqual(ProtocolVersion.V4, try ProtocolVersion.fromString("4/v4"));
    testing.expectEqual(ProtocolVersion.V5, try ProtocolVersion.fromString("5/v5"));
    testing.expectEqual(ProtocolVersion.V5, try ProtocolVersion.fromString("5/v5-beta"));

    testing.expectError(error.InvalidProtocolVersion, ProtocolVersion.fromString("lalal"));
}

test "protocol version: parse" {
    const testCase = struct {
        exp: ProtocolVersion,
        b: [2]u8,
        err: ?anyerror,
    };

    const testCases = [_]testCase{
        testCase{
            .exp = ProtocolVersion.V3,
            .b = [2]u8{ 0x03, 0x83 },
            .err = null,
        },
        testCase{
            .exp = ProtocolVersion.V4,
            .b = [2]u8{ 0x04, 0x84 },
            .err = null,
        },
        testCase{
            .exp = ProtocolVersion.V5,
            .b = [2]u8{ 0x05, 0x85 },
            .err = null,
        },
        testCase{
            .exp = ProtocolVersion.V5,
            .b = [2]u8{ 0x00, 0x00 },
            .err = error.InvalidVersion,
        },
    };

    for (testCases) |tc| {
        var version: ProtocolVersion = undefined;

        var in = std.io.fixedBufferStream(&tc.b);

        var d = std.io.deserializer(builtin.Endian.Big, std.io.Packing.Byte, in.inStream());
        if (tc.err) |err| {
            testing.expectError(err, d.deserialize(ProtocolVersion));
        } else {
            testing.expectEqual(tc.exp, try d.deserialize(ProtocolVersion));
            testing.expectEqual(tc.exp, try d.deserialize(ProtocolVersion));
        }
    }
}

test "compression algorith: fromString" {
    testing.expectEqual(CompressionAlgorithm.LZ4, try CompressionAlgorithm.fromString("lz4"));
    testing.expectEqual(CompressionAlgorithm.Snappy, try CompressionAlgorithm.fromString("snappy"));
    testing.expectError(error.InvalidCompressionAlgorithm, CompressionAlgorithm.fromString("foobar"));
}
