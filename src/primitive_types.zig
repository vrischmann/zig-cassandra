const std = @import("std");
const io = std.io;
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const os = std.os;

const testing = @import("testing.zig");

pub const Value = union(enum) {
    Set: []const u8,
    NotSet: void,
    Null: void,
};

/// This struct is intended to be used as a field in a struct/tuple argument
/// passed to query/execute.
///
/// For example if you have a prepared update query where you don't want to touch
/// the second field, you can pass a struct/tuple like this:
///
/// .{
///    .arg1 = ...,
///    .arg2 = NotSet{ .type = i64 },
/// }
///
/// TODO(vincent): the `type` field is necessary for now to make NotSet work but it's not great.
pub const NotSet = struct {
    type: type,
};

pub const NamedValue = struct {
    name: []const u8,
    value: Value,
};

pub const Values = union(enum) {
    Normal: []Value,
    Named: []NamedValue,
};

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

        var it = mem.split(s, ".");
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

    pub fn print(self: @This(), buf: []u8) ![]u8 {
        return std.fmt.bufPrint(buf, "{}.{}.{}", .{
            self.major,
            self.minor,
            self.patch,
        });
    }
};

pub const ProtocolVersion = packed struct {
    const Self = @This();

    version: u8,

    pub fn init(b: u8) !Self {
        var res = Self{
            .version = b,
        };
        if ((res.version & 0x7) < 3 or (res.version & 0x7) > 5) {
            return error.InvalidProtocolVersion;
        }
        return res;
    }

    pub fn is(self: Self, comptime version: comptime_int) bool {
        return self.version & 0x7 == @as(u8, version);
    }
    pub fn isAtLeast(self: Self, comptime version: comptime_int) bool {
        const tmp = self.version & 0x7;
        return tmp >= version;
    }
    pub fn isRequest(self: Self) bool {
        return self.version & 0x0f == self.version;
    }
    pub fn isResponse(self: Self) bool {
        return self.version & 0xf0 == 0x80;
    }

    pub fn fromString(s: []const u8) !ProtocolVersion {
        if (mem.startsWith(u8, s, "3/")) {
            return ProtocolVersion{ .version = 3 };
        } else if (mem.startsWith(u8, s, "4/")) {
            return ProtocolVersion{ .version = 4 };
        } else if (mem.startsWith(u8, s, "5/")) {
            return ProtocolVersion{ .version = 5 };
        } else {
            return error.InvalidProtocolVersion;
        }
    }

    pub fn toString(self: Self) ![]const u8 {
        if (self.is(3)) {
            return "3/v3";
        } else if (self.is(4)) {
            return "4/v4";
        } else if (self.is(5)) {
            return "5/v5-beta";
        } else {
            return error.InvalidProtocolVersion;
        }
    }
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

    pub fn toString(self: @This()) []const u8 {
        return switch (self) {
            .LZ4 => "lz4",
            .Snappy => "snappy",
        };
    }
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

pub const BatchType = packed enum(u8) {
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
};

pub const OptionID = packed enum(u16) {
    Custom = 0x0000,
    Ascii = 0x0001,
    Bigint = 0x0002,
    Blob = 0x0003,
    Boolean = 0x0004,
    Counter = 0x0005,
    Decimal = 0x0006,
    Double = 0x0007,
    Float = 0x0008,
    Int = 0x0009,
    Timestamp = 0x000B,
    UUID = 0x000C,
    Varchar = 0x000D,
    Varint = 0x000E,
    Timeuuid = 0x000F,
    Inet = 0x0010,
    Date = 0x0011,
    Time = 0x0012,
    Smallint = 0x0013,
    Tinyint = 0x0014,
    Duration = 0x0015,
    List = 0x0020,
    Map = 0x0021,
    Set = 0x0022,
    UDT = 0x0030,
    Tuple = 0x0031,
};

test "cql version: fromString" {
    testing.expectEqual(CQLVersion{ .major = 3, .minor = 0, .patch = 0 }, try CQLVersion.fromString("3.0.0"));
    testing.expectEqual(CQLVersion{ .major = 3, .minor = 4, .patch = 0 }, try CQLVersion.fromString("3.4.0"));
    testing.expectEqual(CQLVersion{ .major = 3, .minor = 5, .patch = 4 }, try CQLVersion.fromString("3.5.4"));
    testing.expectError(error.InvalidCQLVersion, CQLVersion.fromString("1.0.0"));
}

test "protocol version: fromString" {
    testing.expect((try ProtocolVersion.fromString("3/v3")).is(3));
    testing.expect((try ProtocolVersion.fromString("4/v4")).is(4));
    testing.expect((try ProtocolVersion.fromString("5/v5")).is(5));
    testing.expect((try ProtocolVersion.fromString("5/v5-beta")).is(5));
    testing.expectError(error.InvalidProtocolVersion, ProtocolVersion.fromString("lalal"));
}

test "protocol version: serialize and deserialize" {
    const testCase = struct {
        exp: comptime_int,
        b: [2]u8,
        err: ?anyerror,
    };

    const testCases = [_]testCase{
        testCase{
            .exp = 3,
            .b = [2]u8{ 0x03, 0x83 },
            .err = null,
        },
        testCase{
            .exp = 4,
            .b = [2]u8{ 0x04, 0x84 },
            .err = null,
        },
        testCase{
            .exp = 5,
            .b = [2]u8{ 0x05, 0x85 },
            .err = null,
        },
        testCase{
            .exp = 0,
            .b = [2]u8{ 0x00, 0x00 },
            .err = error.InvalidProtocolVersion,
        },
    };

    inline for (testCases) |tc| {
        var version: ProtocolVersion = undefined;

        if (tc.err) |err| {
            testing.expectError(err, ProtocolVersion.init(tc.b[0]));
        } else {
            var v1 = try ProtocolVersion.init(tc.b[0]);
            var v2 = try ProtocolVersion.init(tc.b[1]);
            testing.expect(v1.is(tc.exp));
            testing.expect(v1.isRequest());
            testing.expect(v2.isResponse());
        }
    }
}

test "compression algorith: fromString" {
    testing.expectEqual(CompressionAlgorithm.LZ4, try CompressionAlgorithm.fromString("lz4"));
    testing.expectEqual(CompressionAlgorithm.Snappy, try CompressionAlgorithm.fromString("snappy"));
    testing.expectError(error.InvalidCompressionAlgorithm, CompressionAlgorithm.fromString("foobar"));
}
