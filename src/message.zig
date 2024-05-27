const std = @import("std");
const io = std.io;
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const os = std.os;
const posix = std.posix;
const testing = std.testing;

const string_map = @import("string_map.zig");

const testutils = @import("testutils.zig");

// TODO(vincent): test all error codes
pub const ErrorCode = enum(u32) {
    ServerError = 0x0000,
    ProtocolError = 0x000A,
    AuthError = 0x0100,
    UnavailableReplicas = 0x1000,
    CoordinatorOverloaded = 0x1001,
    CoordinatorIsBootstrapping = 0x1002,
    TruncateError = 0x1003,
    WriteTimeout = 0x1100,
    ReadTimeout = 0x1200,
    ReadFailure = 0x1300,
    FunctionFailure = 0x1400,
    WriteFailure = 0x1500,
    CDCWriteFailure = 0x1600,
    CASWriteUnknown = 0x1700,
    SyntaxError = 0x2000,
    Unauthorized = 0x2100,
    InvalidQuery = 0x2200,
    ConfigError = 0x2300,
    AlreadyExists = 0x2400,
    Unprepared = 0x2500,
};

pub const UnavailableReplicasError = struct {
    consistency_level: Consistency,
    required: u32,
    alive: u32,
};

pub const FunctionFailureError = struct {
    keyspace: []const u8,
    function: []const u8,
    arg_types: []const []const u8,
};

pub const WriteError = struct {
    // TODO(vincent): document this
    pub const WriteType = enum {
        SIMPLE,
        BATCH,
        UNLOGGED_BATCH,
        COUNTER,
        BATCH_LOG,
        CAS,
        VIEW,
        CDC,
    };

    pub const Timeout = struct {
        consistency_level: Consistency,
        received: u32,
        block_for: u32,
        write_type: WriteType,
        contentions: ?u16,
    };

    pub const Failure = struct {
        pub const Reason = struct {
            endpoint: net.Address,
            // TODO(vincent): what's this failure code ?!
            failure_code: u16,
        };

        consistency_level: Consistency,
        received: u32,
        block_for: u32,
        reason_map: []Reason,
        write_type: WriteType,
    };

    pub const CASUnknown = struct {
        consistency_level: Consistency,
        received: u32,
        block_for: u32,
    };
};

pub const ReadError = struct {
    pub const Timeout = struct {
        consistency_level: Consistency,
        received: u32,
        block_for: u32,
        data_present: u8,
    };

    pub const Failure = struct {
        pub const Reason = struct {
            endpoint: net.Address,
            // TODO(vincent): what's this failure code ?!
            failure_code: u16,
        };

        consistency_level: Consistency,
        received: u32,
        block_for: u32,
        reason_map: []Reason,
        data_present: u8,
    };
};

pub const AlreadyExistsError = struct {
    keyspace: []const u8,
    table: []const u8,
};

pub const UnpreparedError = struct {
    statement_id: []const u8,
};

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

        var it = mem.splitSequence(u8, s, ".");
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
        return std.fmt.bufPrint(buf, "{d}.{d}.{d}", .{
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
        const res = Self{
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
        } else if (mem.startsWith(u8, s, "6/")) {
            return ProtocolVersion{ .version = 6 };
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
        } else if (self.is(6)) {
            return "6/v6-beta";
        } else {
            return error.InvalidProtocolVersion;
        }
    }
};

pub const Opcode = enum(u8) {
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

pub const Consistency = enum(u16) {
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

pub const BatchType = enum(u8) {
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
};

pub const OptionID = enum(u16) {
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
    try testing.expectEqual(CQLVersion{ .major = 3, .minor = 0, .patch = 0 }, try CQLVersion.fromString("3.0.0"));
    try testing.expectEqual(CQLVersion{ .major = 3, .minor = 4, .patch = 0 }, try CQLVersion.fromString("3.4.0"));
    try testing.expectEqual(CQLVersion{ .major = 3, .minor = 5, .patch = 4 }, try CQLVersion.fromString("3.5.4"));
    try testing.expectError(error.InvalidCQLVersion, CQLVersion.fromString("1.0.0"));
}

test "protocol version: fromString" {
    try testing.expect((try ProtocolVersion.fromString("3/v3")).is(3));
    try testing.expect((try ProtocolVersion.fromString("4/v4")).is(4));
    try testing.expect((try ProtocolVersion.fromString("5/v5")).is(5));
    try testing.expect((try ProtocolVersion.fromString("5/v5-beta")).is(5));
    try testing.expectError(error.InvalidProtocolVersion, ProtocolVersion.fromString("lalal"));
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
        if (tc.err) |err| {
            try testing.expectError(err, ProtocolVersion.init(tc.b[0]));
        } else {
            var v1 = try ProtocolVersion.init(tc.b[0]);
            var v2 = try ProtocolVersion.init(tc.b[1]);
            try testing.expect(v1.is(tc.exp));
            try testing.expect(v1.isRequest());
            try testing.expect(v2.isResponse());
        }
    }
}

test "compression algorith: fromString" {
    try testing.expectEqual(CompressionAlgorithm.LZ4, try CompressionAlgorithm.fromString("lz4"));
    try testing.expectEqual(CompressionAlgorithm.Snappy, try CompressionAlgorithm.fromString("snappy"));
    try testing.expectError(error.InvalidCompressionAlgorithm, CompressionAlgorithm.fromString("foobar"));
}

pub const MessageReader = struct {
    const Self = @This();

    buffer: io.FixedBufferStream([]const u8),
    reader: io.FixedBufferStream([]const u8).Reader,

    pub fn reset(self: *Self, buf: []const u8) void {
        self.buffer = io.fixedBufferStream(buf);
        self.reader = self.buffer.reader();
    }

    /// Read either a short, a int or a long from the buffer.
    pub fn readInt(self: *Self, comptime T: type) !T {
        return self.reader.readInt(T, .big);
    }

    /// Read a single byte from the buffer.
    pub fn readByte(self: *Self) !u8 {
        return self.reader.readByte();
    }

    /// Read a length-prefixed byte slice from the stream. The length is 2 bytes.
    /// The slice can be null.
    pub fn readShortBytes(self: *Self, allocator: mem.Allocator) !?[]const u8 {
        return self.readBytesGeneric(allocator, i16);
    }

    /// Read a length-prefixed byte slice from the stream. The length is 4 bytes.
    /// The slice can be null.
    pub fn readBytes(self: *Self, allocator: mem.Allocator) !?[]const u8 {
        return self.readBytesGeneric(allocator, i32);
    }

    /// Read bytes from the stream in a generic way.
    fn readBytesGeneric(self: *Self, allocator: mem.Allocator, comptime LenType: type) !?[]const u8 {
        const len = try self.readInt(LenType);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return &[_]u8{};
        }

        const buf = try allocator.alloc(u8, @as(usize, @intCast(len)));

        const n_read = try self.reader.readAll(buf);
        if (n_read != len) {
            return error.UnexpectedEOF;
        }

        return buf;
    }

    /// Read a length-prefixed string from the stream. The length is 2 bytes.
    /// The string can't be null.
    pub fn readString(self: *Self, allocator: mem.Allocator) ![]const u8 {
        if (try self.readBytesGeneric(allocator, i16)) |v| {
            return v;
        } else {
            return error.UnexpectedEOF;
        }
    }

    /// Read a length-prefixed string from the stream. The length is 4 bytes.
    /// The string can't be null.
    pub fn readLongString(self: *Self, allocator: mem.Allocator) ![]const u8 {
        if (try self.readBytesGeneric(allocator, i32)) |v| {
            return v;
        } else {
            return error.UnexpectedEOF;
        }
    }

    /// Read a UUID from the stream.
    pub fn readUUID(self: *Self) ![16]u8 {
        var buf: [16]u8 = undefined;
        _ = try self.reader.readAll(&buf);
        return buf;
    }

    /// Read a list of string from the stream.
    pub fn readStringList(self: *Self, allocator: mem.Allocator) ![]const []const u8 {
        const len = @as(usize, try self.readInt(u16));

        var list = try std.ArrayList([]const u8).initCapacity(allocator, len);

        var i: usize = 0;
        while (i < len) {
            const tmp = try self.readString(allocator);
            try list.append(tmp);

            i += 1;
        }

        return try list.toOwnedSlice();
    }

    /// Read a value from the stream.
    pub fn readValue(self: *Self, allocator: mem.Allocator) !Value {
        const len = try self.readInt(i32);

        if (len >= 0) {
            const result = try allocator.alloc(u8, @intCast(len));
            _ = try self.reader.readAll(result);

            return Value{ .Set = result };
        } else if (len == -1) {
            return Value.Null;
        } else if (len == -2) {
            return Value.NotSet;
        } else {
            return error.InvalidValueLength;
        }
    }

    pub fn readUnsignedVint(self: *Self, comptime IntType: type) !IntType {
        const bits = switch (@typeInfo(IntType)) {
            .Int => |info| blk: {
                comptime std.debug.assert(info.bits >= 16);
                break :blk info.bits;
            },
            else => unreachable,
        };

        var shift: meta.Int(.unsigned, std.math.log2(bits)) = 0;

        var res: IntType = 0;

        while (true) {
            const b = try self.reader.readByte();
            const tmp = @as(IntType, @intCast(b)) & ~@as(IntType, 0x80);

            // TODO(vincent): runtime check if the number will actually fit in the type T ?

            res |= (tmp << shift);

            if (b & 0x80 == 0) {
                break;
            }

            shift += 7;
        }

        return res;
    }

    pub fn readVint(self: *Self, comptime IntType: type) !IntType {
        const bits = switch (@typeInfo(IntType)) {
            .Int => |info| blk: {
                comptime std.debug.assert(info.signedness == .signed);
                comptime std.debug.assert(info.bits >= 16);

                break :blk info.bits;
            },
            else => unreachable,
        };

        const UnsignedType = meta.Int(.unsigned, bits);

        const tmp = try self.readUnsignedVint(UnsignedType);

        const lhs: IntType = @intCast(tmp >> 1);
        const rhs = -@as(IntType, @intCast(tmp & 1));

        const n = lhs ^ rhs;

        return n;
    }

    pub inline fn readInetaddr(self: *Self) !net.Address {
        return self.readInetGeneric(false);
    }
    pub inline fn readInet(self: *Self) !net.Address {
        return self.readInetGeneric(true);
    }

    fn readInetGeneric(self: *Self, comptime with_port: bool) !net.Address {
        const n = try self.readByte();

        return switch (n) {
            4 => {
                var buf: [4]u8 = undefined;
                _ = try self.reader.readAll(&buf);

                const port = if (with_port) try self.readInt(i32) else 0;

                return net.Address.initIp4(buf, @intCast(port));
            },
            16 => {
                var buf: [16]u8 = undefined;
                _ = try self.reader.readAll(&buf);

                const port = if (with_port) try self.readInt(i32) else 0;

                return net.Address.initIp6(buf, @intCast(port), 0, 0);
            },
            else => return error.InvalidInetSize,
        };
    }

    pub fn readConsistency(self: *Self) !Consistency {
        const n = try self.readInt(u16);
        return @enumFromInt(n);
    }

    pub fn readStringMap(self: *Self, allocator: mem.Allocator) !string_map.Map {
        const n = try self.readInt(u16);

        var map = string_map.Map.init(allocator);

        var i: usize = 0;
        while (i < n) : (i += 1) {
            const k = try self.readString(allocator);
            const v = try self.readString(allocator);

            _ = try map.put(k, v);
        }

        return map;
    }

    pub fn readStringMultimap(self: *Self, allocator: mem.Allocator) !string_map.Multimap {
        const n = try self.readInt(u16);

        var map = string_map.Multimap.init(allocator);

        var i: usize = 0;
        while (i < n) : (i += 1) {
            const k = try self.readString(allocator);
            const list = try self.readStringList(allocator);

            _ = try map.put(k, list);
        }

        return map;
    }
};

test "message reader: read int" {
    var mr: MessageReader = undefined;
    mr.reset("\x00\x20\x11\x00");
    try testing.expectEqual(@as(i32, 2101504), try mr.readInt(i32));

    mr.reset("\x00\x00\x40\x00\x00\x20\x11\x00");
    try testing.expectEqual(@as(i64, 70368746279168), try mr.readInt(i64));

    mr.reset("\x11\x00");
    try testing.expectEqual(@as(u16, 4352), try mr.readInt(u16));

    mr.reset("\xff");
    try testing.expectEqual(@as(u8, 0xFF), try mr.readByte());
}

test "message reader: read strings and bytes" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mr: MessageReader = undefined;
    {
        // short string
        mr.reset("\x00\x06foobar");
        try testing.expectEqualStrings("foobar", try mr.readString(arena.allocator()));

        // long string
        mr.reset("\x00\x00\x00\x06foobar");
        try testing.expectEqualStrings("foobar", try mr.readLongString(arena.allocator()));
    }

    {
        // int32 + bytes
        mr.reset("\x00\x00\x00\x0A123456789A");
        try testing.expectEqualStrings("123456789A", (try mr.readBytes(arena.allocator())).?);

        mr.reset("\x00\x00\x00\x00");
        try testing.expectEqualStrings("", (try mr.readBytes(arena.allocator())).?);

        mr.reset("\xff\xff\xff\xff");
        try testing.expect((try mr.readBytes(arena.allocator())) == null);
    }

    {
        // int16 + bytes
        mr.reset("\x00\x0A123456789A");
        try testing.expectEqualStrings("123456789A", (try mr.readShortBytes(arena.allocator())).?);

        mr.reset("\x00\x00");
        try testing.expectEqualStrings("", (try mr.readShortBytes(arena.allocator())).?);

        mr.reset("\xff\xff");
        try testing.expect((try mr.readShortBytes(arena.allocator())) == null);
    }
}

test "message reader: read uuid" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var uuid: [16]u8 = undefined;
    try std.posix.getrandom(&uuid);

    var mr: MessageReader = undefined;
    mr.reset(&uuid);

    try testing.expectEqualSlices(u8, &uuid, &(try mr.readUUID()));
}

test "message reader: read string list" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mr: MessageReader = undefined;
    mr.reset("\x00\x02\x00\x03foo\x00\x03bar");

    const result = try mr.readStringList(arena.allocator());
    try testing.expectEqual(@as(usize, 2), result.len);

    var tmp = result[0];
    try testing.expectEqualStrings("foo", tmp);

    tmp = result[1];
    try testing.expectEqualStrings("bar", tmp);
}

test "message reader: read value" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // Normal value
    var mr: MessageReader = undefined;
    mr.reset("\x00\x00\x00\x02\x61\x62");

    const value = try mr.readValue(arena.allocator());
    try testing.expect(value == .Set);
    try testing.expectEqualStrings("ab", value.Set);

    // Null value

    mr.reset("\xff\xff\xff\xff");
    const value2 = try mr.readValue(arena.allocator());
    try testing.expect(value2 == .Null);

    // "Not set" value

    mr.reset("\xff\xff\xff\xfe");
    const value3 = try mr.readValue(arena.allocator());
    try testing.expect(value3 == .NotSet);
}

test "read unsigned vint" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var pw = try PrimitiveWriter.init(arena.allocator());
    try pw.writeUnsignedVint(@as(u64, 282240));
    try pw.writeUnsignedVint(@as(u32, 140022));
    try pw.writeUnsignedVint(@as(u16, 24450));

    var mr: MessageReader = undefined;
    mr.reset(pw.getWritten());

    const n1 = try mr.readUnsignedVint(u64);
    try testing.expect(n1 == @as(u64, 282240));
    const n2 = try mr.readUnsignedVint(u32);
    try testing.expect(n2 == @as(u32, 140022));
    const n3 = try mr.readUnsignedVint(u16);
    try testing.expect(n3 == @as(u16, 24450));
}

test "read vint" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var pw = try PrimitiveWriter.init(arena.allocator());
    try pw.writeVint(@as(i64, 282240));
    try pw.writeVint(@as(i64, -2400));
    try pw.writeVint(@as(i32, -38000));
    try pw.writeVint(@as(i32, 80000000));

    var mr: MessageReader = undefined;
    mr.reset(pw.getWritten());

    const n1 = try mr.readVint(i64);
    try testing.expect(n1 == @as(i64, 282240));
    const n2 = try mr.readVint(i64);
    try testing.expect(n2 == @as(i64, -2400));
    const n3 = try mr.readVint(i32);
    try testing.expect(n3 == @as(i32, -38000));
    const n4 = try mr.readVint(i32);
    try testing.expect(n4 == @as(i32, 80000000));
}

test "message reader: read inet and inetaddr" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mr: MessageReader = undefined;

    // IPv4
    mr.reset("\x04\x12\x34\x56\x78\x00\x00\x00\x22");

    var result = try mr.readInet();
    try testing.expectEqual(@as(u16, posix.AF.INET), result.any.family);
    try testing.expectEqual(@as(u32, 0x78563412), result.in.sa.addr);
    try testing.expectEqual(@as(u16, 34), result.getPort());

    // IPv6
    mr.reset("\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x22");

    result = try mr.readInet();
    try testing.expectEqual(@as(u16, posix.AF.INET6), result.any.family);
    try testing.expectEqualSlices(u8, &[_]u8{0xff} ** 16, &result.in6.sa.addr);
    try testing.expectEqual(@as(u16, 34), result.getPort());

    // IPv4 without port
    mr.reset("\x04\x12\x34\x56\x78");

    result = try mr.readInetaddr();
    try testing.expectEqual(@as(u16, posix.AF.INET), result.any.family);
    try testing.expectEqual(@as(u32, 0x78563412), result.in.sa.addr);
    try testing.expectEqual(@as(u16, 0), result.getPort());

    // IPv6 without port
    mr.reset("\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff");

    result = try mr.readInetaddr();
    try testing.expectEqual(@as(u16, posix.AF.INET6), result.any.family);
    try testing.expectEqualSlices(u8, &[_]u8{0xff} ** 16, &result.in6.sa.addr);
    try testing.expectEqual(@as(u16, 0), result.getPort());
}

test "message reader: read consistency" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const testCase = struct {
        exp: Consistency,
        b: []const u8,
    };

    const testCases = [_]testCase{
        testCase{ .exp = Consistency.Any, .b = "\x00\x00" },
        testCase{ .exp = Consistency.One, .b = "\x00\x01" },
        testCase{ .exp = Consistency.Two, .b = "\x00\x02" },
        testCase{ .exp = Consistency.Three, .b = "\x00\x03" },
        testCase{ .exp = Consistency.Quorum, .b = "\x00\x04" },
        testCase{ .exp = Consistency.All, .b = "\x00\x05" },
        testCase{ .exp = Consistency.LocalQuorum, .b = "\x00\x06" },
        testCase{ .exp = Consistency.EachQuorum, .b = "\x00\x07" },
        testCase{ .exp = Consistency.Serial, .b = "\x00\x08" },
        testCase{ .exp = Consistency.LocalSerial, .b = "\x00\x09" },
        testCase{ .exp = Consistency.LocalOne, .b = "\x00\x0A" },
    };

    for (testCases) |tc| {
        var mr: MessageReader = undefined;
        mr.reset(tc.b);

        const result = try mr.readConsistency();
        try testing.expectEqual(tc.exp, result);
    }
}

test "message reader: read stringmap" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // 2 elements string map

    var mr: MessageReader = undefined;
    mr.reset("\x00\x02\x00\x03foo\x00\x03baz\x00\x03bar\x00\x03baz");

    var result = try mr.readStringMap(arena.allocator());
    try testing.expectEqual(@as(usize, 2), result.count());

    var it = result.iterator();
    while (it.next()) |entry| {
        try testing.expect(std.mem.eql(u8, "foo", entry.key_ptr.*) or std.mem.eql(u8, "bar", entry.key_ptr.*));
        try testing.expectEqualStrings("baz", entry.value_ptr.*);
    }
}

test "message reader: read string multimap" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // 1 key, 2 values multimap

    var mr: MessageReader = undefined;
    mr.reset("\x00\x01\x00\x03foo\x00\x02\x00\x03bar\x00\x03baz");

    var result = try mr.readStringMultimap(arena.allocator());
    try testing.expectEqual(@as(usize, 1), result.count());

    const slice = result.get("foo").?;
    try testing.expectEqualStrings("bar", slice[0]);
    try testing.expectEqualStrings("baz", slice[1]);
}

pub const PrimitiveWriter = struct {
    const Self = @This();

    wbuf: std.ArrayList(u8),

    pub fn init(allocator: mem.Allocator) !Self {
        return Self{
            .wbuf = try std.ArrayList(u8).initCapacity(
                allocator,
                1024,
            ),
        };
    }

    pub fn deinit(self: *Self) void {
        self.wbuf.deinit();
    }

    pub fn reset(self: *Self) void {
        self.wbuf.clearAndFree();
    }

    pub fn toOwnedSlice(self: *Self) ![]u8 {
        return self.wbuf.toOwnedSlice();
    }

    pub fn getWritten(self: *Self) []u8 {
        return self.wbuf.items;
    }

    /// Write either a short, a int or a long to the buffer.
    pub fn writeInt(self: *Self, comptime T: type, value: T) !void {
        var buf: [(@typeInfo(T).Int.bits + 7) / 8]u8 = undefined;
        mem.writeInt(T, &buf, value, .big);

        return self.wbuf.appendSlice(&buf);
    }

    /// Write a byte to the buffer.
    pub fn writeByte(self: *Self, value: u8) !void {
        return self.wbuf.append(value);
    }

    /// Write a length-prefixed byte slice to the buffer. The length is 2 bytes.
    /// The slice can be null.
    pub fn writeShortBytes(self: *Self, value: ?[]const u8) !void {
        return self.writeBytesGeneric(i16, value);
    }

    /// Write a length-prefixed byte slice to the buffer. The length is 4 bytes.
    /// The slice can be null.
    pub fn writeBytes(self: *Self, value: ?[]const u8) !void {
        return self.writeBytesGeneric(i32, value);
    }

    /// Write bytes from the stream in a generic way.
    fn writeBytesGeneric(self: *Self, comptime LenType: type, value: ?[]const u8) !void {
        if (value) |v| {
            try self.writeInt(LenType, @intCast(v.len));
            return self.wbuf.appendSlice(v);
        } else {
            try self.writeInt(LenType, @intCast(-1));
        }
    }

    /// Write a length-prefixed string to the buffer. The length is 2 bytes.
    /// The string can't be null.
    pub fn writeString(self: *Self, value: []const u8) !void {
        return self.writeBytesGeneric(i16, value);
    }

    /// Write a length-prefixed string to the buffer. The length is 4 bytes.
    /// The string can't be null.
    pub fn writeLongString(self: *Self, value: []const u8) !void {
        return self.writeBytesGeneric(i32, value);
    }

    /// Write a UUID to the buffer.
    pub fn writeUUID(self: *Self, uuid: [16]u8) !void {
        return self.wbuf.appendSlice(&uuid);
    }

    /// Write a list of string to the buffer.
    pub fn writeStringList(self: *Self, list: []const []const u8) !void {
        try self.writeInt(u16, @intCast(list.len));
        for (list) |value| {
            _ = try self.writeString(value);
        }
    }

    /// Write a value to the buffer.
    pub fn writeValue(self: *Self, value: Value) !void {
        return switch (value) {
            .Null => self.writeInt(i32, @as(i32, -1)),
            .NotSet => self.writeInt(i32, @as(i32, -2)),
            .Set => |data| {
                try self.writeInt(i32, @intCast(data.len));
                return self.wbuf.appendSlice(data);
            },
        };
    }

    pub fn writeUnsignedVint(self: *Self, n: anytype) !void {
        switch (@typeInfo(@TypeOf(n))) {
            .Int => |info| {
                comptime std.debug.assert(info.signedness == .unsigned);
            },
            else => unreachable,
        }

        //

        var tmp = n;

        var tmp_buf = [_]u8{0} ** 9;
        var i: usize = 0;

        // split into 7 bits chunks with the most significant bit set when there are more bytes to read.
        //
        // 0x80 == 128 == 0b1000_0000
        // If the number is greater than or equal to that, it must be encoded as a chunk.

        while (tmp >= 0x80) {
            // a chunk is:
            // * the least significant 7 bits
            // * the most significant  bit set to 1
            tmp_buf[i] = @as(u8, @truncate(tmp & 0x7F)) | 0x80;
            tmp >>= 7;
            i += 1;
        }

        // the remaining chunk that is less than 128. The most significant bit must not be set.
        tmp_buf[i] = @truncate(tmp);

        try self.wbuf.appendSlice(tmp_buf[0 .. i + 1]);
    }

    pub fn writeVint(self: *Self, n: anytype) !void {
        const bits = switch (@typeInfo(@TypeOf(n))) {
            .Int => |info| blk: {
                comptime std.debug.assert(info.bits == 32 or info.bits == 64);
                comptime std.debug.assert(info.signedness == .signed);

                break :blk info.bits;
            },
            else => unreachable,
        };

        const UnsignedType = std.meta.Int(.unsigned, bits);

        const lhs: UnsignedType = @bitCast(n >> (bits - 1));
        const rhs: UnsignedType = @bitCast(n << 1);

        const tmp = lhs ^ rhs;

        try self.writeUnsignedVint(tmp);
    }

    pub inline fn writeInetaddr(self: *Self, inet: net.Address) !void {
        return self.writeInetGeneric(inet, false);
    }

    pub inline fn writeInet(self: *Self, inet: net.Address) !void {
        return self.writeInetGeneric(inet, true);
    }

    /// Write a net address to the buffer.
    fn writeInetGeneric(self: *Self, inet: net.Address, comptime with_port: bool) !void {
        switch (inet.any.family) {
            posix.AF.INET => {
                if (with_port) {
                    var buf: [9]u8 = undefined;
                    buf[0] = 4;
                    mem.writeInt(u32, buf[1..5], inet.in.sa.addr, .little);
                    mem.writeInt(u32, buf[5..9], inet.getPort(), .big);
                    return self.wbuf.appendSlice(&buf);
                } else {
                    var buf: [5]u8 = undefined;
                    buf[0] = 4;
                    mem.writeInt(u32, buf[1..5], inet.in.sa.addr, .little);
                    return self.wbuf.appendSlice(&buf);
                }
            },
            posix.AF.INET6 => {
                if (with_port) {
                    var buf: [21]u8 = undefined;
                    buf[0] = 16;
                    mem.copyForwards(u8, buf[1..17], &inet.in6.sa.addr);
                    mem.writeInt(u32, buf[17..21], inet.getPort(), .big);
                    return self.wbuf.appendSlice(&buf);
                } else {
                    var buf: [17]u8 = undefined;
                    buf[0] = 16;
                    mem.copyForwards(u8, buf[1..17], &inet.in6.sa.addr);
                    return self.wbuf.appendSlice(&buf);
                }
            },
            else => |af| std.debug.panic("invalid address family {}\n", .{af}),
        }
    }

    pub fn writeConsistency(self: *Self, consistency: Consistency) !void {
        const n: u16 = @intCast(@intFromEnum(consistency));
        return self.writeInt(u16, n);
    }

    pub fn startStringMap(self: *Self, size: usize) !void {
        _ = try self.writeInt(u16, @intCast(size));
    }
};

test "primitive writer: write int" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var pw = try PrimitiveWriter.init(arena.allocator());

    try pw.writeInt(i32, 2101504);
    try testing.expectEqualSlices(u8, "\x00\x20\x11\x00", pw.getWritten()[0..4]);

    try pw.writeInt(i64, 70368746279168);
    try testing.expectEqualSlices(u8, "\x00\x00\x40\x00\x00\x20\x11\x00", pw.getWritten()[4..12]);

    try pw.writeInt(i16, 4352);
    try testing.expectEqualSlices(u8, "\x11\x00", pw.getWritten()[12..14]);

    try pw.writeByte(0xFF);
    try testing.expectEqualSlices(u8, "\xff", pw.getWritten()[14..15]);
}

test "primitive writer: write strings and bytes" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var pw = try PrimitiveWriter.init(arena.allocator());

    {
        // short string
        _ = try pw.writeString("foobar");
        try testing.expectEqualSlices(u8, "\x00\x06foobar", pw.getWritten()[0..8]);

        // long string

        _ = try pw.writeLongString("foobar");
        try testing.expectEqualSlices(u8, "\x00\x00\x00\x06foobar", pw.getWritten()[8..18]);
    }

    {
        // int32 + bytes
        _ = try pw.writeBytes("123456789A");
        try testing.expectEqualSlices(u8, "\x00\x00\x00\x0A123456789A", pw.getWritten()[18..32]);

        _ = try pw.writeBytes("");
        try testing.expectEqualSlices(u8, "\x00\x00\x00\x00", pw.getWritten()[32..36]);

        _ = try pw.writeBytes(null);
        try testing.expectEqualSlices(u8, "\xff\xff\xff\xff", pw.getWritten()[36..40]);
    }

    {
        // int16 + bytes
        _ = try pw.writeShortBytes("123456789A");
        try testing.expectEqualSlices(u8, "\x00\x0A123456789A", pw.getWritten()[40..52]);

        _ = try pw.writeShortBytes("");
        try testing.expectEqualSlices(u8, "\x00\x00", pw.getWritten()[52..54]);

        _ = try pw.writeShortBytes(null);
        try testing.expectEqualSlices(u8, "\xff\xff", pw.getWritten()[54..56]);
    }
}

test "primitive writer: write uuid" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var pw = try PrimitiveWriter.init(arena.allocator());

    var uuid: [16]u8 = undefined;
    try std.posix.getrandom(&uuid);

    _ = try pw.writeUUID(uuid);
    try testing.expectEqualSlices(u8, &uuid, pw.getWritten()[0..16]);
}

test "primitive writer: write string list" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var pw = try PrimitiveWriter.init(arena.allocator());

    const list = &[_][]const u8{ "foo", "bar" };

    _ = try pw.writeStringList(list);
    try testing.expectEqualSlices(u8, "\x00\x02\x00\x03foo\x00\x03bar", pw.getWritten()[0..12]);
}

test "primitive writer: write value" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var pw = try PrimitiveWriter.init(arena.allocator());

    // Normal value
    _ = try pw.writeValue(Value{ .Set = "ab" });
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x02\x61\x62", pw.getWritten()[0..6]);

    // Null value
    _ = try pw.writeValue(Value{ .Null = {} });
    try testing.expectEqualSlices(u8, "\xff\xff\xff\xff", pw.getWritten()[6..10]);

    // "Not set" value
    _ = try pw.writeValue(Value{ .NotSet = {} });
    try testing.expectEqualSlices(u8, "\xff\xff\xff\xfe", pw.getWritten()[10..14]);
}

test "primitive writer: write inet and inetaddr" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var pw = try PrimitiveWriter.init(arena.allocator());

    // IPv4
    _ = try pw.writeInet(net.Address.initIp4([_]u8{ 0x78, 0x56, 0x34, 0x12 }, 34));
    try testing.expectEqualSlices(u8, "\x04\x78\x56\x34\x12\x00\x00\x00\x22", pw.getWritten()[0..9]);

    // IPv6
    _ = try pw.writeInet(net.Address.initIp6([_]u8{0xff} ** 16, 34, 0, 0));
    try testing.expectEqualSlices(u8, "\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x22", pw.getWritten()[9..30]);

    // IPv4 without port
    _ = try pw.writeInetaddr(net.Address.initIp4([_]u8{ 0x78, 0x56, 0x34, 0x12 }, 34));
    try testing.expectEqualSlices(u8, "\x04\x78\x56\x34\x12", pw.getWritten()[30..35]);

    // IPv6 without port
    _ = try pw.writeInetaddr(net.Address.initIp6([_]u8{0xff} ** 16, 34, 0, 0));
    try testing.expectEqualSlices(u8, "\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", pw.getWritten()[35..52]);
}

test "primitive writer: write consistency" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var pw = try PrimitiveWriter.init(arena.allocator());

    const testCase = struct {
        consistency: Consistency,
        exp: []const u8,
    };

    const testCases = [_]testCase{
        testCase{ .consistency = Consistency.Any, .exp = "\x00\x00" },
        testCase{ .consistency = Consistency.One, .exp = "\x00\x01" },
        testCase{ .consistency = Consistency.Two, .exp = "\x00\x02" },
        testCase{ .consistency = Consistency.Three, .exp = "\x00\x03" },
        testCase{ .consistency = Consistency.Quorum, .exp = "\x00\x04" },
        testCase{ .consistency = Consistency.All, .exp = "\x00\x05" },
        testCase{ .consistency = Consistency.LocalQuorum, .exp = "\x00\x06" },
        testCase{ .consistency = Consistency.EachQuorum, .exp = "\x00\x07" },
        testCase{ .consistency = Consistency.Serial, .exp = "\x00\x08" },
        testCase{ .consistency = Consistency.LocalSerial, .exp = "\x00\x09" },
        testCase{ .consistency = Consistency.LocalOne, .exp = "\x00\x0A" },
    };

    for (testCases) |tc| {
        pw.reset();

        _ = try pw.writeConsistency(tc.consistency);
        try testing.expectEqualSlices(u8, tc.exp, pw.getWritten()[0..2]);
    }
}
