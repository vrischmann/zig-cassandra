const std = @import("std");
const io = std.io;
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const os = std.os;

const sm = @import("string_map.zig");

const testing = @import("testing.zig");

pub const ValueTag = enum {
    Set,
    NotSet,
    Null,
};
pub const Value = union(ValueTag) {
    Set: []const u8,
    NotSet: void,
    Null: void,
};

pub const NamedValue = struct {
    name: []const u8,
    value: Value,
};

pub const ValuesType = enum {
    Normal,
    Named,
};

pub const Values = union(ValuesType) {
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

pub const TopologyChangeType = enum {
    NEW_NODE,
    REMOVED_NODE,
};

pub const StatusChangeType = enum {
    UP,
    DOWN,
};

pub const SchemaChangeType = enum {
    CREATED,
    UPDATED,
    DROPPED,
};

pub const SchemaChangeTarget = enum {
    KEYSPACE,
    TABLE,
    TYPE,
    FUNCTION,
    AGGREGATE,
};

pub const SchemaChangeOptions = struct {
    keyspace: []const u8,
    object_name: []const u8,
    arguments: ?[][]const u8,

    pub fn init() SchemaChangeOptions {
        return SchemaChangeOptions{
            .keyspace = &[_]u8{},
            .object_name = &[_]u8{},
            .arguments = null,
        };
    }
};

pub const TopologyChange = struct {
    type: TopologyChangeType,
    node_address: net.Address,
};

pub const StatusChange = struct {
    type: StatusChangeType,
    node_address: net.Address,
};

pub const SchemaChange = struct {
    const Self = @This();

    type: SchemaChangeType,
    target: SchemaChangeTarget,
    options: SchemaChangeOptions,

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !Self {
        var change = Self{
            .type = undefined,
            .target = undefined,
            .options = undefined,
        };

        change.type = meta.stringToEnum(SchemaChangeType, (try pr.readString())) orelse return error.InvalidSchemaChangeType;
        change.target = meta.stringToEnum(SchemaChangeTarget, (try pr.readString())) orelse return error.InvalidSchemaChangeTarget;

        change.options = SchemaChangeOptions.init();

        switch (change.target) {
            .KEYSPACE => {
                change.options.keyspace = try pr.readString();
            },
            .TABLE, .TYPE => {
                change.options.keyspace = try pr.readString();
                change.options.object_name = try pr.readString();
            },
            .FUNCTION, .AGGREGATE => {
                change.options.keyspace = try pr.readString();
                change.options.object_name = try pr.readString();
                change.options.arguments = (try pr.readStringList()).toOwnedSlice();
            },
        }

        return change;
    }
};

pub const EventType = enum {
    TOPOLOGY_CHANGE,
    STATUS_CHANGE,
    SCHEMA_CHANGE,
};

pub const Event = union(EventType) {
    TOPOLOGY_CHANGE: TopologyChange,
    STATUS_CHANGE: StatusChange,
    SCHEMA_CHANGE: SchemaChange,
};

pub const PrimitiveReader = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    rbuf: []const u8,
    source: io.StreamSource,
    in_stream: io.StreamSource.InStream,

    pub fn init(allocator: *mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .rbuf = undefined,
            .source = undefined,
            .in_stream = undefined,
        };
    }

    pub fn reset(self: *Self, rbuf: []const u8) void {
        self.rbuf = rbuf;
        self.source = io.StreamSource{ .const_buffer = io.fixedBufferStream(rbuf) };
        self.in_stream = self.source.inStream();
    }

    /// Read either a short, a int or a long from the stream.
    pub fn readInt(self: *Self, comptime T: type) !T {
        return self.in_stream.readIntBig(T);
    }

    /// Read a single byte from the stream
    pub fn readByte(self: *Self) !u8 {
        return self.in_stream.readByte();
    }

    const BytesType = enum {
        Short,
        Long,
    };

    /// Read a length-prefixed byte slice from the stream. The length is 2 bytes.
    /// The slice can be null.
    pub fn readShortBytes(self: *Self) !?[]const u8 {
        return self.readBytesGeneric(.Short);
    }

    /// Read a length-prefixed byte slice from the stream. The length is 4 bytes.
    /// The slice can be null.
    pub fn readBytes(self: *Self) !?[]const u8 {
        return self.readBytesGeneric(.Long);
    }

    /// Read bytes from the stream in a generic way.
    fn readBytesGeneric(self: *Self, comptime T: BytesType) !?[]const u8 {
        const len = switch (T) {
            .Short => @as(i32, try self.readInt(i16)),
            .Long => @as(i32, try self.readInt(i32)),
            else => @compileError("invalid bytes length type " ++ @typeName(IntType)),
        };

        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return &[_]u8{};
        }

        const buf = try self.allocator.alloc(u8, @intCast(usize, len));

        const n_read = try self.in_stream.readAll(buf);
        if (n_read != len) {
            return error.UnexpectedEOF;
        }

        return buf;
    }

    /// Read a length-prefixed string from the stream. The length is 2 bytes.
    /// The string can't be null.
    pub fn readString(self: *Self) ![]const u8 {
        if (try self.readBytesGeneric(.Short)) |v| {
            return v;
        } else {
            return error.UnexpectedEOF;
        }
    }

    /// Read a length-prefixed string from the stream. The length is 4 bytes.
    /// The string can't be null.
    pub fn readLongString(self: *Self) ![]const u8 {
        if (try self.readBytesGeneric(.Long)) |v| {
            return v;
        } else {
            return error.UnexpectedEOF;
        }
    }

    /// Read a UUID from the stream.
    pub fn readUUID(self: *Self) ![16]u8 {
        var buf: [16]u8 = undefined;
        _ = try self.in_stream.readAll(&buf);
        return buf;
    }

    /// Read a list of string from the stream.
    pub fn readStringList(self: *Self) !std.ArrayList([]const u8) {
        const len = @as(usize, try self.readInt(u16));

        var list = try std.ArrayList([]const u8).initCapacity(self.allocator, len);

        var i: usize = 0;
        while (i < len) {
            const tmp = try self.readString();
            try list.append(tmp);

            i += 1;
        }

        return list;
    }

    /// Read a value from the stream.
    /// A value can be null.
    pub fn readValue(self: *Self) !Value {
        const len = try self.readInt(i32);

        if (len >= 0) {
            const result = try self.allocator.alloc(u8, @intCast(usize, len));
            _ = try self.in_stream.readAll(result);

            return Value{ .Set = result };
        } else if (len == -1) {
            return Value.Null;
        } else if (len == -2) {
            return Value.NotSet;
        } else {
            return error.InvalidValueLength;
        }
    }

    pub fn readVarint(self: *Self, comptime IntType: type) !IntType {
        // TODO(vincent): implement this for uvint and vint
        unreachable;
    }

    pub fn readInetaddr(self: *Self) !net.Address {
        return self.readInetGeneric(false);
    }

    pub fn readInet(self: *Self) !net.Address {
        return self.readInetGeneric(true);
    }

    fn readInetGeneric(self: *Self, with_port: bool) !net.Address {
        const n = try self.readByte();

        return switch (n) {
            4 => {
                var buf: [4]u8 = undefined;
                _ = try self.in_stream.readAll(&buf);

                const port = if (with_port) try self.readInt(i32) else 0;

                return net.Address.initIp4(buf, @intCast(u16, port));
            },
            16 => {
                var buf: [16]u8 = undefined;
                _ = try self.in_stream.readAll(&buf);

                const port = if (with_port) try self.readInt(i32) else 0;

                return net.Address.initIp6(buf, @intCast(u16, port), 0, 0);
            },
            else => return error.InvalidInetSize,
        };
    }

    pub fn readConsistency(self: *Self) !Consistency {
        const n = try self.readInt(u16);

        return @intToEnum(Consistency, n);
    }

    pub fn readStringMap(self: *Self) !sm.Map {
        const n = try self.readInt(u16);

        var map = sm.Map.init(self.allocator);

        var i: usize = 0;
        while (i < n) : (i += 1) {
            const k = try self.readString();
            const v = try self.readString();

            _ = try map.put(k, v);
        }

        return map;
    }

    pub fn readStringMultimap(self: *Self) !sm.Multimap {
        const n = try self.readInt(u16);

        var map = sm.Multimap.init(self.allocator);

        var i: usize = 0;
        while (i < n) : (i += 1) {
            const k = try self.readString();
            const list = try self.readStringList();

            _ = try map.put(k, list);
        }

        return map;
    }
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

        var d = std.io.deserializer(std.builtin.Endian.Big, std.io.Packing.Byte, in.inStream());
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

test "schema change options" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var options = SchemaChangeOptions.init();

    options.keyspace = try mem.dupe(&arena.allocator, u8, "foobar");
    options.object_name = try mem.dupe(&arena.allocator, u8, "barbaz");
    var arguments = try arena.allocator.alloc([]const u8, 4);
    var i: usize = 0;
    while (i < arguments.len) : (i += 1) {
        arguments[i] = try mem.dupe(&arena.allocator, u8, "hello");
    }
    options.arguments = arguments;
}

test "primitive reader: read int" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init(&arena.allocator);

    // read all int types

    pr.reset("\x00\x20\x11\x00");
    testing.expectEqual(@as(i32, 2101504), try pr.readInt(i32));

    pr.reset("\x00\x00\x40\x00\x00\x20\x11\x00");
    testing.expectEqual(@as(i64, 70368746279168), try pr.readInt(i64));

    pr.reset("\x11\x00");
    testing.expectEqual(@as(u16, 4352), try pr.readInt(u16));

    pr.reset("\xff");
    testing.expectEqual(@as(u8, 0xFF), try pr.readByte());
}

test "primitive reader: read strings and bytes" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init(&arena.allocator);

    // short string
    {
        pr.reset("\x00\x06foobar");
        testing.expectEqualString("foobar", try pr.readString());

        // long string

        pr.reset("\x00\x00\x00\x06foobar");
        testing.expectEqualString("foobar", try pr.readLongString());
    }

    // int32 + bytes
    {
        pr.reset("\x00\x00\x00\x0A123456789A");
        testing.expectEqualString("123456789A", (try pr.readBytes()).?);

        pr.reset("\x00\x00\x00\x00");
        testing.expectEqualString("", (try pr.readBytes()).?);

        pr.reset("\xff\xff\xff\xff");
        testing.expect((try pr.readBytes()) == null);
    }

    // int16 + bytes
    {
        pr.reset("\x00\x0A123456789A");
        testing.expectEqualString("123456789A", (try pr.readShortBytes()).?);

        pr.reset("\x00\x00");
        testing.expectEqualString("", (try pr.readShortBytes()).?);

        pr.reset("\xff\xff");
        testing.expect((try pr.readShortBytes()) == null);
    }
}

test "primitive reader: read uuid" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init(&arena.allocator);

    // read UUID

    var uuid: [16]u8 = undefined;
    try std.os.getrandom(&uuid);
    pr.reset(&uuid);

    testing.expectEqualSlices(u8, &uuid, &(try pr.readUUID()));
}

test "primitive reader: read string list" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init(&arena.allocator);

    // read string lists

    pr.reset("\x00\x02\x00\x03foo\x00\x03bar");

    var list = try pr.readStringList();
    defer list.deinit();

    var result = list.toOwnedSlice();
    testing.expectEqual(@as(usize, 2), result.len);

    var tmp = result[0];
    testing.expectEqualString("foo", tmp);

    tmp = result[1];
    testing.expectEqualString("bar", tmp);
}

test "primitive reader: read value" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init(&arena.allocator);

    // Normal value
    pr.reset("\x00\x00\x00\x02\x61\x62");

    var value = try pr.readValue();
    testing.expect(value == .Set);
    testing.expectEqualString("ab", value.Set);

    // Null value

    pr.reset("\xff\xff\xff\xff");
    var value2 = try pr.readValue();
    testing.expect(value2 == .Null);

    // "Not set" value

    pr.reset("\xff\xff\xff\xfe");
    var value3 = try pr.readValue();
    testing.expect(value3 == .NotSet);
}

test "primitive reader: read inet and inetaddr" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init(&arena.allocator);

    // IPv4
    pr.reset("\x04\x12\x34\x56\x78\x00\x00\x00\x22");

    var result = try pr.readInet();
    testing.expectEqual(@as(u16, os.AF_INET), result.any.family);
    testing.expectEqual(@as(u32, 0x78563412), result.in.addr);
    testing.expectEqual(@as(u16, 34), result.getPort());

    // IPv6
    pr.reset("\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x22");

    result = try pr.readInet();
    testing.expectEqual(@as(u16, os.AF_INET6), result.any.family);
    testing.expectEqualSlices(u8, "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", &result.in6.addr);
    testing.expectEqual(@as(u16, 34), result.getPort());

    // IPv4 without port
    pr.reset("\x04\x12\x34\x56\x78");

    result = try pr.readInetaddr();
    testing.expectEqual(@as(u16, os.AF_INET), result.any.family);
    testing.expectEqual(@as(u32, 0x78563412), result.in.addr);
    testing.expectEqual(@as(u16, 0), result.getPort());

    // IPv6 without port
    pr.reset("\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff");

    result = try pr.readInetaddr();
    testing.expectEqual(@as(u16, os.AF_INET6), result.any.family);
    testing.expectEqualSlices(u8, "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", &result.in6.addr);
    testing.expectEqual(@as(u16, 0), result.getPort());
}

test "primitive reader: read consistency" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init(&arena.allocator);

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
        pr.reset(tc.b);
        var result = try pr.readConsistency();
        testing.expectEqual(tc.exp, result);
    }
}

test "primitive reader: read stringmap" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init(&arena.allocator);

    // 2 elements string map

    pr.reset("\x00\x02\x00\x03foo\x00\x03baz\x00\x03bar\x00\x03baz");

    var result = try pr.readStringMap();
    testing.expectEqual(@as(usize, 2), result.count());

    var it = result.iterator();
    while (it.next()) |entry| {
        testing.expect(std.mem.eql(u8, "foo", entry.key) or std.mem.eql(u8, "bar", entry.key));
        testing.expectEqualString("baz", entry.value);
    }
}

test "primitive reader: read string multimap" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init(&arena.allocator);

    // 1 key, 2 values multimap

    pr.reset("\x00\x01\x00\x03foo\x00\x02\x00\x03bar\x00\x03baz");

    var result = try pr.readStringMultimap();
    testing.expectEqual(@as(usize, 1), result.count());

    var it = result.iterator();
    if (it.next()) |entry| {
        testing.expect(std.mem.eql(u8, "foo", entry.key));

        const slice = entry.value.span();

        testing.expectEqual(@as(usize, 2), slice.len);
        testing.expectEqualString("bar", slice[0]);
        testing.expectEqualString("baz", slice[1]);
    } else {
        std.debug.panic("expected bytes to not be null", .{});
    }
}
