const std = @import("std");
const builtin = @import("builtin");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
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

pub const FrameHeader = packed struct {
    version: ProtocolVersion,
    flags: u8,
    stream: i16,
    opcode: Opcode,
    body_len: u32,

    pub fn write(self: @This(), comptime OutStreamType: type, out: OutStreamType) !void {
        var serializer = std.io.serializer(builtin.Endian.Big, std.io.Packing.Bit, out);
        _ = try serializer.serialize(self);
    }

    pub fn read(comptime InStreamType: type, in: InStreamType) !FrameHeader {
        var deserializer = std.io.deserializer(builtin.Endian.Big, std.io.Packing.Bit, in);
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
    Null,
};
pub const Value = union(ValueTag) {
    Set: []u8,
    NotSet: void,
    Null: void,

    pub fn deinit(self: @This(), allocator: *mem.Allocator) void {
        switch (self) {
            Value.Set => |inner_value| allocator.free(inner_value),
            Value.NotSet, Value.Null => return,
        }
    }
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

    pub fn deinit(self: @This(), allocator: *mem.Allocator) void {
        switch (self) {
            .Normal => |nv| {
                for (nv) |v| {
                    v.deinit(allocator);
                }
                allocator.free(nv);
            },
            .Named => |nv| {
                for (nv) |v| {
                    allocator.free(v.name);
                    v.value.deinit(allocator);
                }
                allocator.free(nv);
            },
            else => unreachable,
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
    allocator: *mem.Allocator,

    keyspace: []const u8,
    object_name: []const u8,
    arguments: ?[][]const u8,

    pub fn deinit(self: *const @This()) void {
        self.allocator.free(self.keyspace);
        self.allocator.free(self.object_name);
        if (self.arguments) |args| {
            for (args) |arg| {
                self.allocator.free(arg);
            }
            self.allocator.free(args);
        }
    }

    pub fn init(allocator: *mem.Allocator) SchemaChangeOptions {
        return SchemaChangeOptions{
            .allocator = allocator,
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

    pub fn deinit(self: *const Self) void {
        self.options.deinit();
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var change = Self{
            .type = undefined,
            .target = undefined,
            .options = undefined,
        };

        const type_string = try framer.readString();
        defer allocator.free(type_string);

        const target_string = try framer.readString();
        defer allocator.free(target_string);

        change.type = meta.stringToEnum(SchemaChangeType, type_string) orelse return error.InvalidSchemaChangeType;
        change.target = meta.stringToEnum(SchemaChangeTarget, target_string) orelse return error.InvalidSchemaChangeTarget;

        change.options = SchemaChangeOptions.init(allocator);

        switch (change.target) {
            .KEYSPACE => {
                change.options.keyspace = try framer.readString();
            },
            .TABLE, .TYPE => {
                change.options.keyspace = try framer.readString();
                change.options.object_name = try framer.readString();
            },
            .FUNCTION, .AGGREGATE => {
                change.options.keyspace = try framer.readString();
                change.options.object_name = try framer.readString();
                change.options.arguments = (try framer.readStringList()).toOwnedSlice();
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

test "frame header: read and write" {
    const exp = "\x04\x00\x00\xd7\x05\x00\x00\x00\x00";
    var fbs = std.io.fixedBufferStream(exp);

    // deserialize the header

    var header = try FrameHeader.read(@TypeOf(fbs.inStream()), fbs.inStream());
    testing.expectEqual(ProtocolVersion.V4, header.version);
    testing.expectEqual(@as(u8, 0), header.flags);
    testing.expectEqual(@as(i16, 215), header.stream);
    testing.expectEqual(Opcode.Options, header.opcode);
    testing.expectEqual(@as(u32, 0), header.body_len);
    testing.expectEqual(@as(usize, 0), exp.len - @sizeOf(FrameHeader));

    // reserialize it

    var new_buf: [32]u8 = undefined;
    var new_fbs = std.io.fixedBufferStream(&new_buf);

    _ = try header.write(@TypeOf(new_fbs.outStream()), new_fbs.outStream());
}

test "schema change options" {
    var options = SchemaChangeOptions.init(testing.allocator);
    defer options.deinit();

    options.keyspace = try mem.dupe(testing.allocator, u8, "foobar");
    options.object_name = try mem.dupe(testing.allocator, u8, "barbaz");
    var arguments = try testing.allocator.alloc([]const u8, 4);
    var i: usize = 0;
    while (i < arguments.len) : (i += 1) {
        arguments[i] = try mem.dupe(testing.allocator, u8, "hello");
    }
    options.arguments = arguments;
}

pub fn checkHeader(opcode: Opcode, data_len: usize, header: FrameHeader) void {
    // We can only use v4 for now
    testing.expectEqual(ProtocolVersion.V4, header.version);
    // Don't care about the flags here
    // Don't care about the stream
    testing.expectEqual(opcode, header.opcode);
    testing.expectEqual(@as(usize, header.body_len), data_len - @sizeOf(FrameHeader));
}
