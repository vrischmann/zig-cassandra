const std = @import("std");
const builtin = @import("builtin");
const testing = std.testing;
const ArrayList = std.ArrayList;

const ProtocolVersion = packed enum(u8) {
    V3,
    V4,
    V5,

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

const FrameFlags = packed enum(u8) {
    Compression = 0x01,
    Tracing = 0x02,
    CustomPayload = 0x04,
    Warning = 0x08,
    UseBeta = 0x10,
};

const Opcode = packed enum(u8) {
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

const FrameHeader = packed struct {
    version: ProtocolVersion,
    flags: u8,
    stream: u16,
    opcode: Opcode,
    body_len: u32,
};

const CompressionAlgorithm = enum {};

const ValueTag = enum {
    Set,
    NotSet,
};
const Value = union(ValueTag) {
    Set: []u8,
    NotSet: void,
};

const StartupFrame = struct {
    cql_version: []const u8,
    compression: ?CompressionAlgorithm,
};

fn readStartupFrame(allocator: *std.mem.Allocator, deserializer: FrameDeserializer) !StartupFrame {
    const map = try deserializer.readStringMap();

    unreachable;
}

pub fn FrameDeserializer(comptime InStreamType: type) type {
    const BytesType = enum {
        Short,
        Long,
    };

    return struct {
        const Self = @This();

        allocator: *std.mem.Allocator,
        in_stream: InStreamType,

        pub fn init(allocator: *std.mem.Allocator, in: InStreamType) Self {
            return Self{
                .allocator = allocator,
                .in_stream = in,
            };
        }

        pub fn deinit(self: *Self) void {}

        /// Read either a short, a int or a long from the stream.
        pub fn readInt(self: *Self, comptime T: type) !T {
            return self.in_stream.readIntBig(T);
        }

        /// Read a single byte from the stream
        pub fn readByte(self: *Self) !u8 {
            return self.in_stream.readByte();
        }

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
            return self.readStringGeneric(.Short);
        }

        /// Read a length-prefixed string from the stream. The length is 4 bytes.
        /// The string can't be null.
        pub fn readLongString(self: *Self) ![]const u8 {
            return self.readStringGeneric(.Long);
        }

        /// Read a string from the stream in a generic way.
        fn readStringGeneric(self: *Self, comptime T: BytesType) ![]const u8 {
            if (try self.readBytesGeneric(T)) |v| {
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
        pub fn readStringList(self: *Self) !ArrayList([]const u8) {
            const len = @as(usize, try self.readInt(u16));

            var list = try ArrayList([]const u8).initCapacity(self.allocator, len);

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
        pub fn readValue(self: *Self) !?Value {
            const len = try self.readInt(i32);

            if (len >= 0) {
                const result = try self.allocator.alloc(u8, @intCast(usize, len));
                _ = try self.in_stream.readAll(result);

                return Value{ .Set = result };
            } else if (len == -1) {
                return null;
            } else if (len == -2) {
                return Value.NotSet;
            } else {
                return error.InvalidValueLength;
            }
        }
    };
}

pub fn Frame(comptime T: type) type {
    return struct {
        const Self = @This();

        header: FrameHeader,
        body: T,

        pub fn init(header: FrameHeader, body: T) Self {
            return Self{
                .header = header,
                .body = body,
            };
        }
    };
}

fn resetAndWrite(comptime T: type, fbs: *T, data: []const u8) void {
    fbs.reset();
    _ = fbs.write(data) catch |err| {
        unreachable;
    };
    fbs.reset();
}

test "frame deserializer" {
    // Do some setup
    //

    // Make a reusable stream
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    // Make our frame deserializer
    var d = FrameDeserializer(@TypeOf(in_stream)).init(std.testing.allocator, in_stream);

    // Int types

    resetAndWrite(fbs_type, &fbs, "\x00\x20\x11\x00");
    testing.expectEqual(@as(i32, 2101504), try d.readInt(i32));

    resetAndWrite(fbs_type, &fbs, "\x00\x00\x40\x00\x00\x20\x11\x00");
    testing.expectEqual(@as(i64, 70368746279168), try d.readInt(i64));

    resetAndWrite(fbs_type, &fbs, "\x11\x00");
    testing.expectEqual(@as(u16, 4352), try d.readInt(u16));

    resetAndWrite(fbs_type, &fbs, "\xff");
    testing.expectEqual(@as(u8, 0xFF), try d.readByte());

    // Strings
    {
        resetAndWrite(fbs_type, &fbs, "\x00\x06foobar");
        var result = try d.readString();

        defer std.testing.allocator.free(result);
        testing.expectEqualSlices(u8, "foobar", result);

        resetAndWrite(fbs_type, &fbs, "\x00\x00\x00\x06foobar");
        result = try d.readLongString();

        defer std.testing.allocator.free(result);
        testing.expectEqualSlices(u8, "foobar", result);
    }

    // UUID
    {
        var uuid: [16]u8 = undefined;
        try std.os.getrandom(&uuid);
        resetAndWrite(fbs_type, &fbs, &uuid);

        testing.expectEqualSlices(u8, &uuid, &(try d.readUUID()));
    }

    // String list
    {
        resetAndWrite(fbs_type, &fbs, "\x00\x02\x00\x03foo\x00\x03bar");

        var list = try d.readStringList();
        defer list.deinit();

        var result = list.toOwnedSlice();
        defer std.testing.allocator.free(result);

        testing.expectEqual(@as(usize, 2), result.len);

        var tmp = result[0];
        defer std.testing.allocator.free(tmp);
        testing.expectEqualSlices(u8, "foo", tmp);

        tmp = result[1];
        defer std.testing.allocator.free(tmp);
        testing.expectEqualSlices(u8, "bar", tmp);
    }

    // Bytes and Short bytes
    {
        // int32 + bytes
        resetAndWrite(fbs_type, &fbs, "\x00\x00\x00\x0A123456789A");
        var result = try d.readBytes();
        if (result) |bytes| {
            defer std.testing.allocator.free(bytes);
            testing.expectEqualSlices(u8, "123456789A", bytes);
        } else {
            std.debug.panic("expected bytes to not be null", .{});
        }

        resetAndWrite(fbs_type, &fbs, "\x00\x00\x00\x00");
        result = try d.readBytes();
        if (result) |bytes| {
            defer std.testing.allocator.free(bytes);
            testing.expectEqualSlices(u8, "", bytes);
        } else {
            std.debug.panic("expected bytes to not be null", .{});
        }

        resetAndWrite(fbs_type, &fbs, "\xff\xff\xff\xff");
        result = try d.readBytes();
        testing.expect(result == null);

        // int16 + bytes
        resetAndWrite(fbs_type, &fbs, "\x00\x0A123456789A");
        result = try d.readShortBytes();
        if (result) |bytes| {
            defer std.testing.allocator.free(bytes);
            testing.expectEqualSlices(u8, "123456789A", bytes);
        } else {
            std.debug.panic("expected bytes to not be null", .{});
        }

        resetAndWrite(fbs_type, &fbs, "\x00\x00");
        result = try d.readShortBytes();
        if (result) |bytes| {
            defer std.testing.allocator.free(bytes);
            testing.expectEqualSlices(u8, "", bytes);
        } else {
            std.debug.panic("expected bytes to not be null", .{});
        }

        resetAndWrite(fbs_type, &fbs, "\xff\xff");
        result = try d.readShortBytes();
        testing.expect(result == null);
    }

    // Value
    {
        // Normal value
        resetAndWrite(fbs_type, &fbs, "\x00\x00\x00\x02\xFE\xFF");

        var value = try d.readValue();
        if (value) |v| {
            testing.expect(v == .Set);

            const bytes = v.Set;
            defer std.testing.allocator.free(bytes);
            testing.expectEqualSlices(u8, "\xFE\xFF", bytes);
        } else {
            std.debug.panic("expected bytes to not be null", .{});
        }

        // Null value

        resetAndWrite(fbs_type, &fbs, "\xff\xff\xff\xff");

        value = try d.readValue();
        if (value) |v| {
            std.debug.panic("expected bytes to be null", .{});
        }

        // "Not set" value

        resetAndWrite(fbs_type, &fbs, "\xff\xff\xff\xfe");

        value = try d.readValue();
        if (value) |v| {
            testing.expect(v == .NotSet);
        } else {
            std.debug.panic("expected bytes to not be null", .{});
        }
    }
}

test "parse protocol version" {
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

        var deserializer = std.io.deserializer(builtin.Endian.Big, std.io.Packing.Byte, in.inStream());
        if (tc.err) |err| {
            testing.expectError(err, deserializer.deserialize(ProtocolVersion));
        } else {
            testing.expectEqual(tc.exp, try deserializer.deserialize(ProtocolVersion));
            testing.expectEqual(tc.exp, try deserializer.deserialize(ProtocolVersion));
        }
    }
}

test "parse startup frame header" {
    // from cqlsh exported via Wireshark
    const frame = "\x04\x00\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x05\x33\x2e\x30\x2e\x30";
    var in = std.io.fixedBufferStream(frame);

    var deserializer = std.io.deserializer(builtin.Endian.Big, std.io.Packing.Byte, in.inStream());

    const header = try deserializer.deserialize(FrameHeader);

    testing.expectEqual(ProtocolVersion.V4, header.version);
    testing.expectEqual(@as(u8, 0), header.flags);
    testing.expectEqual(@as(u16, 0), header.stream);
    testing.expectEqual(Opcode.Startup, header.opcode);
    testing.expectEqual(@as(u32, 22), header.body_len);
}

test "parse startup framer" {
    // from cqlsh exported via Wireshark
    const frame = "\x04\x00\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x05\x33\x2e\x30\x2e\x30";
    var in = std.io.fixedBufferStream(frame);

    var deserializer = std.io.deserializer(builtin.Endian.Big, std.io.Packing.Byte, in.inStream());

    const header = try deserializer.deserialize(FrameHeader);
}
