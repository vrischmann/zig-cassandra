const std = @import("std");
const builtin = @import("builtin");
const net = std.net;
const os = std.os;
const testing = std.testing;
const ArrayList = std.ArrayList;

const StringMap = @import("string_map.zig");

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
    stream: i16,
    opcode: Opcode,
    body_len: u32,

    pub fn read(comptime InStreamType: type, in: InStreamType) !FrameHeader {
        var deserializer = std.io.deserializer(builtin.Endian.Big, std.io.Packing.Byte, in);
        return deserializer.deserialize(FrameHeader);
    }
};

const CompressionAlgorithm = enum {
    LZ4,
    Snappy,
};

const ValueTag = enum {
    Set,
    NotSet,
};
const Value = union(ValueTag) {
    Set: []u8,
    NotSet: void,
};

const StartupFrameError = error{
    InvalidCQLVersion,
    InvalidCompression,
};

const UnavailableReplicasError = struct {
    consistency_level: Consistency,
    required: u32,
    alive: u32,
};

const FunctionFailureError = struct {
    keyspace: []const u8,
    function: []const u8,
    arg_types: std.ArrayList([]const u8),
};

const WriteError = struct {
    // TODO(vincent): document this
    const WriteType = enum {
        SIMPLE,
        BATCH,
        UNLOGGED_BATCH,
        COUNTER,
        BATCH_LOG,
        CAS,
        VIEW,
        CDC,
    };

    const Timeout = struct {
        consistency_level: Consistency,
        received: u32,
        block_for: u32,
        write_type: WriteType,
        contentions: ?u16,
    };

    const Failure = struct {
        const Reason = struct {
            endpoint: net.Address,
            // TODO(vincent): what's this failure code ?!
            failure_code: u16,
        };

        consistency_level: Consistency,
        received: u32,
        block_for: u32,
        reason_map: std.ArrayList(Reason),
        write_type: WriteType,
    };

    const CASUnknown = struct {
        consistency_level: Consistency,
        received: u32,
        block_for: u32,
    };
};

const ReadError = struct {
    const Timeout = struct {
        consistency_level: Consistency,
        received: u32,
        block_for: u32,
        data_present: u8,
    };

    const Failure = struct {
        const Reason = struct {
            endpoint: net.Address,
            // TODO(vincent): what's this failure code ?!
            failure_code: u16,
        };

        consistency_level: Consistency,
        received: u32,
        block_for: u32,
        reason_map: std.ArrayList(Reason),
        data_present: u8,
    };
};

const AlreadyExistsError = struct {
    keyspace: []const u8,
    table: []const u8,
};

const UnpreparedError = struct {
    statement_id: []const u8,
};

// TODO(vincent): test all error codes
const ErrorCode = packed enum(u32) {
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

const ErrorFrame = struct {
    const Self = @This();

    allocator: *std.mem.Allocator,

    error_code: ErrorCode,
    message: []const u8,

    unavaiable_replicas: ?UnavailableReplicasError,
    function_failure: ?FunctionFailureError,
    write_timeout: ?WriteError.Timeout,
    read_timeout: ?ReadError.Timeout,
    write_failure: ?WriteError.Failure,
    read_failure: ?ReadError.Failure,
    cas_write_unknown: ?WriteError.CASUnknown,
    already_exists: ?AlreadyExistsError,
    unprepared: ?UnpreparedError,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.message);

        if (self.function_failure) |err| {
            self.allocator.free(err.keyspace);
            self.allocator.free(err.function);
            for (err.arg_types.span()) |v| {
                self.allocator.free(v);
            }
            err.arg_types.deinit();
        }
        if (self.write_failure) |err| {
            err.reason_map.deinit();
        }
        if (self.read_failure) |err| {
            err.reason_map.deinit();
        }
        if (self.already_exists) |err| {
            self.allocator.free(err.keyspace);
            self.allocator.free(err.table);
        }
        if (self.unprepared) |err| {
            self.allocator.free(err.statement_id);
        }
    }

    pub fn read(allocator: *std.mem.Allocator, comptime FramerType: type, framer: *FramerType) !ErrorFrame {
        var frame: ErrorFrame = undefined;
        frame.allocator = allocator;
        frame.error_code = @intToEnum(ErrorCode, try framer.readInt(u32));
        frame.message = try framer.readString();

        switch (frame.error_code) {
            .UnavailableReplicas => {
                frame.unavaiable_replicas = UnavailableReplicasError{
                    .consistency_level = try framer.readConsistency(),
                    .required = try framer.readInt(u32),
                    .alive = try framer.readInt(u32),
                };
            },
            .FunctionFailure => {
                frame.function_failure = FunctionFailureError{
                    .keyspace = try framer.readString(),
                    .function = try framer.readString(),
                    .arg_types = try framer.readStringList(),
                };
            },
            .WriteTimeout => {
                var write_timeout = WriteError.Timeout{
                    .consistency_level = try framer.readConsistency(),
                    .received = try framer.readInt(u32),
                    .block_for = try framer.readInt(u32),
                    .write_type = undefined,
                    .contentions = null,
                };

                if (std.meta.stringToEnum(WriteError.WriteType, try framer.readString())) |write_type| {
                    write_timeout.write_type = write_type;
                } else {
                    return error.InvalidWriteType;
                }

                if (write_timeout.write_type == .CAS) {
                    write_timeout.contentions = try framer.readInt(u16);
                }

                frame.write_timeout = write_timeout;
            },
            .ReadTimeout => {
                frame.read_timeout = ReadError.Timeout{
                    .consistency_level = try framer.readConsistency(),
                    .received = try framer.readInt(u32),
                    .block_for = try framer.readInt(u32),
                    .data_present = try framer.readByte(),
                };
            },
            .WriteFailure => {
                var write_failure = WriteError.Failure{
                    .consistency_level = try framer.readConsistency(),
                    .received = try framer.readInt(u32),
                    .block_for = try framer.readInt(u32),
                    .reason_map = std.ArrayList(WriteError.Failure.Reason).init(allocator),
                    .write_type = undefined,
                };

                const n = try framer.readInt(u32);
                var i: usize = 0;
                while (i < n) : (i += 1) {
                    const reason = WriteError.Failure.Reason{
                        .endpoint = try framer.readInetaddr(),
                        .failure_code = try framer.readInt(u16),
                    };
                    _ = try write_failure.reason_map.append(reason);
                }

                if (std.meta.stringToEnum(WriteError.WriteType, try framer.readString())) |write_type| {
                    write_failure.write_type = write_type;
                } else {
                    return error.InvalidWriteType;
                }

                frame.write_failure = write_failure;
            },
            .ReadFailure => {
                var read_failure = ReadError.Failure{
                    .consistency_level = try framer.readConsistency(),
                    .received = try framer.readInt(u32),
                    .block_for = try framer.readInt(u32),
                    .reason_map = std.ArrayList(ReadError.Failure.Reason).init(allocator),
                    .data_present = undefined,
                };

                const n = try framer.readInt(u32);
                var i: usize = 0;
                while (i < n) : (i += 1) {
                    const reason = ReadError.Failure.Reason{
                        .endpoint = try framer.readInetaddr(),
                        .failure_code = try framer.readInt(u16),
                    };
                    _ = try read_failure.reason_map.append(reason);
                }

                read_failure.data_present = try framer.readByte();

                frame.read_failure = read_failure;
            },
            .CASWriteUnknown => {
                frame.cas_write_unknown = WriteError.CASUnknown{
                    .consistency_level = try framer.readConsistency(),
                    .received = try framer.readInt(u32),
                    .block_for = try framer.readInt(u32),
                };
            },
            .AlreadyExists => {
                frame.already_exists = AlreadyExistsError{
                    .keyspace = try framer.readString(),
                    .table = try framer.readString(),
                };
            },
            .Unprepared => {
                if (try framer.readShortBytes()) |statement_id| {
                    frame.unprepared = UnpreparedError{
                        .statement_id = statement_id,
                    };
                } else {
                    // TODO(vincent): make this a proper error ?
                    return error.InvalidStatementID;
                }
            },
            else => {},
        }

        return frame;
    }
};

const StartupFrame = struct {
    const Self = @This();

    allocator: *std.mem.Allocator,

    cql_version: []const u8,
    compression: ?CompressionAlgorithm,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.cql_version);
    }

    pub fn read(allocator: *std.mem.Allocator, comptime FramerType: type, framer: *FramerType) !StartupFrame {
        const map = try framer.readStringMap();
        defer map.deinit();

        var frame = Self{
            .allocator = allocator,
            .cql_version = undefined,
            .compression = null,
        };

        // TODO(vincent): maybe avoid copying the strings ?

        // CQL_VERSION is mandatory and the only version supported is 3.0.0 right now.
        if (map.get("CQL_VERSION")) |version| {
            if (!std.mem.eql(u8, "3.0.0", version.value)) {
                return StartupFrameError.InvalidCQLVersion;
            }
            frame.cql_version = try std.mem.dupe(allocator, u8, version.value);
        } else {
            return StartupFrameError.InvalidCQLVersion;
        }

        if (map.get("COMPRESSION")) |compression| {
            if (std.mem.eql(u8, compression.value, "lz4")) {
                frame.compression = CompressionAlgorithm.LZ4;
            } else if (std.mem.eql(u8, compression.value, "snappy")) {
                frame.compression = CompressionAlgorithm.Snappy;
            } else {
                return StartupFrameError.InvalidCompression;
            }
        }

        return frame;
    }
};

pub fn Framer(comptime InStreamType: type) type {
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

        pub fn readVarint(self: *Self, comptime IntType: type) !IntType {
            // TODO(vincent): implement this for uvint and vint
            unreachable;
        }

        // TODO(vincent): add read option

        // pub fn readOptionID(self: *Self) !u16 {
        //     return self.readInt(u16);
        // }

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

        pub fn readStringMap(self: *Self) !StringMap.Map {
            const n = try self.readInt(u16);

            var map = StringMap.Map.init(self.allocator);

            var i: usize = 0;
            while (i < n) : (i += 1) {
                // NOTE(vincent): the multimap makes a copy of both key and value
                // so we discard the strings here
                const k = try self.readString();
                defer self.allocator.free(k);

                const v = try self.readString();
                defer self.allocator.free(v);

                _ = try map.put(k, v);
            }

            return map;
        }

        pub fn readStringMultimap(self: *Self) !StringMap.Multimap {
            const n = try self.readInt(u16);

            var map = StringMap.Multimap.init(self.allocator);

            var i: usize = 0;
            while (i < n) : (i += 1) {
                // NOTE(vincent): the multimap makes a copy of both key and value
                // so we discard the strings here
                const k = try self.readString();
                defer self.allocator.free(k);

                const v = try self.readString();
                defer self.allocator.free(v);

                _ = try map.put(k, v);
            }

            return map;
        }
    };
}

const Consistency = packed enum(u16) {
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

test "framer: read int" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // read all int types

    resetAndWrite(fbs_type, &fbs, "\x00\x20\x11\x00");
    testing.expectEqual(@as(i32, 2101504), try framer.readInt(i32));

    resetAndWrite(fbs_type, &fbs, "\x00\x00\x40\x00\x00\x20\x11\x00");
    testing.expectEqual(@as(i64, 70368746279168), try framer.readInt(i64));

    resetAndWrite(fbs_type, &fbs, "\x11\x00");
    testing.expectEqual(@as(u16, 4352), try framer.readInt(u16));

    resetAndWrite(fbs_type, &fbs, "\xff");
    testing.expectEqual(@as(u8, 0xFF), try framer.readByte());
}

test "framer: read strings and bytes" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // short string
    {
        resetAndWrite(fbs_type, &fbs, "\x00\x06foobar");
        var result = try framer.readString();
        defer std.testing.allocator.free(result);

        testing.expectEqualSlices(u8, "foobar", result);

        // long string

        resetAndWrite(fbs_type, &fbs, "\x00\x00\x00\x06foobar");
        result = try framer.readLongString();
        defer std.testing.allocator.free(result);

        testing.expectEqualSlices(u8, "foobar", result);
    }

    // int32 + bytes
    {
        resetAndWrite(fbs_type, &fbs, "\x00\x00\x00\x0A123456789A");
        var result = (try framer.readBytes()).?;
        defer std.testing.allocator.free(result);
        testing.expectEqualSlices(u8, "123456789A", result);

        resetAndWrite(fbs_type, &fbs, "\x00\x00\x00\x00");
        var result2 = (try framer.readBytes()).?;
        defer std.testing.allocator.free(result2);
        testing.expectEqualSlices(u8, "", result2);

        resetAndWrite(fbs_type, &fbs, "\xff\xff\xff\xff");
        testing.expect((try framer.readBytes()) == null);
    }

    // int16 + bytes
    {
        resetAndWrite(fbs_type, &fbs, "\x00\x0A123456789A");
        var result = (try framer.readShortBytes()).?;
        defer std.testing.allocator.free(result);
        testing.expectEqualSlices(u8, "123456789A", result);

        resetAndWrite(fbs_type, &fbs, "\x00\x00");
        var result2 = (try framer.readShortBytes()).?;
        defer std.testing.allocator.free(result2);
        testing.expectEqualSlices(u8, "", result2);

        resetAndWrite(fbs_type, &fbs, "\xff\xff");
        testing.expect((try framer.readShortBytes()) == null);
    }
}

test "framer: read uuid" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // read UUID

    var uuid: [16]u8 = undefined;
    try std.os.getrandom(&uuid);
    resetAndWrite(fbs_type, &fbs, &uuid);

    testing.expectEqualSlices(u8, &uuid, &(try framer.readUUID()));
}

test "framer: read string list" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // read string lists

    resetAndWrite(fbs_type, &fbs, "\x00\x02\x00\x03foo\x00\x03bar");

    var list = try framer.readStringList();
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

test "framer: read value" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // Normal value
    resetAndWrite(fbs_type, &fbs, "\x00\x00\x00\x02\xFE\xFF");

    var value = try framer.readValue();
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

    value = try framer.readValue();
    if (value) |v| {
        std.debug.panic("expected bytes to be null", .{});
    }

    // "Not set" value

    resetAndWrite(fbs_type, &fbs, "\xff\xff\xff\xfe");

    value = try framer.readValue();
    if (value) |v| {
        testing.expect(v == .NotSet);
    } else {
        std.debug.panic("expected bytes to not be null", .{});
    }
}

test "framer: read inet and inetaddr" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // IPv4
    resetAndWrite(fbs_type, &fbs, "\x04\x12\x34\x56\x78\x00\x00\x00\x22");

    var result = try framer.readInet();
    testing.expectEqual(@as(u16, os.AF_INET), result.any.family);
    testing.expectEqual(@as(u32, 0x78563412), result.in.addr);
    testing.expectEqual(@as(u16, 34), result.getPort());

    // IPv6
    resetAndWrite(fbs_type, &fbs, "\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x22");

    result = try framer.readInet();
    testing.expectEqual(@as(u16, os.AF_INET6), result.any.family);
    testing.expectEqualSlices(u8, "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", &result.in6.addr);
    testing.expectEqual(@as(u16, 34), result.getPort());

    // IPv4 without port
    resetAndWrite(fbs_type, &fbs, "\x04\x12\x34\x56\x78");

    result = try framer.readInetaddr();
    testing.expectEqual(@as(u16, os.AF_INET), result.any.family);
    testing.expectEqual(@as(u32, 0x78563412), result.in.addr);
    testing.expectEqual(@as(u16, 0), result.getPort());

    // IPv6 without port
    resetAndWrite(fbs_type, &fbs, "\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff");

    result = try framer.readInetaddr();
    testing.expectEqual(@as(u16, os.AF_INET6), result.any.family);
    testing.expectEqualSlices(u8, "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", &result.in6.addr);
    testing.expectEqual(@as(u16, 0), result.getPort());
}

test "framer: read consistency" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

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
        resetAndWrite(fbs_type, &fbs, tc.b);
        var result = try framer.readConsistency();
        testing.expectEqual(tc.exp, result);
    }
}

test "framer: read stringmap" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // 2 elements string map

    resetAndWrite(fbs_type, &fbs, "\x00\x02\x00\x03foo\x00\x03baz\x00\x03bar\x00\x03baz");

    var result = try framer.readStringMap();
    defer result.deinit();
    testing.expectEqual(@as(usize, 2), result.count());

    var it = result.iterator();
    while (it.next()) |entry| {
        testing.expect(std.mem.eql(u8, "foo", entry.key) or std.mem.eql(u8, "bar", entry.key));
        testing.expectEqualSlices(u8, "baz", entry.value);
    }
}

test "framer: read string multimap" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // 1 key, 2 values multimap

    resetAndWrite(fbs_type, &fbs, "\x00\x02\x00\x03foo\x00\x03bar\x00\x03foo\x00\x03baz");

    var result = try framer.readStringMultimap();
    defer result.deinit();
    testing.expectEqual(@as(usize, 1), result.count());

    var it = result.iterator();
    if (it.next()) |entry| {
        testing.expect(std.mem.eql(u8, "foo", entry.key));

        const slice = entry.value.span();

        testing.expectEqual(@as(usize, 2), slice.len);
        testing.expectEqualSlices(u8, "bar", slice[0]);
        testing.expectEqualSlices(u8, "baz", slice[1]);
    } else {
        std.debug.panic("expected bytes to not be null", .{});
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

        var d = std.io.deserializer(builtin.Endian.Big, std.io.Packing.Byte, in.inStream());
        if (tc.err) |err| {
            testing.expectError(err, d.deserialize(ProtocolVersion));
        } else {
            testing.expectEqual(tc.exp, try d.deserialize(ProtocolVersion));
            testing.expectEqual(tc.exp, try d.deserialize(ProtocolVersion));
        }
    }
}

test "frame: parse startup frame" {
    // from cqlsh exported via Wireshark
    const data = "\x04\x00\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x05\x33\x2e\x30\x2e\x30";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    const header = try FrameHeader.read(@TypeOf(in_stream), in_stream);
    testing.expectEqual(ProtocolVersion.V4, header.version);
    testing.expectEqual(@as(u8, 0), header.flags);
    testing.expectEqual(@as(i16, 0), header.stream);
    testing.expectEqual(Opcode.Startup, header.opcode);
    testing.expectEqual(@as(u32, 22), header.body_len);
    testing.expectEqual(@as(usize, 22), data.len - @sizeOf(FrameHeader));

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    const frame = try StartupFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();
}

test "error frame: invalid query, no keyspace specified" {
    const data = "\x84\x00\x00\x02\x00\x00\x00\x00\x5e\x00\x00\x22\x00\x00\x58\x4e\x6f\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x20\x68\x61\x73\x20\x62\x65\x65\x6e\x20\x73\x70\x65\x63\x69\x66\x69\x65\x64\x2e\x20\x55\x53\x45\x20\x61\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x2c\x20\x6f\x72\x20\x65\x78\x70\x6c\x69\x63\x69\x74\x6c\x79\x20\x73\x70\x65\x63\x69\x66\x79\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x2e\x74\x61\x62\x6c\x65\x6e\x61\x6d\x65";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    const header = try FrameHeader.read(@TypeOf(in_stream), in_stream);
    testing.expectEqual(ProtocolVersion.V4, header.version);
    testing.expectEqual(@as(u8, 0), header.flags);
    testing.expectEqual(@as(i16, 2), header.stream);
    testing.expectEqual(Opcode.Error, header.opcode);
    testing.expectEqual(@as(u32, 94), header.body_len);
    testing.expectEqual(@as(usize, 94), data.len - @sizeOf(FrameHeader));

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    const frame = try ErrorFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(ErrorCode.InvalidQuery, frame.error_code);
    testing.expectEqualSlices(u8, "No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename", frame.message);
}

test "error frame: already exists" {
    const data = "\x84\x00\x00\x23\x00\x00\x00\x00\x53\x00\x00\x24\x00\x00\x3e\x43\x61\x6e\x6e\x6f\x74\x20\x61\x64\x64\x20\x61\x6c\x72\x65\x61\x64\x79\x20\x65\x78\x69\x73\x74\x69\x6e\x67\x20\x74\x61\x62\x6c\x65\x20\x22\x68\x65\x6c\x6c\x6f\x22\x20\x74\x6f\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x20\x22\x66\x6f\x6f\x62\x61\x72\x22\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x05\x68\x65\x6c\x6c\x6f";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    const header = try FrameHeader.read(@TypeOf(in_stream), in_stream);
    testing.expectEqual(ProtocolVersion.V4, header.version);
    testing.expectEqual(@as(u8, 0), header.flags);
    testing.expectEqual(@as(i16, 35), header.stream);
    testing.expectEqual(Opcode.Error, header.opcode);
    testing.expectEqual(@as(u32, 83), header.body_len);
    testing.expectEqual(@as(usize, 83), data.len - @sizeOf(FrameHeader));

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    const frame = try ErrorFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(ErrorCode.AlreadyExists, frame.error_code);
    testing.expectEqualSlices(u8, "Cannot add already existing table \"hello\" to keyspace \"foobar\"", frame.message);
    const already_exists_error = frame.already_exists.?;
    testing.expectEqualSlices(u8, "foobar", already_exists_error.keyspace);
    testing.expectEqualSlices(u8, "hello", already_exists_error.table);
}

test "error frame: syntax error" {
    const data = "\x84\x00\x00\x2f\x00\x00\x00\x00\x41\x00\x00\x20\x00\x00\x3b\x6c\x69\x6e\x65\x20\x32\x3a\x30\x20\x6d\x69\x73\x6d\x61\x74\x63\x68\x65\x64\x20\x69\x6e\x70\x75\x74\x20\x27\x3b\x27\x20\x65\x78\x70\x65\x63\x74\x69\x6e\x67\x20\x4b\x5f\x46\x52\x4f\x4d\x20\x28\x73\x65\x6c\x65\x63\x74\x2a\x5b\x3b\x5d\x29";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    const header = try FrameHeader.read(@TypeOf(in_stream), in_stream);
    testing.expectEqual(ProtocolVersion.V4, header.version);
    testing.expectEqual(@as(u8, 0), header.flags);
    testing.expectEqual(@as(i16, 47), header.stream);
    testing.expectEqual(Opcode.Error, header.opcode);
    testing.expectEqual(@as(u32, 65), header.body_len);
    testing.expectEqual(@as(usize, 65), data.len - @sizeOf(FrameHeader));

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    const frame = try ErrorFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(ErrorCode.SyntaxError, frame.error_code);
    testing.expectEqualSlices(u8, "line 2:0 mismatched input ';' expecting K_FROM (select*[;])", frame.message);
}
