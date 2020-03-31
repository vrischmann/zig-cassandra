const std = @import("std");
const mem = std.mem;
const net = std.net;
const testing = std.testing;
const ArrayList = std.ArrayList;

const Framer = @import("framer.zig").Framer;
const sm = @import("string_map.zig");
usingnamespace @import("primitive_types.zig");

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

// Below are request frames only (ie client -> server).

/// STARTUP is sent to a node to initialize a connection.
///
/// Described in the protocol spec at §4.1.1.
const StartupFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    cql_version: []const u8,
    compression: ?CompressionAlgorithm,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.cql_version);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !StartupFrame {
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
            if (!mem.eql(u8, "3.0.0", version.value)) {
                return StartupFrameError.InvalidCQLVersion;
            }
            frame.cql_version = try mem.dupe(allocator, u8, version.value);
        } else {
            return StartupFrameError.InvalidCQLVersion;
        }

        if (map.get("COMPRESSION")) |compression| {
            if (mem.eql(u8, compression.value, "lz4")) {
                frame.compression = CompressionAlgorithm.LZ4;
            } else if (mem.eql(u8, compression.value, "snappy")) {
                frame.compression = CompressionAlgorithm.Snappy;
            } else {
                return StartupFrameError.InvalidCompression;
            }
        }

        return frame;
    }
};

/// AUTH_RESPONSE is sent to a node to answser a authentication challenge.
///
/// Described in the protocol spec at §4.1.2.
const AuthResponseFrame = struct {};

/// OPTIONS is sent to a node to ask which STARTUP options are supported.
///
/// Described in the protocol spec at §4.1.3.
const OptionsFrame = struct {};

/// QUERY is sent to ask the node to perform a CQL query.
///
/// Described in the protocol spec at §4.1.4
const QueryFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    const NamedValue = struct {
        name: []const u8,
        value: ?Value,
    };

    const FlagWithValues: u32 = 0x0001;
    const FlagSkipMetadata: u32 = 0x0002;
    const FlagWithPageSize: u32 = 0x0004;
    const FlagWithPagingState: u32 = 0x0008;
    const FlagWithSerialConsistency: u32 = 0x0010;
    const FlagWithDefaultTimestamp: u32 = 0x0020;
    const FlagWithNamedValues: u32 = 0x0040;
    const FlagWithKeyspace: u32 = 0x0080;
    const FlagWithNowInSeconds: u32 = 0x100;

    query: []const u8,
    consistency_level: Consistency,

    values: ?[]?Value,
    named_values: ?[]NamedValue,
    page_size: ?u32,
    paging_state: ?[]const u8, // NOTE(vincent): not a string
    serial_consistency_level: ?Consistency,
    timestamp: ?u64,
    keyspace: ?[]const u8,
    now_in_seconds: ?u32,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.query);

        if (self.values) |values| {
            for (values) |outer_value| {
                if (outer_value) |not_null_value| {
                    switch (not_null_value) {
                        Value.Set => |inner_value| self.allocator.free(inner_value),
                        Value.NotSet => continue,
                    }
                }
            }
            self.allocator.free(values);
        }

        if (self.named_values) |values| {
            for (values) |outer_value| {
                self.allocator.free(outer_value.name);
                if (outer_value.value) |not_null_value| {
                    switch (not_null_value) {
                        Value.Set => |v| self.allocator.free(v),
                        Value.NotSet => continue,
                    }
                }
            }
            self.allocator.free(values);
        }

        if (self.paging_state) |ps| {
            self.allocator.free(ps);
        }

        if (self.keyspace) |keyspace| {
            self.allocator.free(keyspace);
        }
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !QueryFrame {
        var frame: Self = undefined;
        frame.allocator = allocator;
        frame.query = try framer.readLongString();
        frame.consistency_level = try framer.readConsistency();

        // The remaining data in the frame depends on the flags

        // The size of the flags bitmask depends on the protocol version.
        var flags: u32 = 0;
        if (framer.header.version == ProtocolVersion.V5) {
            flags = try framer.readInt(u32);
        } else {
            flags = try framer.readInt(u8);
        }

        // TODO(vincent): some flags are only valid with protocol version 5

        if (flags & FlagWithValues == FlagWithValues) {
            const n = try framer.readInt(u16);

            if (flags & FlagWithNamedValues == FlagWithNamedValues) {
                var list = std.ArrayList(NamedValue).init(allocator);
                errdefer list.deinit();

                var i: usize = 0;
                while (i < @as(usize, n)) : (i += 1) {
                    const nm = NamedValue{
                        .name = try framer.readString(),
                        .value = try framer.readValue(),
                    };
                    _ = try list.append(nm);
                }

                frame.named_values = list.toOwnedSlice();
            } else {
                var list = std.ArrayList(?Value).init(allocator);
                errdefer list.deinit();

                var i: usize = 0;
                while (i < @as(usize, n)) : (i += 1) {
                    const value = try framer.readValue();
                    _ = try list.append(value);
                }

                frame.values = list.toOwnedSlice();
            }
        }
        if (flags & FlagSkipMetadata == FlagSkipMetadata) {
            // Nothing to do
        }
        if (flags & FlagWithPageSize == FlagWithPageSize) {
            frame.page_size = try framer.readInt(u32);
        }
        if (flags & FlagWithPagingState == FlagWithPagingState) {
            frame.paging_state = try framer.readBytes();
        }
        if (flags & FlagWithSerialConsistency == FlagWithSerialConsistency) {
            const consistency_level = try framer.readConsistency();
            if (consistency_level != .Serial and consistency_level != .LocalSerial) {
                return error.InvalidSerialConsistency;
            }
            frame.serial_consistency_level = consistency_level;
        }
        if (flags & FlagWithDefaultTimestamp == FlagWithDefaultTimestamp) {
            const timestamp = try framer.readInt(u64);
            if (timestamp < 0) {
                return error.InvalidNegativeTimestamp;
            }
            frame.timestamp = timestamp;
        }
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            frame.keyspace = try framer.readString();
        }
        if (flags & FlagWithNowInSeconds == FlagWithNowInSeconds) {
            frame.now_in_seconds = try framer.readInt(u32);
        }

        return frame;
    }
};

// Below are response frames only (ie server -> client).

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

/// ERROR is sent by a node if there's an error processing a request.
///
/// Described in the protocol spec at §4.2.1.
const ErrorFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

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

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !ErrorFrame {
        var frame: Self = undefined;
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

/// READY is sent by a node to indicate it is ready to process queries.
///
/// Described in the protocol spec at §4.2.2.
const ReadyFrame = struct {};

/// AUTHENTICATE is sent by a node in response to a STARTUP frame if authentication is required.
///
/// Described in the protocol spec at §4.2.3.
const AuthenticateFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    authenticator: []const u8,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.authenticator);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !AuthenticateFrame {
        return AuthenticateFrame{
            .allocator = allocator,
            .authenticator = try framer.readString(),
        };
    }
};

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

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !SupportedFrame {
        const options = try framer.readStringMultimap();
        defer options.deinit();

        var frame = SupportedFrame{
            .allocator = allocator,
            .protocol_versions = &[_]ProtocolVersion{},
            .cql_versions = &[_]CQLVersion{},
            .compression_algorithms = &[_]CompressionAlgorithm{},
        };

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

test "frame: parse startup frame" {
    // from cqlsh exported via Wireshark
    const data = "\x04\x00\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x05\x33\x2e\x30\x2e\x30";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    testing.expectEqual(ProtocolVersion.V4, framer.header.version);
    testing.expectEqual(@as(u8, 0), framer.header.flags);
    testing.expectEqual(@as(i16, 0), framer.header.stream);
    testing.expectEqual(Opcode.Startup, framer.header.opcode);
    testing.expectEqual(@as(u32, 22), framer.header.body_len);
    testing.expectEqual(@as(usize, 22), data.len - @sizeOf(FrameHeader));

    const frame = try StartupFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();
}

test "error frame: invalid query, no keyspace specified" {
    const data = "\x84\x00\x00\x02\x00\x00\x00\x00\x5e\x00\x00\x22\x00\x00\x58\x4e\x6f\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x20\x68\x61\x73\x20\x62\x65\x65\x6e\x20\x73\x70\x65\x63\x69\x66\x69\x65\x64\x2e\x20\x55\x53\x45\x20\x61\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x2c\x20\x6f\x72\x20\x65\x78\x70\x6c\x69\x63\x69\x74\x6c\x79\x20\x73\x70\x65\x63\x69\x66\x79\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x2e\x74\x61\x62\x6c\x65\x6e\x61\x6d\x65";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    testing.expectEqual(ProtocolVersion.V4, framer.header.version);
    testing.expectEqual(@as(u8, 0), framer.header.flags);
    testing.expectEqual(@as(i16, 2), framer.header.stream);
    testing.expectEqual(Opcode.Error, framer.header.opcode);
    testing.expectEqual(@as(u32, 94), framer.header.body_len);
    testing.expectEqual(@as(usize, 94), data.len - @sizeOf(FrameHeader));

    const frame = try ErrorFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(ErrorCode.InvalidQuery, frame.error_code);
    testing.expectEqualSlices(u8, "No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename", frame.message);
}

test "error frame: already exists" {
    const data = "\x84\x00\x00\x23\x00\x00\x00\x00\x53\x00\x00\x24\x00\x00\x3e\x43\x61\x6e\x6e\x6f\x74\x20\x61\x64\x64\x20\x61\x6c\x72\x65\x61\x64\x79\x20\x65\x78\x69\x73\x74\x69\x6e\x67\x20\x74\x61\x62\x6c\x65\x20\x22\x68\x65\x6c\x6c\x6f\x22\x20\x74\x6f\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x20\x22\x66\x6f\x6f\x62\x61\x72\x22\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x05\x68\x65\x6c\x6c\x6f";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    testing.expectEqual(ProtocolVersion.V4, framer.header.version);
    testing.expectEqual(@as(u8, 0), framer.header.flags);
    testing.expectEqual(@as(i16, 35), framer.header.stream);
    testing.expectEqual(Opcode.Error, framer.header.opcode);
    testing.expectEqual(@as(u32, 83), framer.header.body_len);
    testing.expectEqual(@as(usize, 83), data.len - @sizeOf(FrameHeader));

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

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    testing.expectEqual(ProtocolVersion.V4, framer.header.version);
    testing.expectEqual(@as(u8, 0), framer.header.flags);
    testing.expectEqual(@as(i16, 47), framer.header.stream);
    testing.expectEqual(Opcode.Error, framer.header.opcode);
    testing.expectEqual(@as(u32, 65), framer.header.body_len);
    testing.expectEqual(@as(usize, 65), data.len - @sizeOf(FrameHeader));

    const frame = try ErrorFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(ErrorCode.SyntaxError, frame.error_code);
    testing.expectEqualSlices(u8, "line 2:0 mismatched input ';' expecting K_FROM (select*[;])", frame.message);
}

test "ready frame" {
    const data = "\x84\x00\x00\x02\x02\x00\x00\x00\x00";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    testing.expectEqual(ProtocolVersion.V4, framer.header.version);
    testing.expectEqual(@as(u8, 0), framer.header.flags);
    testing.expectEqual(@as(i16, 2), framer.header.stream);
    testing.expectEqual(Opcode.Ready, framer.header.opcode);
    testing.expectEqual(@as(u32, 0), framer.header.body_len);
    testing.expectEqual(@as(usize, 0), data.len - @sizeOf(FrameHeader));
}

test "authenticate frame" {
    const data = "\x84\x00\x00\x00\x03\x00\x00\x00\x31\x00\x2f\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x2e\x61\x75\x74\x68\x2e\x50\x61\x73\x73\x77\x6f\x72\x64\x41\x75\x74\x68\x65\x6e\x74\x69\x63\x61\x74\x6f\x72";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    testing.expectEqual(ProtocolVersion.V4, framer.header.version);
    testing.expectEqual(@as(u8, 0), framer.header.flags);
    testing.expectEqual(@as(i16, 0), framer.header.stream);
    testing.expectEqual(Opcode.Authenticate, framer.header.opcode);
    testing.expectEqual(@as(u32, 49), framer.header.body_len);
    testing.expectEqual(@as(usize, 49), data.len - @sizeOf(FrameHeader));

    const frame = try AuthenticateFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqualSlices(u8, "org.apache.cassandra.auth.PasswordAuthenticator", frame.authenticator);
}

test "options frame" {
    const data = "\x04\x00\x00\x05\x05\x00\x00\x00\x00";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    testing.expectEqual(ProtocolVersion.V4, framer.header.version);
    testing.expectEqual(@as(u8, 0), framer.header.flags);
    testing.expectEqual(@as(i16, 5), framer.header.stream);
    testing.expectEqual(Opcode.Options, framer.header.opcode);
    testing.expectEqual(@as(u32, 0), framer.header.body_len);
    testing.expectEqual(@as(usize, 0), data.len - @sizeOf(FrameHeader));
}

test "supported frame" {
    const data = "\x84\x00\x00\x09\x06\x00\x00\x00\x60\x00\x03\x00\x11\x50\x52\x4f\x54\x4f\x43\x4f\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x53\x00\x03\x00\x04\x33\x2f\x76\x33\x00\x04\x34\x2f\x76\x34\x00\x09\x35\x2f\x76\x35\x2d\x62\x65\x74\x61\x00\x0b\x43\x4f\x4d\x50\x52\x45\x53\x53\x49\x4f\x4e\x00\x02\x00\x06\x73\x6e\x61\x70\x70\x79\x00\x03\x6c\x7a\x34\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x01\x00\x05\x33\x2e\x34\x2e\x34";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    testing.expectEqual(ProtocolVersion.V4, framer.header.version);
    testing.expectEqual(@as(u8, 0), framer.header.flags);
    testing.expectEqual(@as(i16, 9), framer.header.stream);
    testing.expectEqual(Opcode.Supported, framer.header.opcode);
    testing.expectEqual(@as(u32, 96), framer.header.body_len);
    testing.expectEqual(@as(usize, 96), data.len - @sizeOf(FrameHeader));

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

test "query frame" {
    const data = "\x04\x00\x00\x08\x07\x00\x00\x00\x30\x00\x00\x00\x1b\x53\x45\x4c\x45\x43\x54\x20\x2a\x20\x46\x52\x4f\x4d\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x20\x3b\x00\x01\x34\x00\x00\x00\x64\x00\x08\x00\x05\xa2\x2c\xf0\x57\x3e\x3f";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    testing.expectEqual(ProtocolVersion.V4, framer.header.version);
    testing.expectEqual(@as(u8, 0), framer.header.flags);
    testing.expectEqual(@as(i16, 8), framer.header.stream);
    testing.expectEqual(Opcode.Query, framer.header.opcode);
    testing.expectEqual(@as(u32, 48), framer.header.body_len);
    testing.expectEqual(@as(usize, 48), data.len - @sizeOf(FrameHeader));

    const frame = try QueryFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqualSlices(u8, "SELECT * FROM foobar.user ;", frame.query);
    testing.expectEqual(Consistency.One, frame.consistency_level);
    testing.expect(frame.values == null);
    testing.expect(frame.named_values == null);
    testing.expectEqual(@as(u32, 100), frame.page_size.?);
    testing.expect(frame.paging_state == null);
    testing.expectEqual(Consistency.Serial, frame.serial_consistency_level.?);
    testing.expectEqual(@as(u64, 1585688778063423), frame.timestamp.?);
    testing.expect(frame.keyspace == null);
    testing.expect(frame.now_in_seconds == null);
}
