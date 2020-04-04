const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

const Framer = @import("framer.zig").Framer;
const sm = @import("string_map.zig");
usingnamespace @import("primitive_types.zig");
const testing = @import("testing.zig");

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

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .cql_version = undefined,
            .compression = null,
        };

        // TODO(vincent): maybe avoid copying the strings ?

        const map = try framer.readStringMap();
        defer map.deinit();

        // CQL_VERSION is mandatory and the only version supported is 3.0.0 right now.
        if (map.get("CQL_VERSION")) |version| {
            if (!mem.eql(u8, "3.0.0", version.value)) {
                return error.InvalidCQLVersion;
            }
            frame.cql_version = try mem.dupe(allocator, u8, version.value);
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

/// AUTH_RESPONSE is sent to a node to answser a authentication challenge.
///
/// Described in the protocol spec at §4.1.2.
const AuthResponseFrame = struct {};

/// OPTIONS is sent to a node to ask which STARTUP options are supported.
///
/// Described in the protocol spec at §4.1.3.
const OptionsFrame = struct {};

const NamedValue = struct {
    name: []const u8,
    value: Value,
};

const ValuesType = enum {
    Normal,
    Named,
};

const Values = union(ValuesType) {
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

const QueryParameters = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    consistency_level: Consistency,
    values: ?Values,
    page_size: ?u32,
    paging_state: ?[]const u8, // NOTE(vincent): not a string
    serial_consistency_level: ?Consistency,
    timestamp: ?u64,
    keyspace: ?[]const u8,
    now_in_seconds: ?u32,

    pub fn deinit(self: *const Self) void {
        if (self.values) |values| {
            values.deinit(self.allocator);
        }

        if (self.paging_state) |ps| {
            self.allocator.free(ps);
        }

        if (self.keyspace) |keyspace| {
            self.allocator.free(keyspace);
        }
    }

    const FlagWithValues: u32 = 0x0001;
    const FlagSkipMetadata: u32 = 0x0002;
    const FlagWithPageSize: u32 = 0x0004;
    const FlagWithPagingState: u32 = 0x0008;
    const FlagWithSerialConsistency: u32 = 0x0010;
    const FlagWithDefaultTimestamp: u32 = 0x0020;
    const FlagWithNamedValues: u32 = 0x0040;
    const FlagWithKeyspace: u32 = 0x0080;
    const FlagWithNowInSeconds: u32 = 0x100;

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !QueryParameters {
        var params = QueryParameters{
            .allocator = allocator,
            .consistency_level = undefined,
            .values = null,
            .page_size = null,
            .paging_state = null,
            .serial_consistency_level = null,
            .timestamp = null,
            .keyspace = null,
            .now_in_seconds = null,
        };

        params.consistency_level = try framer.readConsistency();

        // The remaining data in the frame depends on the flags

        // The size of the flags bitmask depends on the protocol version.
        var flags: u32 = 0;
        if (framer.header.version == ProtocolVersion.V5) {
            flags = try framer.readInt(u32);
        } else {
            flags = try framer.readInt(u8);
        }

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

                params.values = Values{ .Named = list.toOwnedSlice() };
            } else {
                var list = std.ArrayList(Value).init(allocator);
                errdefer list.deinit();

                var i: usize = 0;
                while (i < @as(usize, n)) : (i += 1) {
                    const value = try framer.readValue();
                    _ = try list.append(value);
                }

                params.values = Values{ .Normal = list.toOwnedSlice() };
            }
        }
        if (flags & FlagSkipMetadata == FlagSkipMetadata) {
            // Nothing to do
        }
        if (flags & FlagWithPageSize == FlagWithPageSize) {
            params.page_size = try framer.readInt(u32);
        }
        if (flags & FlagWithPagingState == FlagWithPagingState) {
            params.paging_state = try framer.readBytes();
        }
        if (flags & FlagWithSerialConsistency == FlagWithSerialConsistency) {
            const consistency_level = try framer.readConsistency();
            if (consistency_level != .Serial and consistency_level != .LocalSerial) {
                return error.InvalidSerialConsistency;
            }
            params.serial_consistency_level = consistency_level;
        }
        if (flags & FlagWithDefaultTimestamp == FlagWithDefaultTimestamp) {
            const timestamp = try framer.readInt(u64);
            if (timestamp < 0) {
                return error.InvalidNegativeTimestamp;
            }
            params.timestamp = timestamp;
        }

        if (framer.header.version != ProtocolVersion.V5) {
            return params;
        }

        // The following flags are only valid with protocol v5
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            params.keyspace = try framer.readString();
        }
        if (flags & FlagWithNowInSeconds == FlagWithNowInSeconds) {
            params.now_in_seconds = try framer.readInt(u32);
        }

        return params;
    }
};

/// QUERY is sent to perform a CQL query.
///
/// Described in the protocol spec at §4.1.4
const QueryFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    query: []const u8,
    query_parameters: QueryParameters,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.query);
        self.query_parameters.deinit();
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .query = undefined,
            .query_parameters = undefined,
        };

        frame.query = try framer.readLongString();
        frame.query_parameters = try QueryParameters.read(allocator, FramerType, framer);

        return frame;
    }
};

/// PREPARE is sent to prepare a CQL query for later execution (through EXECUTE).
///
/// Described in the protocol spec at §4.1.5
const PrepareFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    query: []const u8,
    keyspace: ?[]const u8,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.query);
    }

    const FlagWithKeyspace = 0x01;

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .query = undefined,
            .keyspace = null,
        };

        frame.query = try framer.readLongString();

        if (framer.header.version != ProtocolVersion.V5) {
            return frame;
        }

        const flags = try framer.readInt(u32);
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            frame.keyspace = try framer.readString();
        }

        return frame;
    }
};

/// EXECUTE is sent to execute a prepared query.
///
/// Described in the protocol spec at §4.1.6
const ExecuteFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    query_id: []const u8,
    result_metadata_id: ?[]const u8,
    query_parameters: QueryParameters,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.query_id);
        if (self.result_metadata_id) |id| {
            self.allocator.free(id);
        }
        self.query_parameters.deinit();
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .query_id = undefined,
            .result_metadata_id = null,
            .query_parameters = undefined,
        };

        frame.query_id = (try framer.readShortBytes()) orelse &[_]u8{};
        if (framer.header.version == ProtocolVersion.V5) {
            frame.result_metadata_id = try framer.readShortBytes();
        }
        frame.query_parameters = try QueryParameters.read(allocator, FramerType, framer);

        return frame;
    }
};

/// Structure of a query in a BATCH frame
const BatchQuery = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    query_string: ?[]const u8,
    query_id: ?[]const u8,

    values: Values,

    pub fn deinit(self: *const Self) void {
        if (self.query_string) |query| {
            self.allocator.free(query);
        }
        if (self.query_id) |id| {
            self.allocator.free(id);
        }
        self.values.deinit(self.allocator);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !BatchQuery {
        var query = Self{
            .allocator = allocator,
            .query_string = null,
            .query_id = null,
            .values = undefined,
        };

        const kind = try framer.readByte();
        switch (kind) {
            0 => query.query_string = try framer.readLongString(),
            1 => query.query_id = try framer.readShortBytes(),
            else => return error.InvalidQueryKind,
        }

        var list = std.ArrayList(Value).init(allocator);
        errdefer list.deinit();

        const n_values = try framer.readInt(u16);
        var j: usize = 0;
        while (j < @as(usize, n_values)) : (j += 1) {
            const value = try framer.readValue();
            _ = try list.append(value);
        }

        query.values = Values{ .Normal = list.toOwnedSlice() };

        return query;
    }
};

/// BATCH is sent to execute a list of queries (prepared or not) as a batch.
///
/// Described in the protocol spec at §4.1.7
const BatchFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    batch_type: BatchType,
    queries: []BatchQuery,
    consistency_level: Consistency,
    serial_consistency_level: ?Consistency,
    timestamp: ?u64,
    keyspace: ?[]const u8,
    now_in_seconds: ?u32,

    const FlagWithSerialConsistency: u32 = 0x0010;
    const FlagWithDefaultTimestamp: u32 = 0x0020;
    const FlagWithNamedValues: u32 = 0x0040; // NOTE(vincent): the spec says this is broker so it's not implemented
    const FlagWithKeyspace: u32 = 0x0080;
    const FlagWithNowInSeconds: u32 = 0x100;

    pub fn deinit(self: *const Self) void {
        for (self.queries) |query| {
            query.deinit();
        }
        self.allocator.free(self.queries);
        if (self.keyspace) |keyspace| {
            self.allocator.free(keyspace);
        }
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .batch_type = undefined,
            .queries = undefined,
            .consistency_level = undefined,
            .serial_consistency_level = null,
            .timestamp = null,
            .keyspace = null,
            .now_in_seconds = null,
        };

        frame.batch_type = @intToEnum(BatchType, try framer.readByte());
        frame.queries = &[_]BatchQuery{};

        // Read all queries in the batch

        var queries = std.ArrayList(BatchQuery).init(allocator);
        errdefer queries.deinit();

        const n = try framer.readInt(u16);
        var i: usize = 0;
        while (i < @as(usize, n)) : (i += 1) {
            const query = try BatchQuery.read(allocator, FramerType, framer);
            _ = try queries.append(query);
        }

        frame.queries = queries.toOwnedSlice();

        // Read the rest of the frame

        frame.consistency_level = try framer.readConsistency();

        // The size of the flags bitmask depends on the protocol version.
        var flags: u32 = 0;
        if (framer.header.version == ProtocolVersion.V5) {
            flags = try framer.readInt(u32);
        } else {
            flags = try framer.readInt(u8);
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

        if (framer.header.version != ProtocolVersion.V5) {
            return frame;
        }

        // The following flags are only valid with protocol v5
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            frame.keyspace = try framer.readString();
        }
        if (flags & FlagWithNowInSeconds == FlagWithNowInSeconds) {
            frame.now_in_seconds = try framer.readInt(u32);
        }

        return frame;
    }
};

/// REGISTER is sent to register this connection to receive some types of events.
///
/// Described in the protocol spec at §4.1.8
const RegisterFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    event_types: [][]const u8,

    pub fn deinit(self: *const Self) void {
        for (self.event_types) |s| {
            self.allocator.free(s);
        }
        self.allocator.free(self.event_types);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .event_types = undefined,
        };

        frame.event_types = (try framer.readStringList()).toOwnedSlice();

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

const UnavailableReplicasError = struct {
    consistency_level: Consistency,
    required: u32,
    alive: u32,
};

const FunctionFailureError = struct {
    keyspace: []const u8,
    function: []const u8,
    arg_types: [][]const u8,
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
        reason_map: []Reason,
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
        reason_map: []Reason,
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
            for (err.arg_types) |v| {
                self.allocator.free(v);
            }
            self.allocator.free(err.arg_types);
        }
        if (self.write_failure) |err| {
            self.allocator.free(err.reason_map);
        }
        if (self.read_failure) |err| {
            self.allocator.free(err.reason_map);
        }
        if (self.already_exists) |err| {
            self.allocator.free(err.keyspace);
            self.allocator.free(err.table);
        }
        if (self.unprepared) |err| {
            self.allocator.free(err.statement_id);
        }
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .error_code = undefined,
            .message = undefined,
            .unavaiable_replicas = null,
            .function_failure = null,
            .write_timeout = null,
            .read_timeout = null,
            .write_failure = null,
            .read_failure = null,
            .cas_write_unknown = null,
            .already_exists = null,
            .unprepared = null,
        };

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
                    .arg_types = (try framer.readStringList()).toOwnedSlice(),
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

                const write_type_string = try framer.readString();
                defer allocator.free(write_type_string);

                write_timeout.write_type = meta.stringToEnum(WriteError.WriteType, write_type_string) orelse return error.InvalidWriteType;
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
                    .reason_map = undefined,
                    .write_type = undefined,
                };

                // Read reason map

                var reason_map = std.ArrayList(WriteError.Failure.Reason).init(allocator);
                errdefer reason_map.deinit();

                const n = try framer.readInt(u32);
                var i: usize = 0;
                while (i < n) : (i += 1) {
                    const reason = WriteError.Failure.Reason{
                        .endpoint = try framer.readInetaddr(),
                        .failure_code = try framer.readInt(u16),
                    };
                    _ = try reason_map.append(reason);
                }
                write_failure.reason_map = reason_map.toOwnedSlice();

                // Read the rest

                const write_type_string = try framer.readString();
                defer allocator.free(write_type_string);

                write_failure.write_type = meta.stringToEnum(WriteError.WriteType, write_type_string) orelse return error.InvalidWriteType;

                frame.write_failure = write_failure;
            },
            .ReadFailure => {
                var read_failure = ReadError.Failure{
                    .consistency_level = try framer.readConsistency(),
                    .received = try framer.readInt(u32),
                    .block_for = try framer.readInt(u32),
                    .reason_map = undefined,
                    .data_present = undefined,
                };

                // Read reason map

                var reason_map = std.ArrayList(ReadError.Failure.Reason).init(allocator);
                errdefer reason_map.deinit();

                const n = try framer.readInt(u32);
                var i: usize = 0;
                while (i < n) : (i += 1) {
                    const reason = ReadError.Failure.Reason{
                        .endpoint = try framer.readInetaddr(),
                        .failure_code = try framer.readInt(u16),
                    };
                    _ = try reason_map.append(reason);
                }
                read_failure.reason_map = reason_map.toOwnedSlice();

                // Read the rest

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

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .authenticator = undefined,
        };

        frame.authenticator = try framer.readString();

        return frame;
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

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .protocol_versions = &[_]ProtocolVersion{},
            .cql_versions = &[_]CQLVersion{},
            .compression_algorithms = &[_]CompressionAlgorithm{},
        };

        const options = try framer.readStringMultimap();
        defer options.deinit();

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

/// RESULT is the result to a query (QUERY, PREPARE, EXECUTE or BATCH messages).
///
/// Described in the protocol spec at §4.2.5.
const ResultFrame = struct {};

/// EVENT is an event pushed by the server.
///
/// Described in the protocol spec at §4.2.6.
const EventFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    event: Event,

    pub fn deinit(self: *const Self) void {
        switch (self.event) {
            .TOPOLOGY_CHANGE, .STATUS_CHANGE => return,
            .SCHEMA_CHANGE => |ev| {
                ev.deinit();
            },
        }
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .event = undefined,
        };

        const event_type_string = try framer.readString();
        defer allocator.free(event_type_string);

        const event_type = meta.stringToEnum(EventType, event_type_string) orelse return error.InvalidEventType;

        switch (event_type) {
            .TOPOLOGY_CHANGE => {
                var change = TopologyChange{
                    .change_type = undefined,
                    .node_address = undefined,
                };

                const change_type_string = try framer.readString();
                defer allocator.free(change_type_string);

                change.change_type = meta.stringToEnum(TopologyChangeType, change_type_string) orelse return error.InvalidTopologyChangeType;
                change.node_address = try framer.readInet();

                frame.event = Event{ .TOPOLOGY_CHANGE = change };

                return frame;
            },
            .STATUS_CHANGE => {
                var change = StatusChange{
                    .change_type = undefined,
                    .node_address = undefined,
                };

                const change_type_string = try framer.readString();
                defer allocator.free(change_type_string);

                change.change_type = meta.stringToEnum(StatusChangeType, change_type_string) orelse return error.InvalidStatusChangeType;
                change.node_address = try framer.readInet();

                frame.event = Event{ .STATUS_CHANGE = change };

                return frame;
            },
            .SCHEMA_CHANGE => {
                var change = SchemaChange{
                    .change_type = undefined,
                    .target = undefined,
                    .options = undefined,
                };

                const change_type_string = try framer.readString();
                defer allocator.free(change_type_string);

                const target_string = try framer.readString();
                defer allocator.free(target_string);

                change.change_type = meta.stringToEnum(SchemaChangeType, change_type_string) orelse return error.InvalidSchemaChangeType;
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

                frame.event = Event{ .SCHEMA_CHANGE = change };

                return frame;
            },
        }
    }
};

/// AUTH_CHALLENGE is a server authentication challenge.
///
/// Described in the protocol spec at §4.2.7.
const AuthChallengeFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    token: []const u8,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.token);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .token = undefined,
        };

        frame.token = try framer.readBytes();

        return frame;
    }
};

/// AUTH_SUCCESS indicates the success of the authentication phase.
///
/// Described in the protocol spec at §4.2.8.
const AuthSuccessFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    token: []const u8,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.token);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .token = undefined,
        };

        frame.token = try framer.readBytes();

        return frame;
    }
};

fn checkHeader(opcode: Opcode, data_len: usize, header: FrameHeader) void {
    // We can only use v4 for now
    testing.expectEqual(ProtocolVersion.V4, header.version);
    // Don't care about the flags here
    // Don't care about the stream
    testing.expectEqual(opcode, header.opcode);
    testing.expectEqual(@as(usize, header.body_len), data_len - @sizeOf(FrameHeader));
}

test "startup frame" {
    // from cqlsh exported via Wireshark
    const data = "\x04\x00\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x05\x33\x2e\x30\x2e\x30";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Startup, data.len, framer.header);

    const frame = try StartupFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();
}

test "auth response frame" {}

test "options frame" {
    const data = "\x04\x00\x00\x05\x05\x00\x00\x00\x00";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Options, data.len, framer.header);
}

test "query frame: no values, no paging state" {
    const data = "\x04\x00\x00\x08\x07\x00\x00\x00\x30\x00\x00\x00\x1b\x53\x45\x4c\x45\x43\x54\x20\x2a\x20\x46\x52\x4f\x4d\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x20\x3b\x00\x01\x34\x00\x00\x00\x64\x00\x08\x00\x05\xa2\x2c\xf0\x57\x3e\x3f";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Query, data.len, framer.header);

    const frame = try QueryFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectString("SELECT * FROM foobar.user ;", frame.query);
    testing.expectEqual(Consistency.One, frame.query_parameters.consistency_level);
    testing.expect(frame.query_parameters.values == null);
    testing.expectEqual(@as(u32, 100), frame.query_parameters.page_size.?);
    testing.expect(frame.query_parameters.paging_state == null);
    testing.expectEqual(Consistency.Serial, frame.query_parameters.serial_consistency_level.?);
    testing.expectEqual(@as(u64, 1585688778063423), frame.query_parameters.timestamp.?);
    testing.expect(frame.query_parameters.keyspace == null);
    testing.expect(frame.query_parameters.now_in_seconds == null);
}

test "error frame: invalid query, no keyspace specified" {
    const data = "\x84\x00\x00\x02\x00\x00\x00\x00\x5e\x00\x00\x22\x00\x00\x58\x4e\x6f\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x20\x68\x61\x73\x20\x62\x65\x65\x6e\x20\x73\x70\x65\x63\x69\x66\x69\x65\x64\x2e\x20\x55\x53\x45\x20\x61\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x2c\x20\x6f\x72\x20\x65\x78\x70\x6c\x69\x63\x69\x74\x6c\x79\x20\x73\x70\x65\x63\x69\x66\x79\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x2e\x74\x61\x62\x6c\x65\x6e\x61\x6d\x65";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Error, data.len, framer.header);

    const frame = try ErrorFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(ErrorCode.InvalidQuery, frame.error_code);
    testing.expectString("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename", frame.message);
}

test "error frame: already exists" {
    const data = "\x84\x00\x00\x23\x00\x00\x00\x00\x53\x00\x00\x24\x00\x00\x3e\x43\x61\x6e\x6e\x6f\x74\x20\x61\x64\x64\x20\x61\x6c\x72\x65\x61\x64\x79\x20\x65\x78\x69\x73\x74\x69\x6e\x67\x20\x74\x61\x62\x6c\x65\x20\x22\x68\x65\x6c\x6c\x6f\x22\x20\x74\x6f\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x20\x22\x66\x6f\x6f\x62\x61\x72\x22\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x05\x68\x65\x6c\x6c\x6f";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Error, data.len, framer.header);

    const frame = try ErrorFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(ErrorCode.AlreadyExists, frame.error_code);
    testing.expectString("Cannot add already existing table \"hello\" to keyspace \"foobar\"", frame.message);
    const already_exists_error = frame.already_exists.?;
    testing.expectString("foobar", already_exists_error.keyspace);
    testing.expectString("hello", already_exists_error.table);
}

test "error frame: syntax error" {
    const data = "\x84\x00\x00\x2f\x00\x00\x00\x00\x41\x00\x00\x20\x00\x00\x3b\x6c\x69\x6e\x65\x20\x32\x3a\x30\x20\x6d\x69\x73\x6d\x61\x74\x63\x68\x65\x64\x20\x69\x6e\x70\x75\x74\x20\x27\x3b\x27\x20\x65\x78\x70\x65\x63\x74\x69\x6e\x67\x20\x4b\x5f\x46\x52\x4f\x4d\x20\x28\x73\x65\x6c\x65\x63\x74\x2a\x5b\x3b\x5d\x29";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Error, data.len, framer.header);

    const frame = try ErrorFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(ErrorCode.SyntaxError, frame.error_code);
    testing.expectString("line 2:0 mismatched input ';' expecting K_FROM (select*[;])", frame.message);
}

test "ready frame" {
    const data = "\x84\x00\x00\x02\x02\x00\x00\x00\x00";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Ready, data.len, framer.header);
}

test "authenticate frame" {
    const data = "\x84\x00\x00\x00\x03\x00\x00\x00\x31\x00\x2f\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x2e\x61\x75\x74\x68\x2e\x50\x61\x73\x73\x77\x6f\x72\x64\x41\x75\x74\x68\x65\x6e\x74\x69\x63\x61\x74\x6f\x72";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Authenticate, data.len, framer.header);

    const frame = try AuthenticateFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectString("org.apache.cassandra.auth.PasswordAuthenticator", frame.authenticator);
}

test "supported frame" {
    const data = "\x84\x00\x00\x09\x06\x00\x00\x00\x60\x00\x03\x00\x11\x50\x52\x4f\x54\x4f\x43\x4f\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x53\x00\x03\x00\x04\x33\x2f\x76\x33\x00\x04\x34\x2f\x76\x34\x00\x09\x35\x2f\x76\x35\x2d\x62\x65\x74\x61\x00\x0b\x43\x4f\x4d\x50\x52\x45\x53\x53\x49\x4f\x4e\x00\x02\x00\x06\x73\x6e\x61\x70\x70\x79\x00\x03\x6c\x7a\x34\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x01\x00\x05\x33\x2e\x34\x2e\x34";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Supported, data.len, framer.header);

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

test "prepare frame" {
    const data = "\x04\x00\x00\xc0\x09\x00\x00\x00\x32\x00\x00\x00\x2e\x53\x45\x4c\x45\x43\x54\x20\x61\x67\x65\x2c\x20\x6e\x61\x6d\x65\x20\x66\x72\x6f\x6d\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x20\x77\x68\x65\x72\x65\x20\x69\x64\x20\x3d\x20\x3f";

    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Prepare, data.len, framer.header);

    const frame = try PrepareFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectString("SELECT age, name from foobar.user where id = ?", frame.query);
    testing.expect(frame.keyspace == null);
}

test "execute frame" {
    const data = "\x04\x00\x01\x00\x0a\x00\x00\x00\x37\x00\x10\x97\x97\x95\x6d\xfe\xb2\x4c\x99\x86\x8e\xd3\x84\xff\x6f\xd9\x4c\x00\x04\x27\x00\x01\x00\x00\x00\x10\xeb\x11\xc9\x1e\xd8\xcc\x48\x4d\xaf\x55\xe9\x9f\x5c\xd9\xec\x4a\x00\x00\x13\x88\x00\x05\xa2\x41\x4c\x1b\x06\x4c";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Execute, data.len, framer.header);

    const frame = try ExecuteFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    const exp_query_id = "\x97\x97\x95\x6d\xfe\xb2\x4c\x99\x86\x8e\xd3\x84\xff\x6f\xd9\x4c";
    testing.expectEqualSlices(u8, exp_query_id, frame.query_id);

    testing.expectEqual(Consistency.Quorum, frame.query_parameters.consistency_level);

    const values = frame.query_parameters.values.?.Normal;
    testing.expectEqual(@as(usize, 1), values.len);
    testing.expectEqualSlices(u8, "\xeb\x11\xc9\x1e\xd8\xcc\x48\x4d\xaf\x55\xe9\x9f\x5c\xd9\xec\x4a", values[0].Set);
    testing.expectEqual(@as(u32, 5000), frame.query_parameters.page_size.?);
    testing.expect(frame.query_parameters.paging_state == null);
    testing.expect(frame.query_parameters.serial_consistency_level == null);
    testing.expectEqual(@as(u64, 1585776216966732), frame.query_parameters.timestamp.?);
    testing.expect(frame.query_parameters.keyspace == null);
    testing.expect(frame.query_parameters.now_in_seconds == null);
}

test "batch frame: query type string" {
    const data = "\x04\x00\x00\xc0\x0d\x00\x00\x00\xcc\x00\x00\x03\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Batch, data.len, framer.header);

    const frame = try BatchFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(BatchType.Logged, frame.batch_type);

    testing.expectEqual(@as(usize, 3), frame.queries.len);
    for (frame.queries) |query| {
        const exp = "INSERT INTO foobar.user(id, name) values(uuid(), 'vincent')";
        testing.expectString(exp, query.query_string.?);
        testing.expect(query.query_id == null);
        testing.expect(query.values == .Normal);
        testing.expectEqual(@as(usize, 0), query.values.Normal.len);
    }

    testing.expectEqual(Consistency.Any, frame.consistency_level);
    testing.expect(frame.serial_consistency_level == null);
    testing.expect(frame.timestamp == null);
    testing.expect(frame.keyspace == null);
    testing.expect(frame.now_in_seconds == null);
}

test "batch frame: query type prepared" {
    const data = "\x04\x00\x01\x00\x0d\x00\x00\x00\xa2\x00\x00\x03\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\x3a\x9a\xab\x41\x68\x24\x4a\xef\x9d\xf5\x72\xc7\x84\xab\xa2\x57\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\xed\x54\xb0\x6d\xcc\xb2\x43\x51\x96\x51\x74\x5e\xee\xae\xd2\xfe\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\x79\xdf\x8a\x28\x5a\x60\x47\x19\x9b\x42\x84\xea\x69\x10\x1a\xe6\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x00\x00\x00";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Batch, data.len, framer.header);

    const frame = try BatchFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(BatchType.Logged, frame.batch_type);

    const expUUIDs = &[_][]const u8{
        "\x3a\x9a\xab\x41\x68\x24\x4a\xef\x9d\xf5\x72\xc7\x84\xab\xa2\x57",
        "\xed\x54\xb0\x6d\xcc\xb2\x43\x51\x96\x51\x74\x5e\xee\xae\xd2\xfe",
        "\x79\xdf\x8a\x28\x5a\x60\x47\x19\x9b\x42\x84\xea\x69\x10\x1a\xe6",
    };

    testing.expectEqual(@as(usize, 3), frame.queries.len);
    var i: usize = 0;
    for (frame.queries) |query| {
        testing.expect(query.query_string == null);
        const exp_query_id = "\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65";
        testing.expectEqualSlices(u8, exp_query_id, query.query_id.?);

        testing.expect(query.values == .Normal);
        testing.expectEqual(@as(usize, 2), query.values.Normal.len);

        const value1 = query.values.Normal[0];
        testing.expectEqualSlices(u8, expUUIDs[i], value1.Set);

        const value2 = query.values.Normal[1];
        testing.expectString("Vincent", value2.Set);

        i += 1;
    }

    testing.expectEqual(Consistency.Any, frame.consistency_level);
    testing.expect(frame.serial_consistency_level == null);
    testing.expect(frame.timestamp == null);
    testing.expect(frame.keyspace == null);
    testing.expect(frame.now_in_seconds == null);
}

test "register frame" {
    const data = "\x04\x00\x00\xc0\x0b\x00\x00\x00\x31\x00\x03\x00\x0f\x54\x4f\x50\x4f\x4c\x4f\x47\x59\x5f\x43\x48\x41\x4e\x47\x45\x00\x0d\x53\x54\x41\x54\x55\x53\x5f\x43\x48\x41\x4e\x47\x45\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Register, data.len, framer.header);

    const frame = try RegisterFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(@as(usize, 3), frame.event_types.len);
    testing.expectString("TOPOLOGY_CHANGE", frame.event_types[0]);
    testing.expectString("STATUS_CHANGE", frame.event_types[1]);
    testing.expectString("SCHEMA_CHANGE", frame.event_types[2]);
}

test "event frame: topology change" {
    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x24\x00\x0f\x54\x4f\x50\x4f\x4c\x4f\x47\x59\x5f\x43\x48\x41\x4e\x47\x45\x00\x08\x4e\x45\x57\x5f\x4e\x4f\x44\x45\x04\x7f\x00\x00\x04\x00\x00\x23\x52";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Event, data.len, framer.header);

    const frame = try EventFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expect(frame.event == .TOPOLOGY_CHANGE);

    const topology_change = frame.event.TOPOLOGY_CHANGE;
    testing.expectEqual(TopologyChangeType.NEW_NODE, topology_change.change_type);

    const localhost = net.Address.initIp4([4]u8{ 0x7f, 0x00, 0x00, 0x04 }, 9042);
    testing.expect(net.Address.eql(localhost, topology_change.node_address));
}

test "event frame: status change" {
    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x1e\x00\x0d\x53\x54\x41\x54\x55\x53\x5f\x43\x48\x41\x4e\x47\x45\x00\x04\x44\x4f\x57\x4e\x04\x7f\x00\x00\x01\x00\x00\x23\x52";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Event, data.len, framer.header);

    const frame = try EventFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expect(frame.event == .STATUS_CHANGE);

    const status_change = frame.event.STATUS_CHANGE;
    testing.expectEqual(StatusChangeType.DOWN, status_change.change_type);

    const localhost = net.Address.initIp4([4]u8{ 0x7f, 0x00, 0x00, 0x01 }, 9042);
    testing.expect(net.Address.eql(localhost, status_change.node_address));
}

test "event frame: schema change" {
    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x2a\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45\x00\x07\x43\x52\x45\x41\x54\x45\x44\x00\x08\x4b\x45\x59\x53\x50\x41\x43\x45\x00\x06\x62\x61\x72\x62\x61\x7a";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Event, data.len, framer.header);

    const frame = try EventFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expect(frame.event == .SCHEMA_CHANGE);

    const schema_change = frame.event.SCHEMA_CHANGE;
    testing.expectEqual(SchemaChangeType.CREATED, schema_change.change_type);
}

test "auth challenge frame" {
    // TODO(vincent): how do I get one of these frame ?
}
