const std = @import("std");
const net = std.net;

usingnamespace @import("primitive_types.zig");

// TODO(vincent): test all error codes
pub const ErrorCode = packed enum(u32) {
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
