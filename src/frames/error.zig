const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

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
    testing.expectEqualString("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename", frame.message);
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
    testing.expectEqualString("Cannot add already existing table \"hello\" to keyspace \"foobar\"", frame.message);
    const already_exists_error = frame.already_exists.?;
    testing.expectEqualString("foobar", already_exists_error.keyspace);
    testing.expectEqualString("hello", already_exists_error.table);
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
    testing.expectEqualString("line 2:0 mismatched input ';' expecting K_FROM (select*[;])", frame.message);
}
