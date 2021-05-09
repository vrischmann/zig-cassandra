const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;

usingnamespace @import("../frame.zig");
usingnamespace @import("../primitive_types.zig");
usingnamespace @import("../error.zig");
const testing = @import("../testing.zig");

/// ERROR is sent by a node if there's an error processing a request.
///
/// Described in the protocol spec at §4.2.1.
pub const ErrorFrame = struct {
    const Self = @This();

    // TODO(vincent): extract this for use by the client

    error_code: ErrorCode,
    message: []const u8,

    unavailable_replicas: ?UnavailableReplicasError,
    function_failure: ?FunctionFailureError,
    write_timeout: ?WriteError.Timeout,
    read_timeout: ?ReadError.Timeout,
    write_failure: ?WriteError.Failure,
    read_failure: ?ReadError.Failure,
    cas_write_unknown: ?WriteError.CASUnknown,
    already_exists: ?AlreadyExistsError,
    unprepared: ?UnpreparedError,

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !Self {
        var frame = Self{
            .error_code = undefined,
            .message = undefined,
            .unavailable_replicas = null,
            .function_failure = null,
            .write_timeout = null,
            .read_timeout = null,
            .write_failure = null,
            .read_failure = null,
            .cas_write_unknown = null,
            .already_exists = null,
            .unprepared = null,
        };

        frame.error_code = @intToEnum(ErrorCode, try pr.readInt(u32));
        frame.message = try pr.readString(allocator);

        switch (frame.error_code) {
            .UnavailableReplicas => {
                frame.unavailable_replicas = UnavailableReplicasError{
                    .consistency_level = try pr.readConsistency(),
                    .required = try pr.readInt(u32),
                    .alive = try pr.readInt(u32),
                };
            },
            .FunctionFailure => {
                frame.function_failure = FunctionFailureError{
                    .keyspace = try pr.readString(allocator),
                    .function = try pr.readString(allocator),
                    .arg_types = try pr.readStringList(allocator),
                };
            },
            .WriteTimeout => {
                var write_timeout = WriteError.Timeout{
                    .consistency_level = try pr.readConsistency(),
                    .received = try pr.readInt(u32),
                    .block_for = try pr.readInt(u32),
                    .write_type = undefined,
                    .contentions = null,
                };

                const write_type_string = try pr.readString(allocator);
                defer allocator.free(write_type_string);

                write_timeout.write_type = meta.stringToEnum(WriteError.WriteType, write_type_string) orelse return error.InvalidWriteType;
                if (write_timeout.write_type == .CAS) {
                    write_timeout.contentions = try pr.readInt(u16);
                }

                frame.write_timeout = write_timeout;
            },
            .ReadTimeout => {
                frame.read_timeout = ReadError.Timeout{
                    .consistency_level = try pr.readConsistency(),
                    .received = try pr.readInt(u32),
                    .block_for = try pr.readInt(u32),
                    .data_present = try pr.readByte(),
                };
            },
            .WriteFailure => {
                var write_failure = WriteError.Failure{
                    .consistency_level = try pr.readConsistency(),
                    .received = try pr.readInt(u32),
                    .block_for = try pr.readInt(u32),
                    .reason_map = undefined,
                    .write_type = undefined,
                };

                // Read reason map
                // TODO(vincent): this is only correct for Protocol v5
                // See point 10. in the the protocol v5 spec.

                var reason_map = std.ArrayList(WriteError.Failure.Reason).init(allocator);
                errdefer reason_map.deinit();

                const n = try pr.readInt(u32);
                var i: usize = 0;
                while (i < n) : (i += 1) {
                    const reason = WriteError.Failure.Reason{
                        .endpoint = try pr.readInetaddr(),
                        .failure_code = try pr.readInt(u16),
                    };
                    _ = try reason_map.append(reason);
                }
                write_failure.reason_map = reason_map.toOwnedSlice();

                // Read the rest

                const write_type_string = try pr.readString(allocator);
                defer allocator.free(write_type_string);

                write_failure.write_type = meta.stringToEnum(WriteError.WriteType, write_type_string) orelse return error.InvalidWriteType;

                frame.write_failure = write_failure;
            },
            .ReadFailure => {
                var read_failure = ReadError.Failure{
                    .consistency_level = try pr.readConsistency(),
                    .received = try pr.readInt(u32),
                    .block_for = try pr.readInt(u32),
                    .reason_map = undefined,
                    .data_present = undefined,
                };

                // Read reason map

                var reason_map = std.ArrayList(ReadError.Failure.Reason).init(allocator);
                errdefer reason_map.deinit();

                const n = try pr.readInt(u32);
                var i: usize = 0;
                while (i < n) : (i += 1) {
                    const reason = ReadError.Failure.Reason{
                        .endpoint = try pr.readInetaddr(),
                        .failure_code = try pr.readInt(u16),
                    };
                    _ = try reason_map.append(reason);
                }
                read_failure.reason_map = reason_map.toOwnedSlice();

                // Read the rest

                read_failure.data_present = try pr.readByte();

                frame.read_failure = read_failure;
            },
            .CASWriteUnknown => {
                frame.cas_write_unknown = WriteError.CASUnknown{
                    .consistency_level = try pr.readConsistency(),
                    .received = try pr.readInt(u32),
                    .block_for = try pr.readInt(u32),
                };
            },
            .AlreadyExists => {
                frame.already_exists = AlreadyExistsError{
                    .keyspace = try pr.readString(allocator),
                    .table = try pr.readString(allocator),
                };
            },
            .Unprepared => {
                if (try pr.readShortBytes(allocator)) |statement_id| {
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
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x02\x00\x00\x00\x00\x5e\x00\x00\x22\x00\x00\x58\x4e\x6f\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x20\x68\x61\x73\x20\x62\x65\x65\x6e\x20\x73\x70\x65\x63\x69\x66\x69\x65\x64\x2e\x20\x55\x53\x45\x20\x61\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x2c\x20\x6f\x72\x20\x65\x78\x70\x6c\x69\x63\x69\x74\x6c\x79\x20\x73\x70\x65\x63\x69\x66\x79\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x2e\x74\x61\x62\x6c\x65\x6e\x61\x6d\x65";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    try checkHeader(Opcode.Error, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ErrorFrame.read(&arena.allocator, &pr);

    try testing.expectEqual(ErrorCode.InvalidQuery, frame.error_code);
    try testing.expectEqualStrings("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename", frame.message);
}

test "error frame: already exists" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x23\x00\x00\x00\x00\x53\x00\x00\x24\x00\x00\x3e\x43\x61\x6e\x6e\x6f\x74\x20\x61\x64\x64\x20\x61\x6c\x72\x65\x61\x64\x79\x20\x65\x78\x69\x73\x74\x69\x6e\x67\x20\x74\x61\x62\x6c\x65\x20\x22\x68\x65\x6c\x6c\x6f\x22\x20\x74\x6f\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x20\x22\x66\x6f\x6f\x62\x61\x72\x22\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x05\x68\x65\x6c\x6c\x6f";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    try checkHeader(Opcode.Error, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ErrorFrame.read(&arena.allocator, &pr);

    try testing.expectEqual(ErrorCode.AlreadyExists, frame.error_code);
    try testing.expectEqualStrings("Cannot add already existing table \"hello\" to keyspace \"foobar\"", frame.message);
    const already_exists_error = frame.already_exists.?;
    try testing.expectEqualStrings("foobar", already_exists_error.keyspace);
    try testing.expectEqualStrings("hello", already_exists_error.table);
}

test "error frame: syntax error" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x2f\x00\x00\x00\x00\x41\x00\x00\x20\x00\x00\x3b\x6c\x69\x6e\x65\x20\x32\x3a\x30\x20\x6d\x69\x73\x6d\x61\x74\x63\x68\x65\x64\x20\x69\x6e\x70\x75\x74\x20\x27\x3b\x27\x20\x65\x78\x70\x65\x63\x74\x69\x6e\x67\x20\x4b\x5f\x46\x52\x4f\x4d\x20\x28\x73\x65\x6c\x65\x63\x74\x2a\x5b\x3b\x5d\x29";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    try checkHeader(Opcode.Error, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ErrorFrame.read(&arena.allocator, &pr);

    try testing.expectEqual(ErrorCode.SyntaxError, frame.error_code);
    try testing.expectEqualStrings("line 2:0 mismatched input ';' expecting K_FROM (select*[;])", frame.message);
}
