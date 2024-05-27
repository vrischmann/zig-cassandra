const std = @import("std");
const heap = std.heap;
const io = std.io;
const mem = std.mem;
const net = std.net;
const os = std.os;
const testing = std.testing;

const message = @import("message.zig");
const CompressionAlgorithm = message.CompressionAlgorithm;
const Opcode = message.Opcode;
const Consistency = message.Consistency;
const NotSet = message.NotSet;
const OptionID = message.OptionID;
const ProtocolVersion = message.ProtocolVersion;
const Value = message.Value;
const Values = message.Values;
const PrimitiveReader = message.PrimitiveReader;
const PrimitiveWriter = message.PrimitiveWriter;
const CQLVersion = message.CQLVersion;
const BatchType = message.BatchType;

const ErrorCode = message.ErrorCode;
const UnavailableReplicasError = message.UnavailableReplicasError;
const FunctionFailureError = message.FunctionFailureError;
const WriteError = message.WriteError;
const ReadError = message.ReadError;
const AlreadyExistsError = message.AlreadyExistsError;
const UnpreparedError = message.UnpreparedError;

const event = @import("event.zig");
const metadata = @import("metadata.zig");
const QueryParameters = @import("QueryParameters.zig");

const testutils = @import("testutils.zig");

pub const FrameFlags = struct {
    pub const Compression: u8 = 0x01;
    pub const Tracing: u8 = 0x02;
    pub const CustomPayload: u8 = 0x04;
    pub const Warning: u8 = 0x08;
    pub const UseBeta: u8 = 0x10;
};

pub const FrameHeader = packed struct {
    version: ProtocolVersion,
    flags: u8,
    stream: i16,
    opcode: Opcode,
    body_len: u32,

    const Size = 9;

    pub fn init(comptime ReaderType: type, in: ReaderType) !FrameHeader {
        var buf: [Size]u8 = undefined;

        const read = try in.readAll(&buf);
        if (read != Size) {
            return error.UnexpectedEOF;
        }

        return FrameHeader{
            .version = try ProtocolVersion.init(buf[0]),
            .flags = buf[1],
            .stream = mem.readInt(i16, @ptrCast(buf[2..4]), .big),
            .opcode = @enumFromInt(buf[4]),
            .body_len = mem.readInt(u32, @ptrCast(buf[5..9]), .big),
        };
    }
};

pub const RawFrame = struct {
    header: FrameHeader,
    body: []const u8,

    pub fn deinit(self: @This(), allocator: mem.Allocator) void {
        allocator.free(self.body);
    }
};

pub fn RawFrameReader(comptime ReaderType: type) type {
    return struct {
        const Self = @This();

        reader: ReaderType,

        pub fn init(in: ReaderType) Self {
            return Self{
                .reader = in,
            };
        }

        pub fn read(self: *Self, allocator: mem.Allocator) !RawFrame {
            var buf: [FrameHeader.Size]u8 = undefined;

            const n_header_read = try self.reader.readAll(&buf);
            if (n_header_read != FrameHeader.Size) {
                return error.UnexpectedEOF;
            }

            const header = FrameHeader{
                .version = ProtocolVersion{ .version = buf[0] },
                .flags = buf[1],
                .stream = mem.readInt(i16, @ptrCast(buf[2..4]), .big),
                .opcode = @enumFromInt(buf[4]),
                .body_len = mem.readInt(u32, @ptrCast(buf[5..9]), .big),
            };

            const len = @as(usize, header.body_len);

            const body = try allocator.alloc(u8, len);
            const n_read = try self.reader.readAll(body);
            if (n_read != len) {
                return error.UnexpectedEOF;
            }

            return RawFrame{
                .header = header,
                .body = body,
            };
        }
    };
}

pub fn RawFrameWriter(comptime WriterType: type) type {
    return struct {
        const Self = @This();

        writer: WriterType,

        pub fn init(out: WriterType) Self {
            return Self{
                .writer = out,
            };
        }

        pub fn write(self: *Self, raw_frame: RawFrame) !void {
            var buf: [FrameHeader.Size]u8 = undefined;

            buf[0] = raw_frame.header.version.version;
            buf[1] = raw_frame.header.flags;
            mem.writeInt(i16, @ptrCast(buf[2..4]), raw_frame.header.stream, .big);
            buf[4] = @intFromEnum(raw_frame.header.opcode);
            mem.writeInt(u32, @ptrCast(buf[5..9]), raw_frame.header.body_len, .big);

            try self.writer.writeAll(&buf);
            try self.writer.writeAll(raw_frame.body);
        }
    };
}

// TODO(vincent): do we want to keep these wrapper types ?

/// ColumnData is a wrapper around a slice of bytes.
pub const ColumnData = struct {
    slice: []const u8,
};

/// RowData is a wrapper around a slice of ColumnData.
pub const RowData = struct {
    slice: []const ColumnData,
};

pub fn checkHeader(opcode: Opcode, data_len: usize, header: FrameHeader) !void {
    // We can only use v4 for now
    try testing.expect(header.version.is(4));
    // Don't care about the flags here
    // Don't care about the stream
    try testing.expectEqual(opcode, header.opcode);
    try testing.expectEqual(@as(usize, header.body_len), data_len - FrameHeader.Size);
}

test "frame header: read and write" {
    const exp = "\x04\x00\x00\xd7\x05\x00\x00\x00\x00";
    var fbs = io.fixedBufferStream(exp);

    // deserialize the header

    const reader = fbs.reader();

    const header = try FrameHeader.init(@TypeOf(reader), fbs.reader());
    try testing.expect(header.version.is(4));
    try testing.expect(header.version.isRequest());
    try testing.expectEqual(@as(u8, 0), header.flags);
    try testing.expectEqual(@as(i16, 215), header.stream);
    try testing.expectEqual(Opcode.Options, header.opcode);
    try testing.expectEqual(@as(u32, 0), header.body_len);
    try testing.expectEqual(@as(usize, 0), exp.len - FrameHeader.Size);
}

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

    pub fn read(allocator: mem.Allocator, pr: *PrimitiveReader) !Self {
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

        frame.error_code = @enumFromInt(try pr.readInt(u32));
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

                write_timeout.write_type = std.meta.stringToEnum(WriteError.WriteType, write_type_string) orelse return error.InvalidWriteType;
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
                write_failure.reason_map = try reason_map.toOwnedSlice();

                // Read the rest

                const write_type_string = try pr.readString(allocator);
                defer allocator.free(write_type_string);

                write_failure.write_type = std.meta.stringToEnum(WriteError.WriteType, write_type_string) orelse return error.InvalidWriteType;

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
                read_failure.reason_map = try reason_map.toOwnedSlice();

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
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x02\x00\x00\x00\x00\x5e\x00\x00\x22\x00\x00\x58\x4e\x6f\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x20\x68\x61\x73\x20\x62\x65\x65\x6e\x20\x73\x70\x65\x63\x69\x66\x69\x65\x64\x2e\x20\x55\x53\x45\x20\x61\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x2c\x20\x6f\x72\x20\x65\x78\x70\x6c\x69\x63\x69\x74\x6c\x79\x20\x73\x70\x65\x63\x69\x66\x79\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x2e\x74\x61\x62\x6c\x65\x6e\x61\x6d\x65";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Error, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ErrorFrame.read(arena.allocator(), &pr);

    try testing.expectEqual(ErrorCode.InvalidQuery, frame.error_code);
    try testing.expectEqualStrings("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename", frame.message);
}

test "error frame: already exists" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x23\x00\x00\x00\x00\x53\x00\x00\x24\x00\x00\x3e\x43\x61\x6e\x6e\x6f\x74\x20\x61\x64\x64\x20\x61\x6c\x72\x65\x61\x64\x79\x20\x65\x78\x69\x73\x74\x69\x6e\x67\x20\x74\x61\x62\x6c\x65\x20\x22\x68\x65\x6c\x6c\x6f\x22\x20\x74\x6f\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x20\x22\x66\x6f\x6f\x62\x61\x72\x22\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x05\x68\x65\x6c\x6c\x6f";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Error, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ErrorFrame.read(arena.allocator(), &pr);

    try testing.expectEqual(ErrorCode.AlreadyExists, frame.error_code);
    try testing.expectEqualStrings("Cannot add already existing table \"hello\" to keyspace \"foobar\"", frame.message);
    const already_exists_error = frame.already_exists.?;
    try testing.expectEqualStrings("foobar", already_exists_error.keyspace);
    try testing.expectEqualStrings("hello", already_exists_error.table);
}

test "error frame: syntax error" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x2f\x00\x00\x00\x00\x41\x00\x00\x20\x00\x00\x3b\x6c\x69\x6e\x65\x20\x32\x3a\x30\x20\x6d\x69\x73\x6d\x61\x74\x63\x68\x65\x64\x20\x69\x6e\x70\x75\x74\x20\x27\x3b\x27\x20\x65\x78\x70\x65\x63\x74\x69\x6e\x67\x20\x4b\x5f\x46\x52\x4f\x4d\x20\x28\x73\x65\x6c\x65\x63\x74\x2a\x5b\x3b\x5d\x29";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Error, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ErrorFrame.read(arena.allocator(), &pr);

    try testing.expectEqual(ErrorCode.SyntaxError, frame.error_code);
    try testing.expectEqualStrings("line 2:0 mismatched input ';' expecting K_FROM (select*[;])", frame.message);
}

/// OPTIONS is sent to a node to ask which STARTUP options are supported.
///
/// Described in the protocol spec at §4.1.3.
pub const OptionsFrame = struct {};

test "options frame" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x04\x00\x00\x05\x05\x00\x00\x00\x00";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Options, data.len, raw_frame.header);
}

/// STARTUP is sent to a node to initialize a connection.
///
/// Described in the protocol spec at §4.1.1.
pub const StartupFrame = struct {
    const Self = @This();

    cql_version: CQLVersion,
    compression: ?CompressionAlgorithm,

    pub fn write(self: Self, pw: *PrimitiveWriter) !void {
        var buf: [16]u8 = undefined;
        const cql_version = try self.cql_version.print(&buf);

        if (self.compression) |c| {
            // Always 2 keys
            _ = try pw.startStringMap(2);

            _ = try pw.writeString("CQL_VERSION");
            _ = try pw.writeString(cql_version);

            _ = try pw.writeString("COMPRESSION");
            switch (c) {
                .LZ4 => _ = try pw.writeString("lz4"),
                .Snappy => _ = try pw.writeString("snappy"),
            }
        } else {
            // Always 1 key
            _ = try pw.startStringMap(1);
            _ = try pw.writeString("CQL_VERSION");
            _ = try pw.writeString(cql_version);
        }
    }

    pub fn read(allocator: mem.Allocator, pr: *PrimitiveReader) !Self {
        var frame = Self{
            .cql_version = undefined,
            .compression = null,
        };

        const map = try pr.readStringMap(allocator);

        // CQL_VERSION is mandatory and the only version supported is 3.0.0 right now.
        if (map.getEntry("CQL_VERSION")) |entry| {
            if (!mem.eql(u8, "3.0.0", entry.value_ptr.*)) {
                return error.InvalidCQLVersion;
            }
            frame.cql_version = try CQLVersion.fromString(entry.value_ptr.*);
        } else {
            return error.InvalidCQLVersion;
        }

        if (map.getEntry("COMPRESSION")) |entry| {
            if (mem.eql(u8, entry.value_ptr.*, "lz4")) {
                frame.compression = CompressionAlgorithm.LZ4;
            } else if (mem.eql(u8, entry.value_ptr.*, "snappy")) {
                frame.compression = CompressionAlgorithm.Snappy;
            } else {
                return error.InvalidCompression;
            }
        }

        return frame;
    }
};

test "startup frame" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x05\x33\x2e\x30\x2e\x30";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), exp);

    try checkHeader(Opcode.Startup, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try StartupFrame.read(arena.allocator(), &pr);

    try testing.expectEqual(CQLVersion{ .major = 3, .minor = 0, .patch = 0 }, frame.cql_version);
    try testing.expect(frame.compression == null);

    // write

    try testutils.expectSameRawFrame(StartupFrame, frame, raw_frame.header, exp);
}

/// EXECUTE is sent to execute a prepared query.
///
/// Described in the protocol spec at §4.1.6
pub const ExecuteFrame = struct {
    const Self = @This();

    query_id: []const u8,
    result_metadata_id: ?[]const u8,
    query_parameters: QueryParameters,

    pub fn write(self: Self, protocol_version: ProtocolVersion, pw: *PrimitiveWriter) !void {
        _ = try pw.writeShortBytes(self.query_id);
        if (protocol_version.is(5)) {
            if (self.result_metadata_id) |id| {
                _ = try pw.writeShortBytes(id);
            }
        }
        _ = try self.query_parameters.write(protocol_version, pw);
    }

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        var frame = Self{
            .query_id = undefined,
            .result_metadata_id = null,
            .query_parameters = undefined,
        };

        frame.query_id = (try pr.readShortBytes(allocator)) orelse &[_]u8{};
        if (protocol_version.is(5)) {
            frame.result_metadata_id = try pr.readShortBytes(allocator);
        }
        frame.query_parameters = try QueryParameters.read(allocator, protocol_version, pr);

        return frame;
    }
};

test "execute frame" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x01\x00\x0a\x00\x00\x00\x37\x00\x10\x97\x97\x95\x6d\xfe\xb2\x4c\x99\x86\x8e\xd3\x84\xff\x6f\xd9\x4c\x00\x04\x27\x00\x01\x00\x00\x00\x10\xeb\x11\xc9\x1e\xd8\xcc\x48\x4d\xaf\x55\xe9\x9f\x5c\xd9\xec\x4a\x00\x00\x13\x88\x00\x05\xa2\x41\x4c\x1b\x06\x4c";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), exp);

    try checkHeader(Opcode.Execute, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ExecuteFrame.read(arena.allocator(), raw_frame.header.version, &pr);

    const exp_query_id = "\x97\x97\x95\x6d\xfe\xb2\x4c\x99\x86\x8e\xd3\x84\xff\x6f\xd9\x4c";
    try testing.expectEqualSlices(u8, exp_query_id, frame.query_id);

    try testing.expectEqual(Consistency.Quorum, frame.query_parameters.consistency_level);

    const values = frame.query_parameters.values.?.Normal;
    try testing.expectEqual(@as(usize, 1), values.len);
    try testing.expectEqualSlices(u8, "\xeb\x11\xc9\x1e\xd8\xcc\x48\x4d\xaf\x55\xe9\x9f\x5c\xd9\xec\x4a", values[0].Set);
    try testing.expectEqual(@as(u32, 5000), frame.query_parameters.page_size.?);
    try testing.expect(frame.query_parameters.paging_state == null);
    try testing.expect(frame.query_parameters.serial_consistency_level == null);
    try testing.expectEqual(@as(u64, 1585776216966732), frame.query_parameters.timestamp.?);
    try testing.expect(frame.query_parameters.keyspace == null);
    try testing.expect(frame.query_parameters.now_in_seconds == null);

    // write

    try testutils.expectSameRawFrame(ExecuteFrame, frame, raw_frame.header, exp);
}

/// AUTHENTICATE is sent by a node in response to a STARTUP frame if authentication is required.
///
/// Described in the protocol spec at §4.2.3.
pub const AuthenticateFrame = struct {
    authenticator: []const u8,

    pub fn read(allocator: mem.Allocator, pr: *PrimitiveReader) !AuthenticateFrame {
        return AuthenticateFrame{
            .authenticator = try pr.readString(allocator),
        };
    }
};

/// AUTH_RESPONSE is sent to a node to answser a authentication challenge.
///
/// Described in the protocol spec at §4.1.2.
pub const AuthResponseFrame = struct {
    token: ?[]const u8,

    pub fn write(self: @This(), pw: *PrimitiveWriter) !void {
        return pw.writeBytes(self.token);
    }

    pub fn read(allocator: mem.Allocator, pr: *PrimitiveReader) !AuthResponseFrame {
        return AuthResponseFrame{
            .token = try pr.readBytes(allocator),
        };
    }
};

/// AUTH_CHALLENGE is a server authentication challenge.
///
/// Described in the protocol spec at §4.2.7.
pub const AuthChallengeFrame = struct {
    token: ?[]const u8,

    pub fn read(allocator: mem.Allocator, pr: *PrimitiveReader) !AuthChallengeFrame {
        return AuthChallengeFrame{
            .token = try pr.readBytes(allocator),
        };
    }
};

/// AUTH_SUCCESS indicates the success of the authentication phase.
///
/// Described in the protocol spec at §4.2.8.
pub const AuthSuccessFrame = struct {
    token: ?[]const u8,

    pub fn read(allocator: mem.Allocator, pr: *PrimitiveReader) !AuthSuccessFrame {
        return AuthSuccessFrame{
            .token = try pr.readBytes(allocator),
        };
    }
};

test "authenticate frame" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x84\x00\x00\x00\x03\x00\x00\x00\x31\x00\x2f\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x2e\x61\x75\x74\x68\x2e\x50\x61\x73\x73\x77\x6f\x72\x64\x41\x75\x74\x68\x65\x6e\x74\x69\x63\x61\x74\x6f\x72";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), exp);

    try checkHeader(Opcode.Authenticate, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try AuthenticateFrame.read(arena.allocator(), &pr);

    try testing.expectEqualStrings("org.apache.cassandra.auth.PasswordAuthenticator", frame.authenticator);
}

test "auth challenge frame" {
    // TODO(vincent): how do I get one of these frame ?
}

test "auth response frame" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\x02\x0f\x00\x00\x00\x18\x00\x00\x00\x14\x00\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x00\x63\x61\x73\x73\x61\x6e\x64\x72\x61";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), exp);

    try checkHeader(Opcode.AuthResponse, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try AuthResponseFrame.read(arena.allocator(), &pr);

    const exp_token = "\x00cassandra\x00cassandra";
    try testing.expectEqualSlices(u8, exp_token, frame.token.?);

    // write

    try testutils.expectSameRawFrame(AuthResponseFrame, frame, raw_frame.header, exp);
}

test "auth success frame" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x84\x00\x00\x02\x10\x00\x00\x00\x04\xff\xff\xff\xff";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), exp);

    try checkHeader(Opcode.AuthSuccess, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try AuthSuccessFrame.read(arena.allocator(), &pr);

    try testing.expect(frame.token == null);
}

///
///
/// Structure of a query in a BATCH frame
const BatchQuery = struct {
    const Self = @This();

    query_string: ?[]const u8,
    query_id: ?[]const u8,

    values: Values,

    pub fn write(self: Self, pw: *PrimitiveWriter) !void {
        if (self.query_string) |query_string| {
            _ = try pw.writeByte(0);
            _ = try pw.writeLongString(query_string);
        } else if (self.query_id) |query_id| {
            _ = try pw.writeByte(1);
            _ = try pw.writeShortBytes(query_id);
        }

        switch (self.values) {
            .Normal => |normal_values| {
                _ = try pw.writeInt(u16, @intCast(normal_values.len));
                for (normal_values) |value| {
                    _ = try pw.writeValue(value);
                }
            },
            .Named => |named_values| {
                _ = try pw.writeInt(u16, @intCast(named_values.len));
                for (named_values) |v| {
                    _ = try pw.writeString(v.name);
                    _ = try pw.writeValue(v.value);
                }
            },
        }
    }

    pub fn read(allocator: mem.Allocator, pr: *PrimitiveReader) !BatchQuery {
        var query = Self{
            .query_string = null,
            .query_id = null,
            .values = undefined,
        };

        const kind = try pr.readByte();
        switch (kind) {
            0 => query.query_string = try pr.readLongString(allocator),
            1 => query.query_id = try pr.readShortBytes(allocator),
            else => return error.InvalidQueryKind,
        }

        var list = std.ArrayList(Value).init(allocator);
        errdefer list.deinit();

        const n_values = try pr.readInt(u16);
        var j: usize = 0;
        while (j < @as(usize, n_values)) : (j += 1) {
            const value = try pr.readValue(allocator);
            _ = try list.append(value);
        }

        query.values = Values{ .Normal = try list.toOwnedSlice() };

        return query;
    }
};

/// BATCH is sent to execute a list of queries (prepared or not) as a batch.
///
/// Described in the protocol spec at §4.1.7
pub const BatchFrame = struct {
    const Self = @This();

    batch_type: BatchType,
    queries: []BatchQuery,
    consistency_level: Consistency,
    serial_consistency_level: ?Consistency,
    timestamp: ?u64,
    keyspace: ?[]const u8,
    now_in_seconds: ?u32,

    const FlagWithSerialConsistency: u32 = 0x0010;
    const FlagWithDefaultTimestamp: u32 = 0x0020;
    const FlagWithNamedValues: u32 = 0x0040; // NOTE(vincent): the spec says this is broken so it's not implemented
    const FlagWithKeyspace: u32 = 0x0080;
    const FlagWithNowInSeconds: u32 = 0x100;

    pub fn write(self: Self, protocol_version: ProtocolVersion, pw: *PrimitiveWriter) !void {
        _ = try pw.writeInt(u8, @intCast(@intFromEnum(self.batch_type)));

        // Write the queries

        _ = try pw.writeInt(u16, @intCast(self.queries.len));
        for (self.queries) |query| {
            _ = try query.write(pw);
        }

        // Write the consistency

        _ = try pw.writeConsistency(self.consistency_level);

        // Build the flags value

        var flags: u32 = 0;
        if (self.serial_consistency_level != null) {
            flags |= FlagWithSerialConsistency;
        }
        if (self.timestamp != null) {
            flags |= FlagWithDefaultTimestamp;
        }
        if (protocol_version.is(5)) {
            if (self.keyspace != null) {
                flags |= FlagWithKeyspace;
            }
            if (self.now_in_seconds != null) {
                flags |= FlagWithNowInSeconds;
            }
        }

        if (protocol_version.is(5)) {
            _ = try pw.writeInt(u32, flags);
        } else {
            _ = try pw.writeInt(u8, @intCast(flags));
        }

        // Write the remaining body
    }

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        var frame = Self{
            .batch_type = undefined,
            .queries = undefined,
            .consistency_level = undefined,
            .serial_consistency_level = null,
            .timestamp = null,
            .keyspace = null,
            .now_in_seconds = null,
        };

        frame.batch_type = @enumFromInt(try pr.readByte());
        frame.queries = &[_]BatchQuery{};

        // Read all queries in the batch

        var queries = std.ArrayList(BatchQuery).init(allocator);
        errdefer queries.deinit();

        const n = try pr.readInt(u16);
        var i: usize = 0;
        while (i < @as(usize, n)) : (i += 1) {
            const query = try BatchQuery.read(allocator, pr);
            _ = try queries.append(query);
        }

        frame.queries = try queries.toOwnedSlice();

        // Read the rest of the frame

        frame.consistency_level = try pr.readConsistency();

        // The size of the flags bitmask depends on the protocol version.
        var flags: u32 = 0;
        if (protocol_version.is(5)) {
            flags = try pr.readInt(u32);
        } else {
            flags = try pr.readInt(u8);
        }

        if (flags & FlagWithSerialConsistency == FlagWithSerialConsistency) {
            const consistency_level = try pr.readConsistency();
            if (consistency_level != .Serial and consistency_level != .LocalSerial) {
                return error.InvalidSerialConsistency;
            }
            frame.serial_consistency_level = consistency_level;
        }
        if (flags & FlagWithDefaultTimestamp == FlagWithDefaultTimestamp) {
            const timestamp = try pr.readInt(u64);
            if (timestamp < 0) {
                return error.InvalidNegativeTimestamp;
            }
            frame.timestamp = timestamp;
        }

        if (!protocol_version.is(5)) {
            return frame;
        }

        // The following flags are only valid with protocol v5
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            frame.keyspace = try pr.readString(allocator);
        }
        if (flags & FlagWithNowInSeconds == FlagWithNowInSeconds) {
            frame.now_in_seconds = try pr.readInt(u32);
        }

        return frame;
    }
};

test "batch frame: query type string" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\xc0\x0d\x00\x00\x00\xcc\x00\x00\x03\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), exp);

    try checkHeader(Opcode.Batch, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try BatchFrame.read(arena.allocator(), raw_frame.header.version, &pr);

    try testing.expectEqual(BatchType.Logged, frame.batch_type);

    try testing.expectEqual(@as(usize, 3), frame.queries.len);
    for (frame.queries) |query| {
        const exp_query = "INSERT INTO foobar.user(id, name) values(uuid(), 'vincent')";
        try testing.expectEqualStrings(exp_query, query.query_string.?);
        try testing.expect(query.query_id == null);
        try testing.expect(query.values == .Normal);
        try testing.expectEqual(@as(usize, 0), query.values.Normal.len);
    }

    try testing.expectEqual(Consistency.Any, frame.consistency_level);
    try testing.expect(frame.serial_consistency_level == null);
    try testing.expect(frame.timestamp == null);
    try testing.expect(frame.keyspace == null);
    try testing.expect(frame.now_in_seconds == null);

    // write

    try testutils.expectSameRawFrame(BatchFrame, frame, raw_frame.header, exp);
}

test "batch frame: query type prepared" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x01\x00\x0d\x00\x00\x00\xa2\x00\x00\x03\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\x3a\x9a\xab\x41\x68\x24\x4a\xef\x9d\xf5\x72\xc7\x84\xab\xa2\x57\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\xed\x54\xb0\x6d\xcc\xb2\x43\x51\x96\x51\x74\x5e\xee\xae\xd2\xfe\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\x79\xdf\x8a\x28\x5a\x60\x47\x19\x9b\x42\x84\xea\x69\x10\x1a\xe6\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x00\x00\x00";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), exp);

    try checkHeader(Opcode.Batch, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try BatchFrame.read(arena.allocator(), raw_frame.header.version, &pr);

    try testing.expectEqual(BatchType.Logged, frame.batch_type);

    const expUUIDs = &[_][]const u8{
        "\x3a\x9a\xab\x41\x68\x24\x4a\xef\x9d\xf5\x72\xc7\x84\xab\xa2\x57",
        "\xed\x54\xb0\x6d\xcc\xb2\x43\x51\x96\x51\x74\x5e\xee\xae\xd2\xfe",
        "\x79\xdf\x8a\x28\x5a\x60\x47\x19\x9b\x42\x84\xea\x69\x10\x1a\xe6",
    };

    try testing.expectEqual(@as(usize, 3), frame.queries.len);
    var i: usize = 0;
    for (frame.queries) |query| {
        try testing.expect(query.query_string == null);
        const exp_query_id = "\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65";
        try testing.expectEqualSlices(u8, exp_query_id, query.query_id.?);

        try testing.expect(query.values == .Normal);
        try testing.expectEqual(@as(usize, 2), query.values.Normal.len);

        const value1 = query.values.Normal[0];
        try testing.expectEqualSlices(u8, expUUIDs[i], value1.Set);

        const value2 = query.values.Normal[1];
        try testing.expectEqualStrings("Vincent", value2.Set);

        i += 1;
    }

    try testing.expectEqual(Consistency.Any, frame.consistency_level);
    try testing.expect(frame.serial_consistency_level == null);
    try testing.expect(frame.timestamp == null);
    try testing.expect(frame.keyspace == null);
    try testing.expect(frame.now_in_seconds == null);

    // write

    try testutils.expectSameRawFrame(BatchFrame, frame, raw_frame.header, exp);
}

/// EVENT is an event pushed by the server.
///
/// Described in the protocol spec at §4.2.6.
pub const EventFrame = struct {
    const Self = @This();

    event: event.Event,

    pub fn read(allocator: mem.Allocator, pr: *PrimitiveReader) !Self {
        var frame = Self{
            .event = undefined,
        };

        const event_type = std.meta.stringToEnum(event.EventType, try pr.readString(allocator)) orelse return error.InvalidEventType;

        switch (event_type) {
            .TOPOLOGY_CHANGE => {
                var change = event.TopologyChange{
                    .type = undefined,
                    .node_address = undefined,
                };

                change.type = std.meta.stringToEnum(event.TopologyChangeType, try pr.readString(allocator)) orelse return error.InvalidTopologyChangeType;
                change.node_address = try pr.readInet();

                frame.event = event.Event{ .TOPOLOGY_CHANGE = change };

                return frame;
            },
            .STATUS_CHANGE => {
                var change = event.StatusChange{
                    .type = undefined,
                    .node_address = undefined,
                };

                change.type = std.meta.stringToEnum(event.StatusChangeType, try pr.readString(allocator)) orelse return error.InvalidStatusChangeType;
                change.node_address = try pr.readInet();

                frame.event = event.Event{ .STATUS_CHANGE = change };

                return frame;
            },
            .SCHEMA_CHANGE => {
                frame.event = event.Event{ .SCHEMA_CHANGE = try event.SchemaChange.read(allocator, pr) };

                return frame;
            },
        }
    }
};

test "event frame: topology change" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x84\x00\xff\xff\x0c\x00\x00\x00\x24\x00\x0f\x54\x4f\x50\x4f\x4c\x4f\x47\x59\x5f\x43\x48\x41\x4e\x47\x45\x00\x08\x4e\x45\x57\x5f\x4e\x4f\x44\x45\x04\x7f\x00\x00\x04\x00\x00\x23\x52";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), exp);

    try checkHeader(Opcode.Event, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try EventFrame.read(arena.allocator(), &pr);

    try testing.expect(frame.event == .TOPOLOGY_CHANGE);

    const topology_change = frame.event.TOPOLOGY_CHANGE;
    try testing.expectEqual(event.TopologyChangeType.NEW_NODE, topology_change.type);

    const localhost = net.Address.initIp4([4]u8{ 0x7f, 0x00, 0x00, 0x04 }, 9042);
    try testing.expect(net.Address.eql(localhost, topology_change.node_address));
}

test "event frame: status change" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x1e\x00\x0d\x53\x54\x41\x54\x55\x53\x5f\x43\x48\x41\x4e\x47\x45\x00\x04\x44\x4f\x57\x4e\x04\x7f\x00\x00\x01\x00\x00\x23\x52";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Event, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try EventFrame.read(arena.allocator(), &pr);

    try testing.expect(frame.event == .STATUS_CHANGE);

    const status_change = frame.event.STATUS_CHANGE;
    try testing.expectEqual(event.StatusChangeType.DOWN, status_change.type);

    const localhost = net.Address.initIp4([4]u8{ 0x7f, 0x00, 0x00, 0x01 }, 9042);
    try testing.expect(net.Address.eql(localhost, status_change.node_address));
}

test "event frame: schema change/keyspace" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x2a\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45\x00\x07\x43\x52\x45\x41\x54\x45\x44\x00\x08\x4b\x45\x59\x53\x50\x41\x43\x45\x00\x06\x62\x61\x72\x62\x61\x7a";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Event, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try EventFrame.read(arena.allocator(), &pr);

    try testing.expect(frame.event == .SCHEMA_CHANGE);

    const schema_change = frame.event.SCHEMA_CHANGE;
    try testing.expectEqual(event.SchemaChangeType.CREATED, schema_change.type);
    try testing.expectEqual(event.SchemaChangeTarget.KEYSPACE, schema_change.target);

    const options = schema_change.options;
    try testing.expectEqualStrings("barbaz", options.keyspace);
    try testing.expectEqualStrings("", options.object_name);
    try testing.expect(options.arguments == null);
}

test "event frame: schema change/table" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x2e\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45\x00\x07\x43\x52\x45\x41\x54\x45\x44\x00\x05\x54\x41\x42\x4c\x45\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x05\x73\x61\x6c\x75\x74";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Event, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try EventFrame.read(arena.allocator(), &pr);

    try testing.expect(frame.event == .SCHEMA_CHANGE);

    const schema_change = frame.event.SCHEMA_CHANGE;
    try testing.expectEqual(event.SchemaChangeType.CREATED, schema_change.type);
    try testing.expectEqual(event.SchemaChangeTarget.TABLE, schema_change.target);

    const options = schema_change.options;
    try testing.expectEqualStrings("foobar", options.keyspace);
    try testing.expectEqualStrings("salut", options.object_name);
    try testing.expect(options.arguments == null);
}

test "event frame: schema change/function" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x40\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45\x00\x07\x43\x52\x45\x41\x54\x45\x44\x00\x08\x46\x55\x4e\x43\x54\x49\x4f\x4e\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x0d\x73\x6f\x6d\x65\x5f\x66\x75\x6e\x63\x74\x69\x6f\x6e\x00\x01\x00\x03\x69\x6e\x74";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Event, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try EventFrame.read(arena.allocator(), &pr);

    try testing.expect(frame.event == .SCHEMA_CHANGE);

    const schema_change = frame.event.SCHEMA_CHANGE;
    try testing.expectEqual(event.SchemaChangeType.CREATED, schema_change.type);
    try testing.expectEqual(event.SchemaChangeTarget.FUNCTION, schema_change.target);

    const options = schema_change.options;
    try testing.expectEqualStrings("foobar", options.keyspace);
    try testing.expectEqualStrings("some_function", options.object_name);
    const arguments = options.arguments.?;
    try testing.expectEqual(@as(usize, 1), arguments.len);
    try testing.expectEqualStrings("int", arguments[0]);
}

/// PREPARE is sent to prepare a CQL query for later execution (through EXECUTE).
///
/// Described in the protocol spec at §4.1.5
pub const PrepareFrame = struct {
    const Self = @This();

    query: []const u8,
    keyspace: ?[]const u8,

    const FlagWithKeyspace = 0x01;

    pub fn write(self: Self, protocol_version: ProtocolVersion, pw: *PrimitiveWriter) !void {
        _ = try pw.writeLongString(self.query);
        if (!protocol_version.is(5)) {
            return;
        }

        if (self.keyspace) |ks| {
            _ = try pw.writeInt(u32, FlagWithKeyspace);
            _ = try pw.writeString(ks);
        } else {
            _ = try pw.writeInt(u32, 0);
        }
    }

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        var frame = Self{
            .query = undefined,
            .keyspace = null,
        };

        frame.query = try pr.readLongString(allocator);

        if (!protocol_version.is(5)) {
            return frame;
        }

        const flags = try pr.readInt(u32);
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            frame.keyspace = try pr.readString(allocator);
        }

        return frame;
    }
};

test "prepare frame" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\xc0\x09\x00\x00\x00\x32\x00\x00\x00\x2e\x53\x45\x4c\x45\x43\x54\x20\x61\x67\x65\x2c\x20\x6e\x61\x6d\x65\x20\x66\x72\x6f\x6d\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x20\x77\x68\x65\x72\x65\x20\x69\x64\x20\x3d\x20\x3f";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), exp);

    try checkHeader(Opcode.Prepare, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try PrepareFrame.read(arena.allocator(), raw_frame.header.version, &pr);

    try testing.expectEqualStrings("SELECT age, name from foobar.user where id = ?", frame.query);
    try testing.expect(frame.keyspace == null);

    // write

    try testutils.expectSameRawFrame(PrepareFrame, frame, raw_frame.header, exp);
}

/// QUERY is sent to perform a CQL query.
///
/// Described in the protocol spec at §4.1.4
pub const QueryFrame = struct {
    const Self = @This();

    query: []const u8,
    query_parameters: QueryParameters,

    pub fn write(self: Self, protocol_version: ProtocolVersion, pw: *PrimitiveWriter) !void {
        _ = try pw.writeLongString(self.query);
        _ = try self.query_parameters.write(protocol_version, pw);
    }

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        return Self{
            .query = try pr.readLongString(allocator),
            .query_parameters = try QueryParameters.read(allocator, protocol_version, pr),
        };
    }
};

test "query frame: no values, no paging state" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\x08\x07\x00\x00\x00\x30\x00\x00\x00\x1b\x53\x45\x4c\x45\x43\x54\x20\x2a\x20\x46\x52\x4f\x4d\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x20\x3b\x00\x01\x34\x00\x00\x00\x64\x00\x08\x00\x05\xa2\x2c\xf0\x57\x3e\x3f";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), exp);

    try checkHeader(Opcode.Query, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try QueryFrame.read(arena.allocator(), raw_frame.header.version, &pr);

    try testing.expectEqualStrings("SELECT * FROM foobar.user ;", frame.query);
    try testing.expectEqual(Consistency.One, frame.query_parameters.consistency_level);
    try testing.expect(frame.query_parameters.values == null);
    try testing.expectEqual(@as(u32, 100), frame.query_parameters.page_size.?);
    try testing.expect(frame.query_parameters.paging_state == null);
    try testing.expectEqual(Consistency.Serial, frame.query_parameters.serial_consistency_level.?);
    try testing.expectEqual(@as(u64, 1585688778063423), frame.query_parameters.timestamp.?);
    try testing.expect(frame.query_parameters.keyspace == null);
    try testing.expect(frame.query_parameters.now_in_seconds == null);

    // write

    try testutils.expectSameRawFrame(QueryFrame, frame, raw_frame.header, exp);
}

/// READY is sent by a node to indicate it is ready to process queries.
///
/// Described in the protocol spec at §4.2.2.
pub const ReadyFrame = struct {};

test "ready frame" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x02\x02\x00\x00\x00\x00";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Ready, data.len, raw_frame.header);
}

/// REGISTER is sent to register this connection to receive some types of events.
///
/// Described in the protocol spec at §4.1.8
const RegisterFrame = struct {
    event_types: []const []const u8,

    pub fn write(self: @This(), pw: *PrimitiveWriter) !void {
        return pw.writeStringList(self.event_types);
    }

    pub fn read(allocator: mem.Allocator, pr: *PrimitiveReader) !RegisterFrame {
        return RegisterFrame{
            .event_types = try pr.readStringList(allocator),
        };
    }
};

test "register frame" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\xc0\x0b\x00\x00\x00\x31\x00\x03\x00\x0f\x54\x4f\x50\x4f\x4c\x4f\x47\x59\x5f\x43\x48\x41\x4e\x47\x45\x00\x0d\x53\x54\x41\x54\x55\x53\x5f\x43\x48\x41\x4e\x47\x45\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), exp);

    try checkHeader(Opcode.Register, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try RegisterFrame.read(arena.allocator(), &pr);

    try testing.expectEqual(@as(usize, 3), frame.event_types.len);
    try testing.expectEqualStrings("TOPOLOGY_CHANGE", frame.event_types[0]);
    try testing.expectEqualStrings("STATUS_CHANGE", frame.event_types[1]);
    try testing.expectEqualStrings("SCHEMA_CHANGE", frame.event_types[2]);

    // write

    try testutils.expectSameRawFrame(RegisterFrame, frame, raw_frame.header, exp);
}

pub const Result = union(ResultKind) {
    Void: void,
    Rows: Rows,
    SetKeyspace: []const u8,
    Prepared: Prepared,
    SchemaChange: event.SchemaChange,
};

/// RESULT is the result to a query (QUERY, PREPARE, EXECUTE or BATCH messages).
///
/// Described in the protocol spec at §4.2.5.
const ResultKind = enum(u32) {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    Prepared = 0x0004,
    SchemaChange = 0x0005,
};

const Rows = struct {
    const Self = @This();

    metadata: metadata.RowsMetadata,
    data: []RowData,

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        var rows = Self{
            .metadata = undefined,
            .data = undefined,
        };

        rows.metadata = try metadata.RowsMetadata.read(allocator, protocol_version, pr);

        // Iterate over rows
        const rows_count = @as(usize, try pr.readInt(u32));

        var data = std.ArrayList(RowData).init(allocator);
        _ = try data.ensureTotalCapacity(rows_count);
        errdefer data.deinit();

        var i: usize = 0;
        while (i < rows_count) : (i += 1) {
            var row_data = std.ArrayList(ColumnData).init(allocator);
            errdefer row_data.deinit();

            // Read a single row
            var j: usize = 0;
            while (j < rows.metadata.columns_count) : (j += 1) {
                const column_data = (try pr.readBytes(allocator)) orelse &[_]u8{};

                _ = try row_data.append(ColumnData{
                    .slice = column_data,
                });
            }

            _ = try data.append(RowData{
                .slice = try row_data.toOwnedSlice(),
            });
        }

        rows.data = try data.toOwnedSlice();

        return rows;
    }
};

const Prepared = struct {
    const Self = @This();

    query_id: []const u8,
    result_metadata_id: ?[]const u8,
    metadata: metadata.PreparedMetadata,
    rows_metadata: metadata.RowsMetadata,

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        var prepared = Self{
            .query_id = undefined,
            .result_metadata_id = null,
            .metadata = undefined,
            .rows_metadata = undefined,
        };

        prepared.query_id = (try pr.readShortBytes(allocator)) orelse return error.NoQueryIDInPreparedFrame;
        if (protocol_version.is(5)) {
            prepared.result_metadata_id = (try pr.readShortBytes(allocator)) orelse return error.NoResultMetadataIDInPreparedFrame;
        }

        prepared.metadata = try metadata.PreparedMetadata.read(allocator, protocol_version, pr);
        prepared.rows_metadata = try metadata.RowsMetadata.read(allocator, protocol_version, pr);

        return prepared;
    }
};

pub const ResultFrame = struct {
    const Self = @This();

    result: Result,

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !ResultFrame {
        var frame = Self{
            .result = undefined,
        };

        const kind: ResultKind = @enumFromInt(try pr.readInt(u32));

        switch (kind) {
            .Void => frame.result = Result{ .Void = {} },
            .Rows => {
                const rows = try Rows.read(allocator, protocol_version, pr);
                frame.result = Result{ .Rows = rows };
            },
            .SetKeyspace => {
                const keyspace = try pr.readString(allocator);
                frame.result = Result{ .SetKeyspace = keyspace };
            },
            .Prepared => {
                const prepared = try Prepared.read(allocator, protocol_version, pr);
                frame.result = Result{ .Prepared = prepared };
            },
            .SchemaChange => {
                const schema_change = try event.SchemaChange.read(allocator, pr);
                frame.result = Result{ .SchemaChange = schema_change };
            },
        }

        return frame;
    }
};

test "result frame: void" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x9d\x08\x00\x00\x00\x04\x00\x00\x00\x01";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(arena.allocator(), raw_frame.header.version, &pr);

    try testing.expect(frame.result == .Void);
}

test "result frame: rows" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x20\x08\x00\x00\x00\xa2\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x03\x00\x00\x00\x10\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\xd7\x77\xd5\xd7\x58\xc0\x4d\x2b\x8c\xf9\xa3\x53\xfa\x8e\x6c\x96\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\x94\xa4\x7b\xb2\x8c\xf7\x43\x3d\x97\x6e\x72\x74\xb3\xfd\xd3\x31\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(arena.allocator(), raw_frame.header.version, &pr);

    try testing.expect(frame.result == .Rows);

    // check metadata

    const rows_metadata = frame.result.Rows.metadata;
    try testing.expect(rows_metadata.paging_state == null);
    try testing.expect(rows_metadata.new_metadata_id == null);
    try testing.expectEqualStrings("foobar", rows_metadata.global_table_spec.?.keyspace);
    try testing.expectEqualStrings("user", rows_metadata.global_table_spec.?.table);
    try testing.expectEqual(@as(usize, 3), rows_metadata.column_specs.len);

    const col1 = rows_metadata.column_specs[0];
    try testing.expectEqualStrings("id", col1.name);
    try testing.expectEqual(OptionID.UUID, col1.option);
    const col2 = rows_metadata.column_specs[1];
    try testing.expectEqualStrings("age", col2.name);
    try testing.expectEqual(OptionID.Tinyint, col2.option);
    const col3 = rows_metadata.column_specs[2];
    try testing.expectEqualStrings("name", col3.name);
    try testing.expectEqual(OptionID.Varchar, col3.option);

    // check data

    const rows = frame.result.Rows;
    try testing.expectEqual(@as(usize, 3), rows.data.len);

    const row1 = rows.data[0].slice;
    try testing.expectEqualSlices(u8, "\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8", row1[0].slice);
    try testing.expectEqualSlices(u8, "\x00", row1[1].slice);
    try testing.expectEqualSlices(u8, "\x56\x69\x6e\x63\x65\x6e\x74\x30", row1[2].slice);

    const row2 = rows.data[1].slice;
    try testing.expectEqualSlices(u8, "\xd7\x77\xd5\xd7\x58\xc0\x4d\x2b\x8c\xf9\xa3\x53\xfa\x8e\x6c\x96", row2[0].slice);
    try testing.expectEqualSlices(u8, "\x01", row2[1].slice);
    try testing.expectEqualSlices(u8, "\x56\x69\x6e\x63\x65\x6e\x74\x31", row2[2].slice);

    const row3 = rows.data[2].slice;
    try testing.expectEqualSlices(u8, "\x94\xa4\x7b\xb2\x8c\xf7\x43\x3d\x97\x6e\x72\x74\xb3\xfd\xd3\x31", row3[0].slice);
    try testing.expectEqualSlices(u8, "\x02", row3[1].slice);
    try testing.expectEqualSlices(u8, "\x56\x69\x6e\x63\x65\x6e\x74\x32", row3[2].slice);
}

test "result frame: rows, don't skip metadata" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x00\x08\x00\x00\x02\x3a\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x00\x00\x28\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x2e\x64\x62\x2e\x6d\x61\x72\x73\x68\x61\x6c\x2e\x42\x79\x74\x65\x54\x79\x70\x65\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x0d\x00\x00\x00\x10\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\x87\x0e\x45\x7f\x56\x4a\x4f\xd5\xb5\xd6\x4a\x48\x4b\xe0\x67\x67\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\xc5\xb1\x12\xf4\x0d\xea\x4d\x22\x83\x5b\xe0\x25\xef\x69\x0c\xe1\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\xd7\x77\xd5\xd7\x58\xc0\x4d\x2b\x8c\xf9\xa3\x53\xfa\x8e\x6c\x96\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\x65\x54\x02\x89\x33\xe1\x42\x73\x82\xcc\x1c\xdb\x3d\x24\x5e\x40\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\x6a\xd1\x07\x9d\x27\x23\x47\x76\x8b\x7f\x39\x4d\xe3\xb8\x97\xc5\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32\x00\x00\x00\x10\xe9\xef\xfe\xa6\xeb\xc5\x4d\xb9\xaf\xc7\xd7\xc0\x28\x43\x27\x40\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\x96\xe6\xdd\x62\x14\xc9\x4e\x7c\xa1\x2f\x98\x5e\xe9\xe0\x91\x0d\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32\x00\x00\x00\x10\xef\xd5\x5a\x9b\xec\x7f\x4c\x5c\x89\xc3\x8c\xfa\x28\xf9\x6d\xfe\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\x94\xa4\x7b\xb2\x8c\xf7\x43\x3d\x97\x6e\x72\x74\xb3\xfd\xd3\x31\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32\x00\x00\x00\x10\xe6\x02\xc7\x47\xbf\xca\x44\xbc\x9d\xc6\x6b\x04\x0f\xb7\x15\xed\x00\x00\x00\x01\x78\x00\x00\x00\x04\x48\x61\x68\x61\x00\x00\x00\x10\xac\x5e\xcc\xa8\x8e\xa1\x42\x2f\x86\xe6\xa0\x93\xbe\xd2\x73\x22\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32\x00\x00\x00\x10\xbe\x90\x37\x66\x31\xe5\x43\x93\xbc\x99\x43\xd3\x69\xf8\xe6\xba\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(arena.allocator(), raw_frame.header.version, &pr);

    try testing.expect(frame.result == .Rows);

    // check metadata

    const rows_metadata = frame.result.Rows.metadata;
    try testing.expect(rows_metadata.paging_state == null);
    try testing.expect(rows_metadata.new_metadata_id == null);
    try testing.expectEqualStrings("foobar", rows_metadata.global_table_spec.?.keyspace);
    try testing.expectEqualStrings("user", rows_metadata.global_table_spec.?.table);
    try testing.expectEqual(@as(usize, 3), rows_metadata.column_specs.len);

    const col1 = rows_metadata.column_specs[0];
    try testing.expectEqualStrings("id", col1.name);
    try testing.expectEqual(OptionID.UUID, col1.option);
    const col2 = rows_metadata.column_specs[1];
    try testing.expectEqualStrings("age", col2.name);
    try testing.expectEqual(OptionID.Custom, col2.option);
    const col3 = rows_metadata.column_specs[2];
    try testing.expectEqualStrings("name", col3.name);
    try testing.expectEqual(OptionID.Varchar, col3.option);

    // check data

    const rows = frame.result.Rows;
    try testing.expectEqual(@as(usize, 13), rows.data.len);

    const row1 = rows.data[0].slice;
    try testing.expectEqualSlices(u8, "\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8", row1[0].slice);
    try testing.expectEqualSlices(u8, "\x00", row1[1].slice);
    try testing.expectEqualSlices(u8, "\x56\x69\x6e\x63\x65\x6e\x74\x30", row1[2].slice);
}

test "result frame: rows, list of uuid" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x00\x08\x00\x00\x00\x58\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x02\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x0a\x61\x67\x65\x5f\x74\x6f\x5f\x69\x64\x73\x00\x03\x61\x67\x65\x00\x09\x00\x03\x69\x64\x73\x00\x20\x00\x0c\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x78\x00\x00\x00\x18\x00\x00\x00\x01\x00\x00\x00\x10\xe6\x02\xc7\x47\xbf\xca\x44\xbc\x9d\xc6\x6b\x04\x0f\xb7\x15\xed";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(arena.allocator(), raw_frame.header.version, &pr);

    try testing.expect(frame.result == .Rows);

    // check metadata

    const rows_metadata = frame.result.Rows.metadata;
    try testing.expect(rows_metadata.paging_state == null);
    try testing.expect(rows_metadata.new_metadata_id == null);
    try testing.expectEqualStrings("foobar", rows_metadata.global_table_spec.?.keyspace);
    try testing.expectEqualStrings("age_to_ids", rows_metadata.global_table_spec.?.table);
    try testing.expectEqual(@as(usize, 2), rows_metadata.column_specs.len);

    const col1 = rows_metadata.column_specs[0];
    try testing.expectEqualStrings("age", col1.name);
    try testing.expectEqual(OptionID.Int, col1.option);
    const col2 = rows_metadata.column_specs[1];
    try testing.expectEqualStrings("ids", col2.name);
    try testing.expectEqual(OptionID.List, col2.option);

    // check data

    const rows = frame.result.Rows;
    try testing.expectEqual(@as(usize, 1), rows.data.len);

    const row1 = rows.data[0].slice;
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x78", row1[0].slice);
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x01\x00\x00\x00\x10\xe6\x02\xc7\x47\xbf\xca\x44\xbc\x9d\xc6\x6b\x04\x0f\xb7\x15\xed", row1[1].slice);
}

test "result frame: set keyspace" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x77\x08\x00\x00\x00\x0c\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(arena.allocator(), raw_frame.header.version, &pr);

    try testing.expect(frame.result == .SetKeyspace);
    try testing.expectEqualStrings("foobar", frame.result.SetKeyspace);
}

test "result frame: prepared insert" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x80\x08\x00\x00\x00\x4f\x00\x00\x00\x04\x00\x10\x63\x7c\x1c\x1f\xd0\x13\x4a\xb8\xfc\x94\xca\x67\xf2\x88\xb2\xa3\x00\x00\x00\x01\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x04\x00\x00\x00\x00";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(arena.allocator(), raw_frame.header.version, &pr);

    try testing.expect(frame.result == .Prepared);

    // check prepared metadata

    {
        const prepared_metadata = frame.result.Prepared.metadata;
        try testing.expectEqualStrings("foobar", prepared_metadata.global_table_spec.?.keyspace);
        try testing.expectEqualStrings("user", prepared_metadata.global_table_spec.?.table);
        try testing.expectEqual(@as(usize, 1), prepared_metadata.pk_indexes.len);
        try testing.expectEqual(@as(u16, 0), prepared_metadata.pk_indexes[0]);
        try testing.expectEqual(@as(usize, 3), prepared_metadata.column_specs.len);

        const col1 = prepared_metadata.column_specs[0];
        try testing.expectEqualStrings("id", col1.name);
        try testing.expectEqual(OptionID.UUID, col1.option);
        const col2 = prepared_metadata.column_specs[1];
        try testing.expectEqualStrings("age", col2.name);
        try testing.expectEqual(OptionID.Tinyint, col2.option);
        const col3 = prepared_metadata.column_specs[2];
        try testing.expectEqualStrings("name", col3.name);
        try testing.expectEqual(OptionID.Varchar, col3.option);
    }

    // check rows metadata

    {
        const rows_metadata = frame.result.Prepared.rows_metadata;
        try testing.expect(rows_metadata.global_table_spec == null);
        try testing.expectEqual(@as(usize, 0), rows_metadata.column_specs.len);
    }
}

test "result frame: prepared select" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\xc0\x08\x00\x00\x00\x63\x00\x00\x00\x04\x00\x10\x3b\x2e\x8d\x03\x43\xf4\x3b\xfc\xad\xa1\x78\x9c\x27\x0e\xcf\xee\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x00\x00\x01\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(arena.allocator(), raw_frame.header.version, &pr);

    try testing.expect(frame.result == .Prepared);

    // check prepared metadata

    {
        const prepared_metadata = frame.result.Prepared.metadata;
        try testing.expectEqualStrings("foobar", prepared_metadata.global_table_spec.?.keyspace);
        try testing.expectEqualStrings("user", prepared_metadata.global_table_spec.?.table);
        try testing.expectEqual(@as(usize, 1), prepared_metadata.pk_indexes.len);
        try testing.expectEqual(@as(u16, 0), prepared_metadata.pk_indexes[0]);
        try testing.expectEqual(@as(usize, 1), prepared_metadata.column_specs.len);

        const col1 = prepared_metadata.column_specs[0];
        try testing.expectEqualStrings("id", col1.name);
        try testing.expectEqual(OptionID.UUID, col1.option);
    }

    // check rows metadata

    {
        const rows_metadata = frame.result.Prepared.rows_metadata;
        try testing.expectEqualStrings("foobar", rows_metadata.global_table_spec.?.keyspace);
        try testing.expectEqualStrings("user", rows_metadata.global_table_spec.?.table);
        try testing.expectEqual(@as(usize, 3), rows_metadata.column_specs.len);

        const col1 = rows_metadata.column_specs[0];
        try testing.expectEqualStrings("id", col1.name);
        try testing.expectEqual(OptionID.UUID, col1.option);
        const col2 = rows_metadata.column_specs[1];
        try testing.expectEqualStrings("age", col2.name);
        try testing.expectEqual(OptionID.Tinyint, col2.option);
        const col3 = rows_metadata.column_specs[2];
        try testing.expectEqualStrings("name", col3.name);
        try testing.expectEqual(OptionID.Varchar, col3.option);
    }
}

/// SUPPORTED is sent by a node in response to a OPTIONS frame.
///
/// Described in the protocol spec at §4.2.4.
pub const SupportedFrame = struct {
    const Self = @This();

    pub const opcode: Opcode = .Supported;

    protocol_versions: []ProtocolVersion,
    cql_versions: []CQLVersion,
    compression_algorithms: []CompressionAlgorithm,

    pub fn read(allocator: mem.Allocator, pr: *PrimitiveReader) !Self {
        var frame = Self{
            .protocol_versions = &[_]ProtocolVersion{},
            .cql_versions = &[_]CQLVersion{},
            .compression_algorithms = &[_]CompressionAlgorithm{},
        };

        const options = try pr.readStringMultimap(allocator);

        if (options.get("CQL_VERSION")) |values| {
            var list = std.ArrayList(CQLVersion).init(allocator);

            for (values) |value| {
                const version = try CQLVersion.fromString(value);
                _ = try list.append(version);
            }

            frame.cql_versions = try list.toOwnedSlice();
        } else {
            return error.NoCQLVersion;
        }

        if (options.get("COMPRESSION")) |values| {
            var list = std.ArrayList(CompressionAlgorithm).init(allocator);

            for (values) |value| {
                const compression_algorithm = try CompressionAlgorithm.fromString(value);
                _ = try list.append(compression_algorithm);
            }

            frame.compression_algorithms = try list.toOwnedSlice();
        }

        if (options.get("PROTOCOL_VERSIONS")) |values| {
            var list = std.ArrayList(ProtocolVersion).init(allocator);

            for (values) |value| {
                const version = try ProtocolVersion.fromString(value);
                _ = try list.append(version);
            }

            frame.protocol_versions = try list.toOwnedSlice();
        }

        return frame;
    }
};

test "supported frame" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x84\x00\x00\x09\x06\x00\x00\x00\x60\x00\x03\x00\x11\x50\x52\x4f\x54\x4f\x43\x4f\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x53\x00\x03\x00\x04\x33\x2f\x76\x33\x00\x04\x34\x2f\x76\x34\x00\x09\x35\x2f\x76\x35\x2d\x62\x65\x74\x61\x00\x0b\x43\x4f\x4d\x50\x52\x45\x53\x53\x49\x4f\x4e\x00\x02\x00\x06\x73\x6e\x61\x70\x70\x79\x00\x03\x6c\x7a\x34\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x01\x00\x05\x33\x2e\x34\x2e\x34";
    const raw_frame = try testutils.readRawFrame(arena.allocator(), exp);

    try checkHeader(Opcode.Supported, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try SupportedFrame.read(arena.allocator(), &pr);

    try testing.expectEqual(@as(usize, 1), frame.cql_versions.len);
    try testing.expectEqual(CQLVersion{ .major = 3, .minor = 4, .patch = 4 }, frame.cql_versions[0]);

    try testing.expectEqual(@as(usize, 3), frame.protocol_versions.len);
    try testing.expect(frame.protocol_versions[0].is(3));
    try testing.expect(frame.protocol_versions[1].is(4));
    try testing.expect(frame.protocol_versions[2].is(5));

    try testing.expectEqual(@as(usize, 2), frame.compression_algorithms.len);
    try testing.expectEqual(CompressionAlgorithm.Snappy, frame.compression_algorithms[0]);
    try testing.expectEqual(CompressionAlgorithm.LZ4, frame.compression_algorithms[1]);
}
