const std = @import("std");
const heap = std.heap;
const io = std.io;
const mem = std.mem;
const net = std.net;
const os = std.os;
const testing = std.testing;
const assert = std.debug.assert;

const lz4 = @import("lz4");

const casstest = @import("casstest.zig");
const event = @import("event.zig");
const metadata = @import("metadata.zig");
const testutils = @import("testutils.zig");

const QueryParameters = @import("QueryParameters.zig");

// Framing format
//
// The most low level "unit" of data that can be read or written.
// This is specific to protocol v5 !
//
// See §2 in native_protocol_v5.spec.

pub const Frame = struct {
    /// Indicates that the payload includes one or more complete envelopes and can be fully processed immediately.
    is_self_contained: bool,
    /// The frame payload, the content depends on `is_self_contained`:
    /// * if self contained, the payload is one or more complete envelopes
    /// * if not self contained, the payload is one part of a large envelope
    payload: []const u8,

    const uncompressed_header_size = 6;
    const compressed_header_size = 8;
    const trailer_size = 4;
    const max_payload_size = 131071;
    const initial_crc32_bytes = "\xFA\x2D\x55\xCA";

    pub const Format = enum {
        compressed,
        uncompressed,
    };

    fn crc24(input: anytype) usize {
        // Don't use @sizeOf because it contains a padding byte
        const Size = @typeInfo(@TypeOf(input)).int.bits / 8;
        comptime assert(Size > 0 and Size < 9);

        // This is adapted from https://github.com/apache/cassandra/blob/1bd4bcf4e561144497adc86e1b48480eab6171d4/src/java/org/apache/cassandra/net/Crc.java#L119-L135

        const crc24_initial = 0x875060;
        const crc24_polynomial = 0x1974F0B;

        var crc: usize = crc24_initial;
        var buf = input;
        var len: usize = Size;

        while (len > 0) : (len -= 1) {
            const b = buf & 0xff;
            crc ^= @as(usize, @intCast(b)) << 16;
            buf >>= 8;

            var i: usize = 0;
            while (i < 8) : (i += 1) {
                crc <<= 1;
                if ((crc & 0x1000000) != 0) {
                    crc ^= crc24_polynomial;
                }
            }
        }

        return crc;
    }

    const PayloadAndTrailer = struct {
        payload: []const u8,
        trailer: [4]u8,

        pub fn length(self: PayloadAndTrailer) usize {
            return self.payload.len + self.trailer.len;
        }
    };

    fn readHeader(reader: anytype, comptime N: comptime_int) ![N]u8 {
        var buf: [N]u8 = undefined;
        const n = try reader.readAll(&buf);
        if (n != N) return error.UnexpectedEOF;

        return buf;
    }

    fn readPayloadAndTrailer(reader: anytype, allocator: mem.Allocator, payload_length: usize) !PayloadAndTrailer {
        const size = payload_length + trailer_size;

        const res = try allocator.alloc(u8, size);
        const n = try reader.readAll(res);
        if (n != size) return error.UnexpectedEOF;

        return .{
            .payload = res[0 .. res.len - trailer_size],
            .trailer = blk: {
                var tmp: [4]u8 = undefined;
                @memcpy(&tmp, res[res.len - trailer_size ..]);
                break :blk tmp;
            },
        };
    }

    fn computeCR32(payload: []const u8) u32 {
        var hash = std.hash.Crc32.init();
        hash.update(initial_crc32_bytes);
        hash.update(payload);
        return hash.final();
    }

    fn computeAndVerifyCRC32(payload_and_trailer: PayloadAndTrailer) !void {
        const computed_crc32 = computeCR32(payload_and_trailer.payload);
        const expected_crc32 = mem.readInt(u32, &payload_and_trailer.trailer, .little);
        if (computed_crc32 != expected_crc32) {
            return error.InvalidPayloadChecksum;
        }
    }

    pub const DecodeError = error{
        UnexpectedEOF,
        InvalidPayloadChecksum,
        InvalidHeaderChecksum,
    } || mem.Allocator.Error || std.posix.ReadError || lz4.DecompressError;

    pub const DecodeResult = struct {
        frame: Frame,
        consumed: usize,
    };

    /// Try to decode a frame contained in the `data` slice.
    ///
    /// If there's not enough data or if the input data is corrupted somehow, an error is returned.
    /// Otherwise a result containing both the frame and the number of bytes consumed is returned.
    pub fn decode(allocator: mem.Allocator, data: []const u8, format: Format) Frame.DecodeError!DecodeResult {
        var fbs = io.fixedBufferStream(data);

        switch (format) {
            .compressed => return decodeCompressed(allocator, fbs.reader()),
            .uncompressed => return decodeUncompressed(allocator, fbs.reader()),
        }
    }

    pub const ReadError = error{} || DecodeError;

    /// Try to read a frame from the reader.
    ///
    /// NOTE(vincent): this will change once we stop using synchronous read calls
    pub fn read(allocator: mem.Allocator, reader: anytype, format: Format) Frame.ReadError!DecodeResult {
        switch (format) {
            .compressed => return decodeCompressed(allocator, reader),
            .uncompressed => return decodeUncompressed(allocator, reader),
        }
    }

    fn decodeUncompressed(allocator: mem.Allocator, reader: anytype) Frame.DecodeError!DecodeResult {
        // Read and parse header
        const header_data = try readHeader(reader, uncompressed_header_size);

        const header3b: u24 = mem.readInt(u24, header_data[0..3], .little);
        const header_expected_crc = @as(usize, @intCast(mem.readInt(u24, header_data[3..uncompressed_header_size], .little)));

        const payload_length: u17 = @intCast(header3b & 0x1FFFF);
        const is_self_contained = header3b & (1 << 17) != 0;
        const computed_header_crc = crc24(header3b);
        if (computed_header_crc != header_expected_crc) {
            return error.InvalidHeaderChecksum;
        }

        // Read payload and trailer
        const payload_and_trailer = try readPayloadAndTrailer(reader, allocator, payload_length);

        // Verify payload CRC32
        try computeAndVerifyCRC32(payload_and_trailer);

        return .{
            .frame = .{
                .payload = payload_and_trailer.payload,
                .is_self_contained = is_self_contained,
            },
            .consumed = uncompressed_header_size + payload_and_trailer.length(),
        };
    }

    fn decodeCompressed(allocator: mem.Allocator, reader: anytype) Frame.DecodeError!DecodeResult {
        // Read and parse header
        const header = try readHeader(reader, compressed_header_size);

        const header5b: u40 = mem.readInt(u40, header[0..5], .little);
        const header_expected_crc = @as(usize, @intCast(mem.readInt(u24, header[5..compressed_header_size], .little)));

        const compressed_length: u17 = @intCast(header5b & 0x1FFFF);
        const uncompressed_length: u17 = @intCast(header5b >> 17 & 0x1FFFF);
        const is_self_contained = header5b & (1 << 34) != 0;

        const computed_header_crc = crc24(header5b);
        if (computed_header_crc != header_expected_crc) {
            return error.InvalidHeaderChecksum;
        }

        // Read payload and trailer
        const payload_and_trailer = try readPayloadAndTrailer(reader, allocator, compressed_length);

        // Verify compressed payload CRC32
        try computeAndVerifyCRC32(payload_and_trailer);

        // Decompress payload if necessary
        const payload = if (uncompressed_length == 0)
            payload_and_trailer.payload
        else
            try lz4.decompress(allocator, payload_and_trailer.payload, @as(usize, @intCast(uncompressed_length)));

        return .{
            .frame = .{
                .payload = payload,
                .is_self_contained = is_self_contained,
            },
            .consumed = compressed_header_size + payload_and_trailer.length(),
        };
    }

    const EncodeError = error{
        PayloadTooBig,
    } || mem.Allocator.Error;

    pub fn encode(allocator: mem.Allocator, payload: []const u8, is_self_contained: bool, format: Format) Frame.EncodeError![]const u8 {
        switch (format) {
            .uncompressed => return encodeUncompressed(allocator, payload, is_self_contained),
            .compressed => return encodeCompressed(allocator, payload, is_self_contained),
        }
    }

    fn encodeUncompressed(allocator: mem.Allocator, payload: []const u8, is_self_contained: bool) Frame.EncodeError![]const u8 {
        if (payload.len > max_payload_size) return error.PayloadTooBig;

        // Create header
        var header3b: u24 = @as(u17, @intCast(payload.len));
        if (is_self_contained) {
            header3b |= 1 << 17;
        }
        const header_crc24 = crc24(header3b);

        var buf: [uncompressed_header_size]u8 = undefined;
        mem.writeInt(u24, buf[0..3], header3b, .little);
        mem.writeInt(u24, buf[3..uncompressed_header_size], @truncate(header_crc24), .little);

        const header_data = buf[0..];

        // Compute trailer
        const payload_crc32 = computeCR32(payload);

        // Finally build the frame
        var frame_data = try std.ArrayList(u8).initCapacity(allocator, uncompressed_header_size + payload.len + trailer_size);
        defer frame_data.deinit();

        try frame_data.appendSlice(header_data);
        try frame_data.appendSlice(payload);
        try frame_data.writer().writeInt(u32, payload_crc32, .little);

        return frame_data.toOwnedSlice();
    }

    fn encodeCompressed(allocator: mem.Allocator, payload: []const u8, is_self_contained: bool) Frame.EncodeError![]const u8 {
        _ = allocator;
        _ = payload;
        _ = is_self_contained;
        return error.OutOfMemory;
    }
};

test "frame reader: QUERY message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const TestCase = struct {
        data: []const u8,
        format: Frame.Format,
    };

    const testCases = &[_]TestCase{
        .{
            .data = @embedFile("testdata/query_frame_uncompressed.bin"),
            .format = .uncompressed,
        },
        .{
            .data = @embedFile("testdata/query_frame_compressed.bin"),
            .format = .compressed,
        },
    };

    inline for (testCases) |tc| {
        const result = try Frame.decode(arena.allocator(), tc.data, tc.format);

        const frame = result.frame;
        try testing.expectEqual(@as(usize, tc.data.len), result.consumed);

        try testing.expect(frame.is_self_contained);
        try testing.expectEqual(@as(usize, 66), frame.payload.len);

        const envelope = try testReadEnvelope(arena.allocator(), frame.payload);
        try checkEnvelopeHeader(5, Opcode.query, frame.payload.len, envelope.header);

        var mr: MessageReader = undefined;
        mr.reset(envelope.body);

        const query_message = try QueryMessage.read(
            arena.allocator(),
            envelope.header.version,
            &mr,
        );

        try testing.expectEqualStrings("select * from foobar.age_to_ids ;", query_message.query);
        try testing.expectEqual(.One, query_message.query_parameters.consistency_level);
        try testing.expect(query_message.query_parameters.values == null);
    }
}

test "frame reader: QUERY message incomplete" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // Simulate reading a frame in multiple steps because the input buffer doesn't contain a complete frame
    // Also simulate reading a frame in a buffer that has more data than the frame itself.
    //
    // This is how frames will be read in the event loop.

    const frame_data = @embedFile("testdata/query_frame_compressed.bin");
    const test_data = frame_data ++ [_]u8{'z'} ** 2000;
    const test_format: Frame.Format = .compressed;

    const tmp1 = Frame.decode(arena.allocator(), test_data[0..1], test_format);
    try testing.expectError(error.UnexpectedEOF, tmp1);

    const tmp2 = Frame.decode(arena.allocator(), test_data[0..10], test_format);
    try testing.expectError(error.UnexpectedEOF, tmp2);

    const result = try Frame.decode(arena.allocator(), test_data, test_format);

    //

    const frame = result.frame;
    try testing.expectEqual(@as(usize, frame_data.len), result.consumed);

    try testing.expect(frame.is_self_contained);
    try testing.expectEqual(@as(usize, 66), frame.payload.len);

    const envelope = try testReadEnvelope(arena.allocator(), frame.payload);
    try checkEnvelopeHeader(5, Opcode.query, frame.payload.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);

    const query_message = try QueryMessage.read(
        arena.allocator(),
        envelope.header.version,
        &mr,
    );

    try testing.expectEqualStrings("select * from foobar.age_to_ids ;", query_message.query);
    try testing.expectEqual(.One, query_message.query_parameters.consistency_level);
    try testing.expect(query_message.query_parameters.values == null);
}

test "frame reader: RESULT message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mr: MessageReader = undefined;

    // The rows in the result message have the following columns.
    // The order is important !
    const MyRow = struct {
        age: u32,
        balance: std.math.big.int.Const,
        ids: []u16,
        name: []const u8,
    };

    const checkRowData = struct {
        fn do(expAge: usize, exp_balance_str: []const u8, exp_ids: []const u16, exp_name_opt: ?[]const u8, row: MyRow) !void {
            try testing.expectEqual(expAge, row.age);

            var buf: [1024]u8 = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&buf);

            var exp_balance = try std.math.big.int.Managed.init(fba.allocator());
            try exp_balance.setString(10, exp_balance_str);
            try testing.expect(exp_balance.toConst().eql(row.balance));

            try testing.expectEqualSlices(u16, exp_ids, row.ids);
            const exp_name = exp_name_opt orelse "";
            try testing.expectEqualStrings(exp_name, row.name);
        }
    }.do;

    // Uncompressed frame
    {
        const data = @embedFile("testdata/result_frame_uncompressed.bin");
        const result = try Frame.decode(arena.allocator(), data, .uncompressed);

        const frame = result.frame;
        try testing.expectEqual(@as(usize, data.len), result.consumed);

        try testing.expect(frame.is_self_contained);
        try testing.expectEqual(@as(usize, 605), frame.payload.len);

        const envelope = try testReadEnvelope(arena.allocator(), frame.payload);
        try checkEnvelopeHeader(5, Opcode.result, frame.payload.len, envelope.header);

        mr.reset(envelope.body);
        const result_message = try ResultMessage.read(
            arena.allocator(),
            envelope.header.version,
            &mr,
        );

        const rows = try collectRows(
            MyRow,
            arena.allocator(),
            result_message.result.Rows,
        );
        try testing.expectEqual(10, rows.items.len);

        try checkRowData(50, "-350956306", &[_]u16{ 0, 2, 4, 8 }, null, rows.items[0]);
        try checkRowData(10, "-350956306", &[_]u16{ 0, 2, 4, 8 }, null, rows.items[1]);
        try checkRowData(60, "40502020", &[_]u16{ 0, 2, 4, 8 }, "Vincent 6", rows.items[2]);
        try checkRowData(80, "40502020", &[_]u16{ 0, 2, 4, 8 }, "Vincent 8", rows.items[3]);
        try checkRowData(30, "-350956306", &[_]u16{ 0, 2, 4, 8 }, null, rows.items[4]);
        try checkRowData(0, "40502020", &[_]u16{ 0, 2, 4, 8 }, "Vincent 0", rows.items[5]);
        try checkRowData(20, "40502020", &[_]u16{ 0, 2, 4, 8 }, "Vincent 2", rows.items[6]);
        try checkRowData(40, "40502020", &[_]u16{ 0, 2, 4, 8 }, "Vincent 4", rows.items[7]);
        try checkRowData(70, "-350956306", &[_]u16{ 0, 2, 4, 8 }, null, rows.items[8]);
        try checkRowData(90, "-350956306", &[_]u16{ 0, 2, 4, 8 }, null, rows.items[9]);
    }

    // Compressed frame
    {
        const data = @embedFile("testdata/result_frame_compressed.bin");
        const result = try Frame.decode(arena.allocator(), data, .compressed);

        const frame = result.frame;
        try testing.expectEqual(@as(usize, data.len), result.consumed);

        try testing.expect(frame.is_self_contained);
        try testing.expectEqual(@as(usize, 5532), frame.payload.len);

        const envelope = try testReadEnvelope(arena.allocator(), frame.payload);
        try checkEnvelopeHeader(5, Opcode.result, frame.payload.len, envelope.header);

        mr.reset(envelope.body);
        const result_message = try ResultMessage.read(
            arena.allocator(),
            envelope.header.version,
            &mr,
        );

        const rows = try collectRows(
            MyRow,
            arena.allocator(),
            result_message.result.Rows,
        );
        try testing.expectEqual(100, rows.items.len);
    }
}

test "frame write: PREPARE message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const protocol_version = try ProtocolVersion.init(5);

    // Encode

    const frame = blk: {
        // Write the message to a buffer
        const message = PrepareMessage{
            .query = "SELECT 1 FROM foobar",
            .keyspace = "hello",
        };

        var mw = try MessageWriter.init(arena.allocator());
        try message.write(protocol_version, &mw);

        const message_data = mw.getWritten();

        // Create and write the envelope to a buffer
        const envelope = Envelope{
            .header = EnvelopeHeader{
                .version = protocol_version,
                .flags = 0,
                .stream = 0,
                .opcode = .prepare,
                .body_len = @intCast(message_data.len),
            },
            .body = message_data,
        };

        var envelope_writer_buffer = std.ArrayList(u8).init(arena.allocator());
        var envelope_writer = EnvelopeWriter(std.ArrayList(u8).Writer).init(envelope_writer_buffer.writer());

        try envelope_writer.write(envelope);

        // Write the frame to a buffer
        const frame_payload = try envelope_writer_buffer.toOwnedSlice();
        const frame = try Frame.encode(arena.allocator(), frame_payload, true, .uncompressed);

        break :blk frame;
    };

    // Decode then verify

    {
        const result = try Frame.decode(arena.allocator(), frame, .uncompressed);

        const envelope = try testReadEnvelope(arena.allocator(), result.frame.payload);
        try checkEnvelopeHeader(5, Opcode.prepare, result.frame.payload.len, envelope.header);

        var mr: MessageReader = undefined;
        mr.reset(envelope.body);

        const prepare_message = try PrepareMessage.read(
            arena.allocator(),
            envelope.header.version,
            &mr,
        );

        try testing.expectEqualStrings("SELECT 1 FROM foobar", prepare_message.query);
        try testing.expectEqualStrings("hello", prepare_message.keyspace.?);
    }
}

//
//
//

pub const EnvelopeFlags = struct {
    pub const Compression: u8 = 0x01;
    pub const Tracing: u8 = 0x02;
    pub const CustomPayload: u8 = 0x04;
    pub const Warning: u8 = 0x08;
    pub const UseBeta: u8 = 0x10;
};

pub const EnvelopeHeader = packed struct {
    version: ProtocolVersion,
    flags: u8,
    stream: i16,
    opcode: Opcode,
    body_len: u32,

    const size = 9;
};

pub const Envelope = struct {
    header: EnvelopeHeader,
    body: []const u8,

    pub fn deinit(self: @This(), allocator: mem.Allocator) void {
        allocator.free(self.body);
    }

    pub fn read(allocator: mem.Allocator, reader: anytype) !Envelope {
        var buf: [EnvelopeHeader.size]u8 = undefined;

        const n_header_read = try reader.readAll(&buf);
        if (n_header_read != EnvelopeHeader.size) {
            return error.UnexpectedEOF;
        }

        const header = EnvelopeHeader{
            .version = ProtocolVersion{ .version = buf[0] },
            .flags = buf[1],
            .stream = mem.readInt(i16, @ptrCast(buf[2..4]), .big),
            .opcode = @enumFromInt(buf[4]),
            .body_len = mem.readInt(u32, @ptrCast(buf[5..9]), .big),
        };

        const len = @as(usize, header.body_len);

        const body = try allocator.alloc(u8, len);
        const n_read = try reader.readAll(body);
        if (n_read != len) {
            return error.UnexpectedEOF;
        }

        return Envelope{
            .header = header,
            .body = body,
        };
    }
};

pub fn EnvelopeWriter(comptime WriterType: type) type {
    return struct {
        const Self = @This();

        writer: WriterType,

        pub fn init(out: WriterType) Self {
            return Self{
                .writer = out,
            };
        }

        pub fn write(self: *Self, envelope: Envelope) !void {
            var buf: [EnvelopeHeader.size]u8 = undefined;

            buf[0] = envelope.header.version.version;
            buf[1] = envelope.header.flags;
            mem.writeInt(i16, @ptrCast(buf[2..4]), envelope.header.stream, .big);
            buf[4] = @intFromEnum(envelope.header.opcode);
            mem.writeInt(u32, @ptrCast(buf[5..9]), envelope.header.body_len, .big);

            try self.writer.writeAll(&buf);
            try self.writer.writeAll(envelope.body);
        }
    };
}

//
//
//

// TODO(vincent): test all error codes
pub const ErrorCode = enum(u32) {
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

pub const Value = union(enum) {
    Set: []const u8,
    NotSet: void,
    Null: void,
};

/// This struct is intended to be used as a field in a struct/tuple argument
/// passed to query/execute.
///
/// For example if you have a prepared update query where you don't want to touch
/// the second field, you can pass a struct/tuple like this:
///
/// .{
///    .arg1 = ...,
///    .arg2 = NotSet{ .type = i64 },
/// }
///
/// TODO(vincent): the `type` field is necessary for now to make NotSet work but it's not great.
pub const NotSet = struct {
    type: type,
};

pub const NamedValue = struct {
    name: []const u8,
    value: Value,
};

pub const Values = union(enum) {
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

        var it = mem.splitSequence(u8, s, ".");
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

    pub fn print(self: @This(), buf: []u8) ![]u8 {
        return std.fmt.bufPrint(buf, "{d}.{d}.{d}", .{
            self.major,
            self.minor,
            self.patch,
        });
    }
};

pub const ProtocolVersion = packed struct {
    const Self = @This();

    version: u8,

    pub fn init(b: u8) !Self {
        const res = Self{
            .version = b,
        };
        if ((res.version & 0x7) < 3 or (res.version & 0x7) > 5) {
            return error.InvalidProtocolVersion;
        }
        return res;
    }

    pub fn is(self: Self, comptime version: comptime_int) bool {
        return self.version & 0x7 == @as(u8, version);
    }
    pub fn isAtLeast(self: Self, comptime version: comptime_int) bool {
        const tmp = self.version & 0x7;
        return tmp >= version;
    }
    pub fn isAtMost(self: Self, comptime version: comptime_int) bool {
        const tmp = self.version & 0x7;
        return tmp <= version;
    }
    pub fn isRequest(self: Self) bool {
        return self.version & 0x0f == self.version;
    }
    pub fn isResponse(self: Self) bool {
        return self.version & 0xf0 == 0x80;
    }

    pub fn fromString(s: []const u8) !ProtocolVersion {
        if (mem.startsWith(u8, s, "3/")) {
            return ProtocolVersion{ .version = 3 };
        } else if (mem.startsWith(u8, s, "4/")) {
            return ProtocolVersion{ .version = 4 };
        } else if (mem.startsWith(u8, s, "5/")) {
            return ProtocolVersion{ .version = 5 };
        } else if (mem.startsWith(u8, s, "6/")) {
            return ProtocolVersion{ .version = 6 };
        } else {
            return error.InvalidProtocolVersion;
        }
    }

    pub fn toString(self: Self) ![]const u8 {
        if (self.is(3)) {
            return "3/v3";
        } else if (self.is(4)) {
            return "4/v4";
        } else if (self.is(5)) {
            return "5/v5-beta";
        } else if (self.is(6)) {
            return "6/v6-beta";
        } else {
            return error.InvalidProtocolVersion;
        }
    }
};

pub const Opcode = enum(u8) {
    @"error" = 0x00,
    startup = 0x01,
    ready = 0x02,
    authenticate = 0x03,
    options = 0x05,
    supported = 0x06,
    query = 0x07,
    result = 0x08,
    prepare = 0x09,
    execute = 0x0a,
    register = 0x0b,
    event = 0x0c,
    batch = 0x0d,
    auth_challenge = 0x0e,
    auth_response = 0x0f,
    auth_success = 0x10,
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

    pub fn toString(self: @This()) []const u8 {
        return switch (self) {
            .LZ4 => "lz4",
            .Snappy => "snappy",
        };
    }
};

pub const Consistency = enum(u16) {
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

pub const BatchType = enum(u8) {
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
};

pub const OptionID = enum(u16) {
    Custom = 0x0000,
    Ascii = 0x0001,
    Bigint = 0x0002,
    Blob = 0x0003,
    Boolean = 0x0004,
    Counter = 0x0005,
    Decimal = 0x0006,
    Double = 0x0007,
    Float = 0x0008,
    Int = 0x0009,
    Timestamp = 0x000B,
    UUID = 0x000C,
    Varchar = 0x000D,
    Varint = 0x000E,
    Timeuuid = 0x000F,
    Inet = 0x0010,
    Date = 0x0011,
    Time = 0x0012,
    Smallint = 0x0013,
    Tinyint = 0x0014,
    Duration = 0x0015,
    List = 0x0020,
    Map = 0x0021,
    Set = 0x0022,
    UDT = 0x0030,
    Tuple = 0x0031,
};

test "cql version: fromString" {
    try testing.expectEqual(CQLVersion{ .major = 3, .minor = 0, .patch = 0 }, try CQLVersion.fromString("3.0.0"));
    try testing.expectEqual(CQLVersion{ .major = 3, .minor = 4, .patch = 0 }, try CQLVersion.fromString("3.4.0"));
    try testing.expectEqual(CQLVersion{ .major = 3, .minor = 5, .patch = 4 }, try CQLVersion.fromString("3.5.4"));
    try testing.expectError(error.InvalidCQLVersion, CQLVersion.fromString("1.0.0"));
}

test "protocol version: fromString" {
    try testing.expect((try ProtocolVersion.fromString("3/v3")).is(3));
    try testing.expect((try ProtocolVersion.fromString("4/v4")).is(4));
    try testing.expect((try ProtocolVersion.fromString("5/v5")).is(5));
    try testing.expect((try ProtocolVersion.fromString("5/v5-beta")).is(5));
    try testing.expectError(error.InvalidProtocolVersion, ProtocolVersion.fromString("lalal"));
}

test "protocol version: serialize and deserialize" {
    const testCase = struct {
        exp: comptime_int,
        b: [2]u8,
        err: ?anyerror,
    };

    const testCases = [_]testCase{
        testCase{
            .exp = 3,
            .b = [2]u8{ 0x03, 0x83 },
            .err = null,
        },
        testCase{
            .exp = 4,
            .b = [2]u8{ 0x04, 0x84 },
            .err = null,
        },
        testCase{
            .exp = 5,
            .b = [2]u8{ 0x05, 0x85 },
            .err = null,
        },
        testCase{
            .exp = 0,
            .b = [2]u8{ 0x00, 0x00 },
            .err = error.InvalidProtocolVersion,
        },
    };

    inline for (testCases) |tc| {
        if (tc.err) |err| {
            try testing.expectError(err, ProtocolVersion.init(tc.b[0]));
        } else {
            var v1 = try ProtocolVersion.init(tc.b[0]);
            var v2 = try ProtocolVersion.init(tc.b[1]);
            try testing.expect(v1.is(tc.exp));
            try testing.expect(v1.isRequest());
            try testing.expect(v2.isResponse());
        }
    }
}

test "compression algorith: fromString" {
    try testing.expectEqual(CompressionAlgorithm.LZ4, try CompressionAlgorithm.fromString("lz4"));
    try testing.expectEqual(CompressionAlgorithm.Snappy, try CompressionAlgorithm.fromString("snappy"));
    try testing.expectError(error.InvalidCompressionAlgorithm, CompressionAlgorithm.fromString("foobar"));
}

pub const Map = struct {
    const Self = @This();

    const MapType = std.StringHashMap([]const u8);
    const MapEntry = MapType.Entry;
    const Iterator = MapType.Iterator;

    allocator: mem.Allocator,
    map: MapType,

    pub fn init(allocator: mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .map = MapType.init(allocator),
        };
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        const result = try self.map.getOrPut(key);
        if (result.found_existing) {
            self.allocator.free(result.value_ptr.*);
        }
        result.value_ptr.* = value;
    }

    pub fn count(self: *const Self) usize {
        return self.map.count();
    }

    pub fn iterator(self: *const Self) Iterator {
        return self.map.iterator();
    }

    pub fn getEntry(self: *const Self, key: []const u8) ?MapEntry {
        return self.map.getEntry(key);
    }
};

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

const EntryList = []const []const u8;

pub const Multimap = struct {
    const Self = @This();

    const MapType = std.StringHashMap(EntryList);

    map: MapType,

    const KV = struct {
        key: []const u8,
        value: EntryList,
    };

    const Iterator = struct {
        map_it: MapType.Iterator,

        pub fn next(it: *Iterator) ?KV {
            if (it.map_it.next()) |entry| {
                return KV{
                    .key = entry.key_ptr.*,
                    .value = entry.value_ptr.*,
                };
            }

            return null;
        }
    };

    pub fn init(allocator: mem.Allocator) Self {
        return Self{
            .map = std.StringHashMap(EntryList).init(allocator),
        };
    }

    pub fn put(self: *Self, key: []const u8, values: []const []const u8) !void {
        _ = try self.map.put(key, values);
    }

    pub fn get(self: *const Self, key: []const u8) ?[]const []const u8 {
        if (self.map.getEntry(key)) |entry| {
            return entry.value_ptr.*;
        } else {
            return null;
        }
    }

    pub fn count(self: *const Self) usize {
        return self.map.count();
    }

    pub fn iterator(self: *const Self) Iterator {
        return Iterator{
            .map_it = self.map.iterator(),
        };
    }
};

test "map" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

    var m = Map.init(allocator);

    {
        const k1 = try allocator.dupe(u8, "foo");
        const k2 = try allocator.dupe(u8, "bar");

        const v1 = try allocator.dupe(u8, "bar");
        const v2 = try allocator.dupe(u8, "heo");
        const v3 = try allocator.dupe(u8, "baz");

        _ = try m.put(k1, v1);
        _ = try m.put(k1, v2);
        _ = try m.put(k2, v3);
    }

    try testing.expectEqual(@as(usize, 2), m.count());

    try testing.expectEqualStrings("heo", m.getEntry("foo").?.value_ptr.*);
    try testing.expectEqualStrings("baz", m.getEntry("bar").?.value_ptr.*);
}

test "multimap" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

    var m = Multimap.init(allocator);

    {
        const k1 = try allocator.dupe(u8, "foo");
        const v1 = &[_][]const u8{ "bar", "baz" };
        _ = try m.put(k1, v1);
    }

    {
        const k2 = try allocator.dupe(u8, "fou");
        const v2 = &[_][]const u8{ "fb", "ha" };
        _ = try m.put(k2, v2);
    }

    try testing.expectEqual(@as(usize, 2), m.count());

    var it = m.iterator();
    while (it.next()) |entry| {
        try testing.expect(std.mem.eql(u8, "foo", entry.key) or std.mem.eql(u8, "fou", entry.key));

        const slice = entry.value;
        if (std.mem.eql(u8, "foo", entry.key)) {
            try testing.expectEqualStrings("bar", slice[0]);
            try testing.expectEqualStrings("baz", slice[1]);
        } else if (std.mem.eql(u8, "fou", entry.key)) {
            try testing.expectEqualStrings("fb", slice[0]);
            try testing.expectEqualStrings("ha", slice[1]);
        }
    }

    const slice = m.get("foo").?;
    try testing.expectEqualStrings("bar", slice[0]);
    try testing.expectEqualStrings("baz", slice[1]);

    const slice2 = m.get("fou").?;
    try testing.expectEqualStrings("fb", slice2[0]);
    try testing.expectEqualStrings("ha", slice2[1]);
}

pub const MessageReader = struct {
    const Self = @This();

    buffer: io.FixedBufferStream([]const u8),
    reader: io.FixedBufferStream([]const u8).Reader,

    pub fn reset(self: *Self, buf: []const u8) void {
        self.buffer = io.fixedBufferStream(buf);
        self.reader = self.buffer.reader();
    }

    /// Read either a short, a int or a long from the buffer.
    pub fn readInt(self: *Self, comptime T: type) !T {
        return self.reader.readInt(T, .big);
    }

    /// Read a single byte from the buffer.
    pub fn readByte(self: *Self) !u8 {
        return self.reader.readByte();
    }

    /// Read a length-prefixed byte slice from the stream. The length is 2 bytes.
    /// The slice can be null.
    pub fn readShortBytes(self: *Self, allocator: mem.Allocator) !?[]const u8 {
        return self.readBytesGeneric(allocator, i16);
    }

    /// Read a length-prefixed byte slice from the stream. The length is 4 bytes.
    /// The slice can be null.
    pub fn readBytes(self: *Self, allocator: mem.Allocator) !?[]const u8 {
        return self.readBytesGeneric(allocator, i32);
    }

    /// Read bytes from the stream in a generic way.
    fn readBytesGeneric(self: *Self, allocator: mem.Allocator, comptime LenType: type) !?[]const u8 {
        const len = try self.readInt(LenType);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return &[_]u8{};
        }

        const buf = try allocator.alloc(u8, @as(usize, @intCast(len)));

        const n_read = try self.reader.readAll(buf);
        if (n_read != len) {
            return error.UnexpectedEOF;
        }

        return buf;
    }

    /// Read a length-prefixed string from the stream. The length is 2 bytes.
    /// The string can't be null.
    pub fn readString(self: *Self, allocator: mem.Allocator) ![]const u8 {
        if (try self.readBytesGeneric(allocator, i16)) |v| {
            return v;
        } else {
            return error.UnexpectedEOF;
        }
    }

    /// Read a length-prefixed string from the stream. The length is 4 bytes.
    /// The string can't be null.
    pub fn readLongString(self: *Self, allocator: mem.Allocator) ![]const u8 {
        if (try self.readBytesGeneric(allocator, i32)) |v| {
            return v;
        } else {
            return error.UnexpectedEOF;
        }
    }

    /// Read a UUID from the stream.
    pub fn readUUID(self: *Self) ![16]u8 {
        var buf: [16]u8 = undefined;
        _ = try self.reader.readAll(&buf);
        return buf;
    }

    /// Read a list of string from the stream.
    pub fn readStringList(self: *Self, allocator: mem.Allocator) ![]const []const u8 {
        const len = @as(usize, try self.readInt(u16));

        var list = try std.ArrayList([]const u8).initCapacity(allocator, len);

        var i: usize = 0;
        while (i < len) {
            const tmp = try self.readString(allocator);
            try list.append(tmp);

            i += 1;
        }

        return try list.toOwnedSlice();
    }

    /// Read a value from the stream.
    pub fn readValue(self: *Self, allocator: mem.Allocator) !Value {
        const len = try self.readInt(i32);

        if (len >= 0) {
            const result = try allocator.alloc(u8, @intCast(len));
            _ = try self.reader.readAll(result);

            return Value{ .Set = result };
        } else if (len == -1) {
            return Value.Null;
        } else if (len == -2) {
            return Value.NotSet;
        } else {
            return error.InvalidValueLength;
        }
    }

    pub fn readUnsignedVint(self: *Self, comptime IntType: type) !IntType {
        const bits = switch (@typeInfo(IntType)) {
            .int => |info| blk: {
                comptime assert(info.bits >= 16);
                break :blk info.bits;
            },
            else => unreachable,
        };

        var shift: std.meta.Int(.unsigned, std.math.log2(bits)) = 0;

        var res: IntType = 0;

        while (true) {
            const b = try self.reader.readByte();
            const tmp = @as(IntType, @intCast(b)) & ~@as(IntType, 0x80);

            // TODO(vincent): runtime check if the number will actually fit in the type T ?

            res |= (tmp << shift);

            if (b & 0x80 == 0) {
                break;
            }

            shift += 7;
        }

        return res;
    }

    pub fn readVint(self: *Self, comptime IntType: type) !IntType {
        const bits = switch (@typeInfo(IntType)) {
            .int => |info| blk: {
                comptime assert(info.signedness == .signed);
                comptime assert(info.bits >= 16);

                break :blk info.bits;
            },
            else => unreachable,
        };

        const UnsignedType = std.meta.Int(.unsigned, bits);

        const tmp = try self.readUnsignedVint(UnsignedType);

        const lhs: IntType = @intCast(tmp >> 1);
        const rhs = -@as(IntType, @intCast(tmp & 1));

        const n = lhs ^ rhs;

        return n;
    }

    pub inline fn readInetaddr(self: *Self) !net.Address {
        return self.readInetGeneric(false);
    }
    pub inline fn readInet(self: *Self) !net.Address {
        return self.readInetGeneric(true);
    }

    fn readInetGeneric(self: *Self, comptime with_port: bool) !net.Address {
        const n = try self.readByte();

        return switch (n) {
            4 => {
                var buf: [4]u8 = undefined;
                _ = try self.reader.readAll(&buf);

                const port = if (with_port) try self.readInt(i32) else 0;

                return net.Address.initIp4(buf, @intCast(port));
            },
            16 => {
                var buf: [16]u8 = undefined;
                _ = try self.reader.readAll(&buf);

                const port = if (with_port) try self.readInt(i32) else 0;

                return net.Address.initIp6(buf, @intCast(port), 0, 0);
            },
            else => return error.InvalidInetSize,
        };
    }

    pub fn readConsistency(self: *Self) !Consistency {
        const n = try self.readInt(u16);
        return @enumFromInt(n);
    }

    pub fn readStringMap(self: *Self, allocator: mem.Allocator) !Map {
        const n = try self.readInt(u16);

        var map = Map.init(allocator);

        var i: usize = 0;
        while (i < n) : (i += 1) {
            const k = try self.readString(allocator);
            const v = try self.readString(allocator);

            _ = try map.put(k, v);
        }

        return map;
    }

    pub fn readStringMultimap(self: *Self, allocator: mem.Allocator) !Multimap {
        const n = try self.readInt(u16);

        var map = Multimap.init(allocator);

        var i: usize = 0;
        while (i < n) : (i += 1) {
            const k = try self.readString(allocator);
            const list = try self.readStringList(allocator);

            _ = try map.put(k, list);
        }

        return map;
    }
};

test "message reader: read int" {
    var mr: MessageReader = undefined;
    mr.reset("\x00\x20\x11\x00");
    try testing.expectEqual(@as(i32, 2101504), try mr.readInt(i32));

    mr.reset("\x00\x00\x40\x00\x00\x20\x11\x00");
    try testing.expectEqual(@as(i64, 70368746279168), try mr.readInt(i64));

    mr.reset("\x11\x00");
    try testing.expectEqual(@as(u16, 4352), try mr.readInt(u16));

    mr.reset("\xff");
    try testing.expectEqual(@as(u8, 0xFF), try mr.readByte());
}

test "message reader: read strings and bytes" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mr: MessageReader = undefined;
    {
        // short string
        mr.reset("\x00\x06foobar");
        try testing.expectEqualStrings("foobar", try mr.readString(arena.allocator()));

        // long string
        mr.reset("\x00\x00\x00\x06foobar");
        try testing.expectEqualStrings("foobar", try mr.readLongString(arena.allocator()));
    }

    {
        // int32 + bytes
        mr.reset("\x00\x00\x00\x0A123456789A");
        try testing.expectEqualStrings("123456789A", (try mr.readBytes(arena.allocator())).?);

        mr.reset("\x00\x00\x00\x00");
        try testing.expectEqualStrings("", (try mr.readBytes(arena.allocator())).?);

        mr.reset("\xff\xff\xff\xff");
        try testing.expect((try mr.readBytes(arena.allocator())) == null);
    }

    {
        // int16 + bytes
        mr.reset("\x00\x0A123456789A");
        try testing.expectEqualStrings("123456789A", (try mr.readShortBytes(arena.allocator())).?);

        mr.reset("\x00\x00");
        try testing.expectEqualStrings("", (try mr.readShortBytes(arena.allocator())).?);

        mr.reset("\xff\xff");
        try testing.expect((try mr.readShortBytes(arena.allocator())) == null);
    }
}

test "message reader: read uuid" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var uuid: [16]u8 = undefined;
    try std.posix.getrandom(&uuid);

    var mr: MessageReader = undefined;
    mr.reset(&uuid);

    try testing.expectEqualSlices(u8, &uuid, &(try mr.readUUID()));
}

test "message reader: read string list" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mr: MessageReader = undefined;
    mr.reset("\x00\x02\x00\x03foo\x00\x03bar");

    const result = try mr.readStringList(arena.allocator());
    try testing.expectEqual(@as(usize, 2), result.len);

    var tmp = result[0];
    try testing.expectEqualStrings("foo", tmp);

    tmp = result[1];
    try testing.expectEqualStrings("bar", tmp);
}

test "message reader: read value" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // Normal value
    var mr: MessageReader = undefined;
    mr.reset("\x00\x00\x00\x02\x61\x62");

    const value = try mr.readValue(arena.allocator());
    try testing.expect(value == .Set);
    try testing.expectEqualStrings("ab", value.Set);

    // Null value

    mr.reset("\xff\xff\xff\xff");
    const value2 = try mr.readValue(arena.allocator());
    try testing.expect(value2 == .Null);

    // "Not set" value

    mr.reset("\xff\xff\xff\xfe");
    const value3 = try mr.readValue(arena.allocator());
    try testing.expect(value3 == .NotSet);
}

test "read unsigned vint" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mw = try MessageWriter.init(arena.allocator());
    try mw.writeUnsignedVint(@as(u64, 282240));
    try mw.writeUnsignedVint(@as(u32, 140022));
    try mw.writeUnsignedVint(@as(u16, 24450));

    var mr: MessageReader = undefined;
    mr.reset(mw.getWritten());

    const n1 = try mr.readUnsignedVint(u64);
    try testing.expect(n1 == @as(u64, 282240));
    const n2 = try mr.readUnsignedVint(u32);
    try testing.expect(n2 == @as(u32, 140022));
    const n3 = try mr.readUnsignedVint(u16);
    try testing.expect(n3 == @as(u16, 24450));
}

test "read vint" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mw = try MessageWriter.init(arena.allocator());
    try mw.writeVint(@as(i64, 282240));
    try mw.writeVint(@as(i64, -2400));
    try mw.writeVint(@as(i32, -38000));
    try mw.writeVint(@as(i32, 80000000));

    var mr: MessageReader = undefined;
    mr.reset(mw.getWritten());

    const n1 = try mr.readVint(i64);
    try testing.expect(n1 == @as(i64, 282240));
    const n2 = try mr.readVint(i64);
    try testing.expect(n2 == @as(i64, -2400));
    const n3 = try mr.readVint(i32);
    try testing.expect(n3 == @as(i32, -38000));
    const n4 = try mr.readVint(i32);
    try testing.expect(n4 == @as(i32, 80000000));
}

test "message reader: read inet and inetaddr" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mr: MessageReader = undefined;

    // IPv4
    mr.reset("\x04\x12\x34\x56\x78\x00\x00\x00\x22");

    var result = try mr.readInet();
    try testing.expectEqual(@as(u16, std.posix.AF.INET), result.any.family);
    try testing.expectEqual(@as(u32, 0x78563412), result.in.sa.addr);
    try testing.expectEqual(@as(u16, 34), result.getPort());

    // IPv6
    mr.reset("\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x22");

    result = try mr.readInet();
    try testing.expectEqual(@as(u16, std.posix.AF.INET6), result.any.family);
    try testing.expectEqualSlices(u8, &[_]u8{0xff} ** 16, &result.in6.sa.addr);
    try testing.expectEqual(@as(u16, 34), result.getPort());

    // IPv4 without port
    mr.reset("\x04\x12\x34\x56\x78");

    result = try mr.readInetaddr();
    try testing.expectEqual(@as(u16, std.posix.AF.INET), result.any.family);
    try testing.expectEqual(@as(u32, 0x78563412), result.in.sa.addr);
    try testing.expectEqual(@as(u16, 0), result.getPort());

    // IPv6 without port
    mr.reset("\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff");

    result = try mr.readInetaddr();
    try testing.expectEqual(@as(u16, std.posix.AF.INET6), result.any.family);
    try testing.expectEqualSlices(u8, &[_]u8{0xff} ** 16, &result.in6.sa.addr);
    try testing.expectEqual(@as(u16, 0), result.getPort());
}

test "message reader: read consistency" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

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
        var mr: MessageReader = undefined;
        mr.reset(tc.b);

        const result = try mr.readConsistency();
        try testing.expectEqual(tc.exp, result);
    }
}

test "message reader: read stringmap" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // 2 elements string map

    var mr: MessageReader = undefined;
    mr.reset("\x00\x02\x00\x03foo\x00\x03baz\x00\x03bar\x00\x03baz");

    var result = try mr.readStringMap(arena.allocator());
    try testing.expectEqual(@as(usize, 2), result.count());

    var it = result.iterator();
    while (it.next()) |entry| {
        try testing.expect(std.mem.eql(u8, "foo", entry.key_ptr.*) or std.mem.eql(u8, "bar", entry.key_ptr.*));
        try testing.expectEqualStrings("baz", entry.value_ptr.*);
    }
}

test "message reader: read string multimap" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // 1 key, 2 values multimap

    var mr: MessageReader = undefined;
    mr.reset("\x00\x01\x00\x03foo\x00\x02\x00\x03bar\x00\x03baz");

    var result = try mr.readStringMultimap(arena.allocator());
    try testing.expectEqual(@as(usize, 1), result.count());

    const slice = result.get("foo").?;
    try testing.expectEqualStrings("bar", slice[0]);
    try testing.expectEqualStrings("baz", slice[1]);
}

pub const MessageWriter = struct {
    const Self = @This();

    wbuf: std.ArrayList(u8),

    pub fn init(allocator: mem.Allocator) !Self {
        return Self{
            .wbuf = try std.ArrayList(u8).initCapacity(
                allocator,
                1024,
            ),
        };
    }

    pub fn deinit(self: *Self) void {
        self.wbuf.deinit();
    }

    pub fn reset(self: *Self) void {
        self.wbuf.clearAndFree();
    }

    pub fn toOwnedSlice(self: *Self) ![]u8 {
        return self.wbuf.toOwnedSlice();
    }

    pub fn getWritten(self: *Self) []u8 {
        return self.wbuf.items;
    }

    /// Write either a short, a int or a long to the buffer.
    pub fn writeInt(self: *Self, comptime T: type, value: T) !void {
        var buf: [(@typeInfo(T).int.bits + 7) / 8]u8 = undefined;
        mem.writeInt(T, &buf, value, .big);

        return self.wbuf.appendSlice(&buf);
    }

    /// Write a byte to the buffer.
    pub fn writeByte(self: *Self, value: u8) !void {
        return self.wbuf.append(value);
    }

    /// Write a length-prefixed byte slice to the buffer. The length is 2 bytes.
    /// The slice can be null.
    pub fn writeShortBytes(self: *Self, value: ?[]const u8) !void {
        return self.writeBytesGeneric(i16, value);
    }

    /// Write a length-prefixed byte slice to the buffer. The length is 4 bytes.
    /// The slice can be null.
    pub fn writeBytes(self: *Self, value: ?[]const u8) !void {
        return self.writeBytesGeneric(i32, value);
    }

    /// Write bytes from the stream in a generic way.
    fn writeBytesGeneric(self: *Self, comptime LenType: type, value: ?[]const u8) !void {
        if (value) |v| {
            try self.writeInt(LenType, @intCast(v.len));
            return self.wbuf.appendSlice(v);
        } else {
            try self.writeInt(LenType, @intCast(-1));
        }
    }

    /// Write a length-prefixed string to the buffer. The length is 2 bytes.
    /// The string can't be null.
    pub fn writeString(self: *Self, value: []const u8) !void {
        return self.writeBytesGeneric(i16, value);
    }

    /// Write a length-prefixed string to the buffer. The length is 4 bytes.
    /// The string can't be null.
    pub fn writeLongString(self: *Self, value: []const u8) !void {
        return self.writeBytesGeneric(i32, value);
    }

    /// Write a UUID to the buffer.
    pub fn writeUUID(self: *Self, uuid: [16]u8) !void {
        return self.wbuf.appendSlice(&uuid);
    }

    /// Write a list of string to the buffer.
    pub fn writeStringList(self: *Self, list: []const []const u8) !void {
        try self.writeInt(u16, @intCast(list.len));
        for (list) |value| {
            _ = try self.writeString(value);
        }
    }

    /// Write a value to the buffer.
    pub fn writeValue(self: *Self, value: Value) !void {
        return switch (value) {
            .Null => self.writeInt(i32, @as(i32, -1)),
            .NotSet => self.writeInt(i32, @as(i32, -2)),
            .Set => |data| {
                try self.writeInt(i32, @intCast(data.len));
                return self.wbuf.appendSlice(data);
            },
        };
    }

    pub fn writeUnsignedVint(self: *Self, n: anytype) !void {
        switch (@typeInfo(@TypeOf(n))) {
            .int => |info| {
                comptime assert(info.signedness == .unsigned);
            },
            else => unreachable,
        }

        //

        var tmp = n;

        var tmp_buf = [_]u8{0} ** 9;
        var i: usize = 0;

        // split into 7 bits chunks with the most significant bit set when there are more bytes to read.
        //
        // 0x80 == 128 == 0b1000_0000
        // If the number is greater than or equal to that, it must be encoded as a chunk.

        while (tmp >= 0x80) {
            // a chunk is:
            // * the least significant 7 bits
            // * the most significant  bit set to 1
            tmp_buf[i] = @as(u8, @truncate(tmp & 0x7F)) | 0x80;
            tmp >>= 7;
            i += 1;
        }

        // the remaining chunk that is less than 128. The most significant bit must not be set.
        tmp_buf[i] = @truncate(tmp);

        try self.wbuf.appendSlice(tmp_buf[0 .. i + 1]);
    }

    pub fn writeVint(self: *Self, n: anytype) !void {
        const bits = switch (@typeInfo(@TypeOf(n))) {
            .int => |info| blk: {
                comptime assert(info.bits == 32 or info.bits == 64);
                comptime assert(info.signedness == .signed);

                break :blk info.bits;
            },
            else => unreachable,
        };

        const UnsignedType = std.meta.Int(.unsigned, bits);

        const lhs: UnsignedType = @bitCast(n >> (bits - 1));
        const rhs: UnsignedType = @bitCast(n << 1);

        const tmp = lhs ^ rhs;

        try self.writeUnsignedVint(tmp);
    }

    pub inline fn writeInetaddr(self: *Self, inet: net.Address) !void {
        return self.writeInetGeneric(inet, false);
    }

    pub inline fn writeInet(self: *Self, inet: net.Address) !void {
        return self.writeInetGeneric(inet, true);
    }

    /// Write a net address to the buffer.
    fn writeInetGeneric(self: *Self, inet: net.Address, comptime with_port: bool) !void {
        switch (inet.any.family) {
            std.posix.AF.INET => {
                if (with_port) {
                    var buf: [9]u8 = undefined;
                    buf[0] = 4;
                    mem.writeInt(u32, buf[1..5], inet.in.sa.addr, .little);
                    mem.writeInt(u32, buf[5..9], inet.getPort(), .big);
                    return self.wbuf.appendSlice(&buf);
                } else {
                    var buf: [5]u8 = undefined;
                    buf[0] = 4;
                    mem.writeInt(u32, buf[1..5], inet.in.sa.addr, .little);
                    return self.wbuf.appendSlice(&buf);
                }
            },
            std.posix.AF.INET6 => {
                if (with_port) {
                    var buf: [21]u8 = undefined;
                    buf[0] = 16;
                    mem.copyForwards(u8, buf[1..17], &inet.in6.sa.addr);
                    mem.writeInt(u32, buf[17..21], inet.getPort(), .big);
                    return self.wbuf.appendSlice(&buf);
                } else {
                    var buf: [17]u8 = undefined;
                    buf[0] = 16;
                    mem.copyForwards(u8, buf[1..17], &inet.in6.sa.addr);
                    return self.wbuf.appendSlice(&buf);
                }
            },
            else => |af| std.debug.panic("invalid address family {}\n", .{af}),
        }
    }

    pub fn writeConsistency(self: *Self, consistency: Consistency) !void {
        const n: u16 = @intCast(@intFromEnum(consistency));
        return self.writeInt(u16, n);
    }

    pub fn startStringMap(self: *Self, size: usize) !void {
        _ = try self.writeInt(u16, @intCast(size));
    }
};

test "message writer: write int" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mw = try MessageWriter.init(arena.allocator());

    try mw.writeInt(i32, 2101504);
    try testing.expectEqualSlices(u8, "\x00\x20\x11\x00", mw.getWritten()[0..4]);

    try mw.writeInt(i64, 70368746279168);
    try testing.expectEqualSlices(u8, "\x00\x00\x40\x00\x00\x20\x11\x00", mw.getWritten()[4..12]);

    try mw.writeInt(i16, 4352);
    try testing.expectEqualSlices(u8, "\x11\x00", mw.getWritten()[12..14]);

    try mw.writeByte(0xFF);
    try testing.expectEqualSlices(u8, "\xff", mw.getWritten()[14..15]);
}

test "message writer: write strings and bytes" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mw = try MessageWriter.init(arena.allocator());

    {
        // short string
        _ = try mw.writeString("foobar");
        try testing.expectEqualSlices(u8, "\x00\x06foobar", mw.getWritten()[0..8]);

        // long string

        _ = try mw.writeLongString("foobar");
        try testing.expectEqualSlices(u8, "\x00\x00\x00\x06foobar", mw.getWritten()[8..18]);
    }

    {
        // int32 + bytes
        _ = try mw.writeBytes("123456789A");
        try testing.expectEqualSlices(u8, "\x00\x00\x00\x0A123456789A", mw.getWritten()[18..32]);

        _ = try mw.writeBytes("");
        try testing.expectEqualSlices(u8, "\x00\x00\x00\x00", mw.getWritten()[32..36]);

        _ = try mw.writeBytes(null);
        try testing.expectEqualSlices(u8, "\xff\xff\xff\xff", mw.getWritten()[36..40]);
    }

    {
        // int16 + bytes
        _ = try mw.writeShortBytes("123456789A");
        try testing.expectEqualSlices(u8, "\x00\x0A123456789A", mw.getWritten()[40..52]);

        _ = try mw.writeShortBytes("");
        try testing.expectEqualSlices(u8, "\x00\x00", mw.getWritten()[52..54]);

        _ = try mw.writeShortBytes(null);
        try testing.expectEqualSlices(u8, "\xff\xff", mw.getWritten()[54..56]);
    }
}

test "message writer: write uuid" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mw = try MessageWriter.init(arena.allocator());

    var uuid: [16]u8 = undefined;
    try std.posix.getrandom(&uuid);

    _ = try mw.writeUUID(uuid);
    try testing.expectEqualSlices(u8, &uuid, mw.getWritten()[0..16]);
}

test "message writer: write string list" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mw = try MessageWriter.init(arena.allocator());

    const list = &[_][]const u8{ "foo", "bar" };

    _ = try mw.writeStringList(list);
    try testing.expectEqualSlices(u8, "\x00\x02\x00\x03foo\x00\x03bar", mw.getWritten()[0..12]);
}

test "message writer: write value" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mw = try MessageWriter.init(arena.allocator());

    // Normal value
    _ = try mw.writeValue(Value{ .Set = "ab" });
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x02\x61\x62", mw.getWritten()[0..6]);

    // Null value
    _ = try mw.writeValue(Value{ .Null = {} });
    try testing.expectEqualSlices(u8, "\xff\xff\xff\xff", mw.getWritten()[6..10]);

    // "Not set" value
    _ = try mw.writeValue(Value{ .NotSet = {} });
    try testing.expectEqualSlices(u8, "\xff\xff\xff\xfe", mw.getWritten()[10..14]);
}

test "message writer: write inet and inetaddr" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mw = try MessageWriter.init(arena.allocator());

    // IPv4
    _ = try mw.writeInet(net.Address.initIp4([_]u8{ 0x78, 0x56, 0x34, 0x12 }, 34));
    try testing.expectEqualSlices(u8, "\x04\x78\x56\x34\x12\x00\x00\x00\x22", mw.getWritten()[0..9]);

    // IPv6
    _ = try mw.writeInet(net.Address.initIp6([_]u8{0xff} ** 16, 34, 0, 0));
    try testing.expectEqualSlices(u8, "\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x22", mw.getWritten()[9..30]);

    // IPv4 without port
    _ = try mw.writeInetaddr(net.Address.initIp4([_]u8{ 0x78, 0x56, 0x34, 0x12 }, 34));
    try testing.expectEqualSlices(u8, "\x04\x78\x56\x34\x12", mw.getWritten()[30..35]);

    // IPv6 without port
    _ = try mw.writeInetaddr(net.Address.initIp6([_]u8{0xff} ** 16, 34, 0, 0));
    try testing.expectEqualSlices(u8, "\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", mw.getWritten()[35..52]);
}

test "message writer: write consistency" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var mw = try MessageWriter.init(arena.allocator());

    const testCase = struct {
        consistency: Consistency,
        exp: []const u8,
    };

    const testCases = [_]testCase{
        testCase{ .consistency = Consistency.Any, .exp = "\x00\x00" },
        testCase{ .consistency = Consistency.One, .exp = "\x00\x01" },
        testCase{ .consistency = Consistency.Two, .exp = "\x00\x02" },
        testCase{ .consistency = Consistency.Three, .exp = "\x00\x03" },
        testCase{ .consistency = Consistency.Quorum, .exp = "\x00\x04" },
        testCase{ .consistency = Consistency.All, .exp = "\x00\x05" },
        testCase{ .consistency = Consistency.LocalQuorum, .exp = "\x00\x06" },
        testCase{ .consistency = Consistency.EachQuorum, .exp = "\x00\x07" },
        testCase{ .consistency = Consistency.Serial, .exp = "\x00\x08" },
        testCase{ .consistency = Consistency.LocalSerial, .exp = "\x00\x09" },
        testCase{ .consistency = Consistency.LocalOne, .exp = "\x00\x0A" },
    };

    for (testCases) |tc| {
        mw.reset();

        _ = try mw.writeConsistency(tc.consistency);
        try testing.expectEqualSlices(u8, tc.exp, mw.getWritten()[0..2]);
    }
}

/// ColumnData is a wrapper around a slice of bytes.
pub const ColumnData = struct {
    slice: []const u8,
};

/// RowData is a wrapper around a slice of ColumnData.
pub const RowData = struct {
    slice: []const ColumnData,
};

fn checkEnvelopeHeader(version: comptime_int, opcode: Opcode, input_len: usize, header: EnvelopeHeader) !void {
    try testing.expect(header.version.is(version));
    // Don't care about the flags here
    // Don't care about the stream
    try testing.expectEqual(opcode, header.opcode);
    try testing.expectEqual(input_len - EnvelopeHeader.size, @as(usize, header.body_len));
}

test "envelope header: read and write" {
    const exp = "\x04\x00\x00\xd7\x05\x00\x00\x00\x00";
    var fbs = io.fixedBufferStream(exp);

    // deserialize the header

    const envelope = try Envelope.read(testing.allocator, fbs.reader());
    const header = envelope.header;

    try testing.expect(header.version.is(4));
    try testing.expect(header.version.isRequest());
    try testing.expectEqual(@as(u8, 0), header.flags);
    try testing.expectEqual(@as(i16, 215), header.stream);
    try testing.expectEqual(Opcode.options, header.opcode);
    try testing.expectEqual(@as(u32, 0), header.body_len);
    try testing.expectEqual(@as(usize, 0), exp.len - EnvelopeHeader.size);
}

//
// All messages defined in https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec#L39-L62
//

/// ERROR is sent by a node if there's an error processing a request.
///
/// Described in the protocol spec at §4.2.1.
pub const ErrorMessage = struct {
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

    pub fn read(allocator: mem.Allocator, br: *MessageReader) !Self {
        var message = Self{
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

        message.error_code = @enumFromInt(try br.readInt(u32));
        message.message = try br.readString(allocator);

        switch (message.error_code) {
            .UnavailableReplicas => {
                message.unavailable_replicas = UnavailableReplicasError{
                    .consistency_level = try br.readConsistency(),
                    .required = try br.readInt(u32),
                    .alive = try br.readInt(u32),
                };
            },
            .FunctionFailure => {
                message.function_failure = FunctionFailureError{
                    .keyspace = try br.readString(allocator),
                    .function = try br.readString(allocator),
                    .arg_types = try br.readStringList(allocator),
                };
            },
            .WriteTimeout => {
                var write_timeout = WriteError.Timeout{
                    .consistency_level = try br.readConsistency(),
                    .received = try br.readInt(u32),
                    .block_for = try br.readInt(u32),
                    .write_type = undefined,
                    .contentions = null,
                };

                const write_type_string = try br.readString(allocator);
                defer allocator.free(write_type_string);

                write_timeout.write_type = std.meta.stringToEnum(WriteError.WriteType, write_type_string) orelse return error.InvalidWriteType;
                if (write_timeout.write_type == .CAS) {
                    write_timeout.contentions = try br.readInt(u16);
                }

                message.write_timeout = write_timeout;
            },
            .ReadTimeout => {
                message.read_timeout = ReadError.Timeout{
                    .consistency_level = try br.readConsistency(),
                    .received = try br.readInt(u32),
                    .block_for = try br.readInt(u32),
                    .data_present = try br.readByte(),
                };
            },
            .WriteFailure => {
                var write_failure = WriteError.Failure{
                    .consistency_level = try br.readConsistency(),
                    .received = try br.readInt(u32),
                    .block_for = try br.readInt(u32),
                    .reason_map = undefined,
                    .write_type = undefined,
                };

                // Read reason map
                // TODO(vincent): this is only correct for Protocol v5
                // See point 10. in the the protocol v5 spec.

                var reason_map = std.ArrayList(WriteError.Failure.Reason).init(allocator);
                errdefer reason_map.deinit();

                const n = try br.readInt(u32);
                var i: usize = 0;
                while (i < n) : (i += 1) {
                    const reason = WriteError.Failure.Reason{
                        .endpoint = try br.readInetaddr(),
                        .failure_code = try br.readInt(u16),
                    };
                    _ = try reason_map.append(reason);
                }
                write_failure.reason_map = try reason_map.toOwnedSlice();

                // Read the rest

                const write_type_string = try br.readString(allocator);
                defer allocator.free(write_type_string);

                write_failure.write_type = std.meta.stringToEnum(WriteError.WriteType, write_type_string) orelse return error.InvalidWriteType;

                message.write_failure = write_failure;
            },
            .ReadFailure => {
                var read_failure = ReadError.Failure{
                    .consistency_level = try br.readConsistency(),
                    .received = try br.readInt(u32),
                    .block_for = try br.readInt(u32),
                    .reason_map = undefined,
                    .data_present = undefined,
                };

                // Read reason map

                var reason_map = std.ArrayList(ReadError.Failure.Reason).init(allocator);
                errdefer reason_map.deinit();

                const n = try br.readInt(u32);
                var i: usize = 0;
                while (i < n) : (i += 1) {
                    const reason = ReadError.Failure.Reason{
                        .endpoint = try br.readInetaddr(),
                        .failure_code = try br.readInt(u16),
                    };
                    _ = try reason_map.append(reason);
                }
                read_failure.reason_map = try reason_map.toOwnedSlice();

                // Read the rest

                read_failure.data_present = try br.readByte();

                message.read_failure = read_failure;
            },
            .CASWriteUnknown => {
                message.cas_write_unknown = WriteError.CASUnknown{
                    .consistency_level = try br.readConsistency(),
                    .received = try br.readInt(u32),
                    .block_for = try br.readInt(u32),
                };
            },
            .AlreadyExists => {
                message.already_exists = AlreadyExistsError{
                    .keyspace = try br.readString(allocator),
                    .table = try br.readString(allocator),
                };
            },
            .Unprepared => {
                if (try br.readShortBytes(allocator)) |statement_id| {
                    message.unprepared = UnpreparedError{
                        .statement_id = statement_id,
                    };
                } else {
                    // TODO(vincent): make this a proper error ?
                    return error.InvalidStatementID;
                }
            },
            else => {},
        }

        return message;
    }
};

test "error message: invalid query, no keyspace specified" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x02\x00\x00\x00\x00\x5e\x00\x00\x22\x00\x00\x58\x4e\x6f\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x20\x68\x61\x73\x20\x62\x65\x65\x6e\x20\x73\x70\x65\x63\x69\x66\x69\x65\x64\x2e\x20\x55\x53\x45\x20\x61\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x2c\x20\x6f\x72\x20\x65\x78\x70\x6c\x69\x63\x69\x74\x6c\x79\x20\x73\x70\x65\x63\x69\x66\x79\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x2e\x74\x61\x62\x6c\x65\x6e\x61\x6d\x65";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.@"error", data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try ErrorMessage.read(arena.allocator(), &mr);

    try testing.expectEqual(ErrorCode.InvalidQuery, message.error_code);
    try testing.expectEqualStrings("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename", message.message);
}

test "error message: already exists" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x23\x00\x00\x00\x00\x53\x00\x00\x24\x00\x00\x3e\x43\x61\x6e\x6e\x6f\x74\x20\x61\x64\x64\x20\x61\x6c\x72\x65\x61\x64\x79\x20\x65\x78\x69\x73\x74\x69\x6e\x67\x20\x74\x61\x62\x6c\x65\x20\x22\x68\x65\x6c\x6c\x6f\x22\x20\x74\x6f\x20\x6b\x65\x79\x73\x70\x61\x63\x65\x20\x22\x66\x6f\x6f\x62\x61\x72\x22\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x05\x68\x65\x6c\x6c\x6f";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.@"error", data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try ErrorMessage.read(arena.allocator(), &mr);

    try testing.expectEqual(ErrorCode.AlreadyExists, message.error_code);
    try testing.expectEqualStrings("Cannot add already existing table \"hello\" to keyspace \"foobar\"", message.message);
    const already_exists_error = message.already_exists.?;
    try testing.expectEqualStrings("foobar", already_exists_error.keyspace);
    try testing.expectEqualStrings("hello", already_exists_error.table);
}

test "error message: syntax error" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x2f\x00\x00\x00\x00\x41\x00\x00\x20\x00\x00\x3b\x6c\x69\x6e\x65\x20\x32\x3a\x30\x20\x6d\x69\x73\x6d\x61\x74\x63\x68\x65\x64\x20\x69\x6e\x70\x75\x74\x20\x27\x3b\x27\x20\x65\x78\x70\x65\x63\x74\x69\x6e\x67\x20\x4b\x5f\x46\x52\x4f\x4d\x20\x28\x73\x65\x6c\x65\x63\x74\x2a\x5b\x3b\x5d\x29";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.@"error", data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try ErrorMessage.read(arena.allocator(), &mr);

    try testing.expectEqual(ErrorCode.SyntaxError, message.error_code);
    try testing.expectEqualStrings("line 2:0 mismatched input ';' expecting K_FROM (select*[;])", message.message);
}

/// OPTIONS is sent to a node to ask which STARTUP options are supported.
///
/// Described in the protocol spec at §4.1.3.
pub const OptionsMessage = struct {};

test "options message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x04\x00\x00\x05\x05\x00\x00\x00\x00";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.options, data.len, envelope.header);
}

/// STARTUP is sent to a node to initialize a connection.
///
/// Described in the protocol spec at §4.1.1.
pub const StartupMessage = struct {
    const Self = @This();

    cql_version: CQLVersion,
    compression: ?CompressionAlgorithm,

    pub fn write(self: Self, mw: *MessageWriter) !void {
        var buf: [16]u8 = undefined;
        const cql_version = try self.cql_version.print(&buf);

        if (self.compression) |c| {
            // Always 2 keys
            _ = try mw.startStringMap(2);

            _ = try mw.writeString("CQL_VERSION");
            _ = try mw.writeString(cql_version);

            _ = try mw.writeString("COMPRESSION");
            switch (c) {
                .LZ4 => _ = try mw.writeString("lz4"),
                .Snappy => _ = try mw.writeString("snappy"),
            }
        } else {
            // Always 1 key
            _ = try mw.startStringMap(1);
            _ = try mw.writeString("CQL_VERSION");
            _ = try mw.writeString(cql_version);
        }
    }

    pub fn read(allocator: mem.Allocator, br: *MessageReader) !Self {
        var message = Self{
            .cql_version = undefined,
            .compression = null,
        };

        const map = try br.readStringMap(allocator);

        // CQL_VERSION is mandatory and the only version supported is 3.0.0 right now.
        if (map.getEntry("CQL_VERSION")) |entry| {
            if (!mem.eql(u8, "3.0.0", entry.value_ptr.*)) {
                return error.InvalidCQLVersion;
            }
            message.cql_version = try CQLVersion.fromString(entry.value_ptr.*);
        } else {
            return error.InvalidCQLVersion;
        }

        if (map.getEntry("COMPRESSION")) |entry| {
            if (mem.eql(u8, entry.value_ptr.*, "lz4")) {
                message.compression = CompressionAlgorithm.LZ4;
            } else if (mem.eql(u8, entry.value_ptr.*, "snappy")) {
                message.compression = CompressionAlgorithm.Snappy;
            } else {
                return error.InvalidCompression;
            }
        }

        return message;
    }
};

test "startup message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x05\x33\x2e\x30\x2e\x30";
    const envelope = try testReadEnvelope(arena.allocator(), exp);

    try checkEnvelopeHeader(4, Opcode.startup, exp.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try StartupMessage.read(arena.allocator(), &mr);

    try testing.expectEqual(CQLVersion{ .major = 3, .minor = 0, .patch = 0 }, message.cql_version);
    try testing.expect(message.compression == null);

    // write

    try expectSameEnvelope(StartupMessage, message, envelope.header, exp);
}

/// EXECUTE is sent to execute a prepared query.
///
/// Described in the protocol spec at §4.1.6
pub const ExecuteMessage = struct {
    const Self = @This();

    query_id: []const u8,
    result_metadata_id: ?[]const u8,
    query_parameters: QueryParameters,

    pub fn write(self: Self, protocol_version: ProtocolVersion, mw: *MessageWriter) !void {
        _ = try mw.writeShortBytes(self.query_id);
        if (protocol_version.is(5)) {
            if (self.result_metadata_id) |id| {
                _ = try mw.writeShortBytes(id);
            }
        }
        _ = try self.query_parameters.write(protocol_version, mw);
    }

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, br: *MessageReader) !Self {
        var message = Self{
            .query_id = undefined,
            .result_metadata_id = null,
            .query_parameters = undefined,
        };

        message.query_id = (try br.readShortBytes(allocator)) orelse &[_]u8{};
        if (protocol_version.is(5)) {
            message.result_metadata_id = try br.readShortBytes(allocator);
        }
        message.query_parameters = try QueryParameters.read(allocator, protocol_version, br);

        return message;
    }
};

test "execute message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x01\x00\x0a\x00\x00\x00\x37\x00\x10\x97\x97\x95\x6d\xfe\xb2\x4c\x99\x86\x8e\xd3\x84\xff\x6f\xd9\x4c\x00\x04\x27\x00\x01\x00\x00\x00\x10\xeb\x11\xc9\x1e\xd8\xcc\x48\x4d\xaf\x55\xe9\x9f\x5c\xd9\xec\x4a\x00\x00\x13\x88\x00\x05\xa2\x41\x4c\x1b\x06\x4c";
    const envelope = try testReadEnvelope(arena.allocator(), exp);

    try checkEnvelopeHeader(4, Opcode.execute, exp.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try ExecuteMessage.read(arena.allocator(), envelope.header.version, &mr);

    const exp_query_id = "\x97\x97\x95\x6d\xfe\xb2\x4c\x99\x86\x8e\xd3\x84\xff\x6f\xd9\x4c";
    try testing.expectEqualSlices(u8, exp_query_id, message.query_id);

    try testing.expectEqual(Consistency.Quorum, message.query_parameters.consistency_level);

    const values = message.query_parameters.values.?.Normal;
    try testing.expectEqual(@as(usize, 1), values.len);
    try testing.expectEqualSlices(u8, "\xeb\x11\xc9\x1e\xd8\xcc\x48\x4d\xaf\x55\xe9\x9f\x5c\xd9\xec\x4a", values[0].Set);
    try testing.expectEqual(@as(u32, 5000), message.query_parameters.page_size.?);
    try testing.expect(message.query_parameters.paging_state == null);
    try testing.expect(message.query_parameters.serial_consistency_level == null);
    try testing.expectEqual(@as(u64, 1585776216966732), message.query_parameters.timestamp.?);
    try testing.expect(message.query_parameters.keyspace == null);
    try testing.expect(message.query_parameters.now_in_seconds == null);

    // write

    try expectSameEnvelope(ExecuteMessage, message, envelope.header, exp);
}

/// AUTHENTICATE is sent by a node in response to a STARTUP message if authentication is required.
///
/// Described in the protocol spec at §4.2.3.
pub const AuthenticateMessage = struct {
    authenticator: []const u8,

    pub fn read(allocator: mem.Allocator, br: *MessageReader) !AuthenticateMessage {
        return AuthenticateMessage{
            .authenticator = try br.readString(allocator),
        };
    }
};

/// AUTH_RESPONSE is sent to a node to answser a authentication challenge.
///
/// Described in the protocol spec at §4.1.2.
pub const AuthResponseMessage = struct {
    token: ?[]const u8,

    pub fn write(self: @This(), mw: *MessageWriter) !void {
        return mw.writeBytes(self.token);
    }

    pub fn read(allocator: mem.Allocator, br: *MessageReader) !AuthResponseMessage {
        return AuthResponseMessage{
            .token = try br.readBytes(allocator),
        };
    }
};

/// AUTH_CHALLENGE is a server authentication challenge.
///
/// Described in the protocol spec at §4.2.7.
pub const AuthChallengeMessage = struct {
    token: ?[]const u8,

    pub fn read(allocator: mem.Allocator, br: *MessageReader) !AuthChallengeMessage {
        return AuthChallengeMessage{
            .token = try br.readBytes(allocator),
        };
    }
};

/// AUTH_SUCCESS indicates the success of the authentication phase.
///
/// Described in the protocol spec at §4.2.8.
pub const AuthSuccessMessage = struct {
    token: ?[]const u8,

    pub fn read(allocator: mem.Allocator, br: *MessageReader) !AuthSuccessMessage {
        return AuthSuccessMessage{
            .token = try br.readBytes(allocator),
        };
    }
};

test "authenticate message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x84\x00\x00\x00\x03\x00\x00\x00\x31\x00\x2f\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x2e\x61\x75\x74\x68\x2e\x50\x61\x73\x73\x77\x6f\x72\x64\x41\x75\x74\x68\x65\x6e\x74\x69\x63\x61\x74\x6f\x72";
    const envelope = try testReadEnvelope(arena.allocator(), exp);

    try checkEnvelopeHeader(4, Opcode.authenticate, exp.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try AuthenticateMessage.read(arena.allocator(), &mr);

    try testing.expectEqualStrings("org.apache.cassandra.auth.PasswordAuthenticator", message.authenticator);
}

test "auth challenge message" {
    // TODO(vincent): how do I get one of these message ?
}

test "auth response message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\x02\x0f\x00\x00\x00\x18\x00\x00\x00\x14\x00\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x00\x63\x61\x73\x73\x61\x6e\x64\x72\x61";
    const envelope = try testReadEnvelope(arena.allocator(), exp);

    try checkEnvelopeHeader(4, Opcode.auth_response, exp.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try AuthResponseMessage.read(arena.allocator(), &mr);

    const exp_token = "\x00cassandra\x00cassandra";
    try testing.expectEqualSlices(u8, exp_token, message.token.?);

    // write

    try expectSameEnvelope(AuthResponseMessage, message, envelope.header, exp);
}

test "auth success message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x84\x00\x00\x02\x10\x00\x00\x00\x04\xff\xff\xff\xff";
    const envelope = try testReadEnvelope(arena.allocator(), exp);

    try checkEnvelopeHeader(4, Opcode.auth_success, exp.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try AuthSuccessMessage.read(arena.allocator(), &mr);

    try testing.expect(message.token == null);
}

/// Structure of a query in a BATCH message
const BatchQuery = struct {
    const Self = @This();

    query_string: ?[]const u8,
    query_id: ?[]const u8,

    values: Values,

    pub fn write(self: Self, mw: *MessageWriter) !void {
        if (self.query_string) |query_string| {
            _ = try mw.writeByte(0);
            _ = try mw.writeLongString(query_string);
        } else if (self.query_id) |query_id| {
            _ = try mw.writeByte(1);
            _ = try mw.writeShortBytes(query_id);
        }

        switch (self.values) {
            .Normal => |normal_values| {
                _ = try mw.writeInt(u16, @intCast(normal_values.len));
                for (normal_values) |value| {
                    _ = try mw.writeValue(value);
                }
            },
            .Named => |named_values| {
                _ = try mw.writeInt(u16, @intCast(named_values.len));
                for (named_values) |v| {
                    _ = try mw.writeString(v.name);
                    _ = try mw.writeValue(v.value);
                }
            },
        }
    }

    pub fn read(allocator: mem.Allocator, br: *MessageReader) !BatchQuery {
        var query = Self{
            .query_string = null,
            .query_id = null,
            .values = undefined,
        };

        const kind = try br.readByte();
        switch (kind) {
            0 => query.query_string = try br.readLongString(allocator),
            1 => query.query_id = try br.readShortBytes(allocator),
            else => return error.InvalidQueryKind,
        }

        var list = std.ArrayList(Value).init(allocator);
        errdefer list.deinit();

        const n_values = try br.readInt(u16);
        var j: usize = 0;
        while (j < @as(usize, n_values)) : (j += 1) {
            const value = try br.readValue(allocator);
            _ = try list.append(value);
        }

        query.values = Values{ .Normal = try list.toOwnedSlice() };

        return query;
    }
};

/// BATCH is sent to execute a list of queries (prepared or not) as a batch.
///
/// Described in the protocol spec at §4.1.7
pub const BatchMessage = struct {
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

    pub fn write(self: Self, protocol_version: ProtocolVersion, mw: *MessageWriter) !void {
        _ = try mw.writeInt(u8, @intCast(@intFromEnum(self.batch_type)));

        // Write the queries

        _ = try mw.writeInt(u16, @intCast(self.queries.len));
        for (self.queries) |query| {
            _ = try query.write(mw);
        }

        // Write the consistency

        _ = try mw.writeConsistency(self.consistency_level);

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
            _ = try mw.writeInt(u32, flags);
        } else {
            _ = try mw.writeInt(u8, @intCast(flags));
        }

        // Write the remaining body
    }

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, br: *MessageReader) !Self {
        var message = Self{
            .batch_type = undefined,
            .queries = undefined,
            .consistency_level = undefined,
            .serial_consistency_level = null,
            .timestamp = null,
            .keyspace = null,
            .now_in_seconds = null,
        };

        message.batch_type = @enumFromInt(try br.readByte());
        message.queries = &[_]BatchQuery{};

        // Read all queries in the batch

        var queries = std.ArrayList(BatchQuery).init(allocator);
        errdefer queries.deinit();

        const n = try br.readInt(u16);
        var i: usize = 0;
        while (i < @as(usize, n)) : (i += 1) {
            const query = try BatchQuery.read(allocator, br);
            _ = try queries.append(query);
        }

        message.queries = try queries.toOwnedSlice();

        // Read the rest of the message

        message.consistency_level = try br.readConsistency();

        // The size of the flags bitmask depends on the protocol version.
        var flags: u32 = 0;
        if (protocol_version.is(5)) {
            flags = try br.readInt(u32);
        } else {
            flags = try br.readInt(u8);
        }

        if (flags & FlagWithSerialConsistency == FlagWithSerialConsistency) {
            const consistency_level = try br.readConsistency();
            if (consistency_level != .Serial and consistency_level != .LocalSerial) {
                return error.InvalidSerialConsistency;
            }
            message.serial_consistency_level = consistency_level;
        }
        if (flags & FlagWithDefaultTimestamp == FlagWithDefaultTimestamp) {
            const timestamp = try br.readInt(u64);
            if (timestamp < 0) {
                return error.InvalidNegativeTimestamp;
            }
            message.timestamp = timestamp;
        }

        if (!protocol_version.is(5)) {
            return message;
        }

        // The following flags are only valid with protocol v5
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            message.keyspace = try br.readString(allocator);
        }
        if (flags & FlagWithNowInSeconds == FlagWithNowInSeconds) {
            message.now_in_seconds = try br.readInt(u32);
        }

        return message;
    }
};

test "batch message: query type string" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\xc0\x0d\x00\x00\x00\xcc\x00\x00\x03\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00";
    const envelope = try testReadEnvelope(arena.allocator(), exp);

    try checkEnvelopeHeader(4, Opcode.batch, exp.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try BatchMessage.read(arena.allocator(), envelope.header.version, &mr);

    try testing.expectEqual(BatchType.Logged, message.batch_type);

    try testing.expectEqual(@as(usize, 3), message.queries.len);
    for (message.queries) |query| {
        const exp_query = "INSERT INTO foobar.user(id, name) values(uuid(), 'vincent')";
        try testing.expectEqualStrings(exp_query, query.query_string.?);
        try testing.expect(query.query_id == null);
        try testing.expect(query.values == .Normal);
        try testing.expectEqual(@as(usize, 0), query.values.Normal.len);
    }

    try testing.expectEqual(Consistency.Any, message.consistency_level);
    try testing.expect(message.serial_consistency_level == null);
    try testing.expect(message.timestamp == null);
    try testing.expect(message.keyspace == null);
    try testing.expect(message.now_in_seconds == null);

    // write

    try expectSameEnvelope(BatchMessage, message, envelope.header, exp);
}

test "batch message: query type prepared" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x01\x00\x0d\x00\x00\x00\xa2\x00\x00\x03\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\x3a\x9a\xab\x41\x68\x24\x4a\xef\x9d\xf5\x72\xc7\x84\xab\xa2\x57\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\xed\x54\xb0\x6d\xcc\xb2\x43\x51\x96\x51\x74\x5e\xee\xae\xd2\xfe\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\x79\xdf\x8a\x28\x5a\x60\x47\x19\x9b\x42\x84\xea\x69\x10\x1a\xe6\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x00\x00\x00";
    const envelope = try testReadEnvelope(arena.allocator(), exp);

    try checkEnvelopeHeader(4, Opcode.batch, exp.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try BatchMessage.read(arena.allocator(), envelope.header.version, &mr);

    try testing.expectEqual(BatchType.Logged, message.batch_type);

    const expUUIDs = &[_][]const u8{
        "\x3a\x9a\xab\x41\x68\x24\x4a\xef\x9d\xf5\x72\xc7\x84\xab\xa2\x57",
        "\xed\x54\xb0\x6d\xcc\xb2\x43\x51\x96\x51\x74\x5e\xee\xae\xd2\xfe",
        "\x79\xdf\x8a\x28\x5a\x60\x47\x19\x9b\x42\x84\xea\x69\x10\x1a\xe6",
    };

    try testing.expectEqual(@as(usize, 3), message.queries.len);
    var i: usize = 0;
    for (message.queries) |query| {
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

    try testing.expectEqual(Consistency.Any, message.consistency_level);
    try testing.expect(message.serial_consistency_level == null);
    try testing.expect(message.timestamp == null);
    try testing.expect(message.keyspace == null);
    try testing.expect(message.now_in_seconds == null);

    // write

    try expectSameEnvelope(BatchMessage, message, envelope.header, exp);
}

/// EVENT is an event pushed by the server.
///
/// Described in the protocol spec at §4.2.6.
pub const EventMessage = struct {
    const Self = @This();

    event: event.Event,

    pub fn read(allocator: mem.Allocator, br: *MessageReader) !Self {
        var message = Self{
            .event = undefined,
        };

        const event_type = std.meta.stringToEnum(event.EventType, try br.readString(allocator)) orelse return error.InvalidEventType;

        switch (event_type) {
            .TOPOLOGY_CHANGE => {
                var change = event.TopologyChange{
                    .type = undefined,
                    .node_address = undefined,
                };

                change.type = std.meta.stringToEnum(event.TopologyChangeType, try br.readString(allocator)) orelse return error.InvalidTopologyChangeType;
                change.node_address = try br.readInet();

                message.event = event.Event{ .TOPOLOGY_CHANGE = change };

                return message;
            },
            .STATUS_CHANGE => {
                var change = event.StatusChange{
                    .type = undefined,
                    .node_address = undefined,
                };

                change.type = std.meta.stringToEnum(event.StatusChangeType, try br.readString(allocator)) orelse return error.InvalidStatusChangeType;
                change.node_address = try br.readInet();

                message.event = event.Event{ .STATUS_CHANGE = change };

                return message;
            },
            .SCHEMA_CHANGE => {
                message.event = event.Event{ .SCHEMA_CHANGE = try event.SchemaChange.read(allocator, br) };

                return message;
            },
        }
    }
};

test "event message: topology change" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x84\x00\xff\xff\x0c\x00\x00\x00\x24\x00\x0f\x54\x4f\x50\x4f\x4c\x4f\x47\x59\x5f\x43\x48\x41\x4e\x47\x45\x00\x08\x4e\x45\x57\x5f\x4e\x4f\x44\x45\x04\x7f\x00\x00\x04\x00\x00\x23\x52";
    const envlope = try testReadEnvelope(arena.allocator(), exp);

    try checkEnvelopeHeader(4, Opcode.event, exp.len, envlope.header);

    var mr: MessageReader = undefined;
    mr.reset(envlope.body);
    const message = try EventMessage.read(arena.allocator(), &mr);

    try testing.expect(message.event == .TOPOLOGY_CHANGE);

    const topology_change = message.event.TOPOLOGY_CHANGE;
    try testing.expectEqual(event.TopologyChangeType.NEW_NODE, topology_change.type);

    const localhost = net.Address.initIp4([4]u8{ 0x7f, 0x00, 0x00, 0x04 }, 9042);
    try testing.expect(net.Address.eql(localhost, topology_change.node_address));
}

test "event message: status change" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x1e\x00\x0d\x53\x54\x41\x54\x55\x53\x5f\x43\x48\x41\x4e\x47\x45\x00\x04\x44\x4f\x57\x4e\x04\x7f\x00\x00\x01\x00\x00\x23\x52";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.event, data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try EventMessage.read(arena.allocator(), &mr);

    try testing.expect(message.event == .STATUS_CHANGE);

    const status_change = message.event.STATUS_CHANGE;
    try testing.expectEqual(event.StatusChangeType.DOWN, status_change.type);

    const localhost = net.Address.initIp4([4]u8{ 0x7f, 0x00, 0x00, 0x01 }, 9042);
    try testing.expect(net.Address.eql(localhost, status_change.node_address));
}

test "event message: schema change/keyspace" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x2a\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45\x00\x07\x43\x52\x45\x41\x54\x45\x44\x00\x08\x4b\x45\x59\x53\x50\x41\x43\x45\x00\x06\x62\x61\x72\x62\x61\x7a";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.event, data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try EventMessage.read(arena.allocator(), &mr);

    try testing.expect(message.event == .SCHEMA_CHANGE);

    const schema_change = message.event.SCHEMA_CHANGE;
    try testing.expectEqual(event.SchemaChangeType.CREATED, schema_change.type);
    try testing.expectEqual(event.SchemaChangeTarget.KEYSPACE, schema_change.target);

    const options = schema_change.options;
    try testing.expectEqualStrings("barbaz", options.keyspace);
    try testing.expectEqualStrings("", options.object_name);
    try testing.expect(options.arguments == null);
}

test "event message: schema change/table" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x2e\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45\x00\x07\x43\x52\x45\x41\x54\x45\x44\x00\x05\x54\x41\x42\x4c\x45\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x05\x73\x61\x6c\x75\x74";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.event, data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try EventMessage.read(arena.allocator(), &mr);

    try testing.expect(message.event == .SCHEMA_CHANGE);

    const schema_change = message.event.SCHEMA_CHANGE;
    try testing.expectEqual(event.SchemaChangeType.CREATED, schema_change.type);
    try testing.expectEqual(event.SchemaChangeTarget.TABLE, schema_change.target);

    const options = schema_change.options;
    try testing.expectEqualStrings("foobar", options.keyspace);
    try testing.expectEqualStrings("salut", options.object_name);
    try testing.expect(options.arguments == null);
}

test "event message: schema change/function" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x40\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45\x00\x07\x43\x52\x45\x41\x54\x45\x44\x00\x08\x46\x55\x4e\x43\x54\x49\x4f\x4e\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x0d\x73\x6f\x6d\x65\x5f\x66\x75\x6e\x63\x74\x69\x6f\x6e\x00\x01\x00\x03\x69\x6e\x74";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.event, data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try EventMessage.read(arena.allocator(), &mr);

    try testing.expect(message.event == .SCHEMA_CHANGE);

    const schema_change = message.event.SCHEMA_CHANGE;
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
pub const PrepareMessage = struct {
    const Self = @This();

    query: []const u8,
    keyspace: ?[]const u8,

    const FlagWithKeyspace = 0x01;

    pub fn write(self: Self, protocol_version: ProtocolVersion, mw: *MessageWriter) !void {
        _ = try mw.writeLongString(self.query);
        if (!protocol_version.is(5)) {
            return;
        }

        if (self.keyspace) |ks| {
            _ = try mw.writeInt(u32, FlagWithKeyspace);
            _ = try mw.writeString(ks);
        } else {
            _ = try mw.writeInt(u32, 0);
        }
    }

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, br: *MessageReader) !Self {
        var message = Self{
            .query = undefined,
            .keyspace = null,
        };

        message.query = try br.readLongString(allocator);

        if (!protocol_version.is(5)) {
            return message;
        }

        const flags = try br.readInt(u32);
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            message.keyspace = try br.readString(allocator);
        }

        return message;
    }
};

test "prepare message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\xc0\x09\x00\x00\x00\x32\x00\x00\x00\x2e\x53\x45\x4c\x45\x43\x54\x20\x61\x67\x65\x2c\x20\x6e\x61\x6d\x65\x20\x66\x72\x6f\x6d\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x20\x77\x68\x65\x72\x65\x20\x69\x64\x20\x3d\x20\x3f";
    const envelope = try testReadEnvelope(arena.allocator(), exp);

    try checkEnvelopeHeader(4, Opcode.prepare, exp.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try PrepareMessage.read(arena.allocator(), envelope.header.version, &mr);

    try testing.expectEqualStrings("SELECT age, name from foobar.user where id = ?", message.query);
    try testing.expect(message.keyspace == null);

    // write

    try expectSameEnvelope(PrepareMessage, message, envelope.header, exp);
}

/// QUERY is sent to perform a CQL query.
///
/// Described in the protocol spec at §4.1.4
pub const QueryMessage = struct {
    const Self = @This();

    query: []const u8,
    query_parameters: QueryParameters,

    pub fn write(self: Self, protocol_version: ProtocolVersion, mw: *MessageWriter) !void {
        _ = try mw.writeLongString(self.query);
        _ = try self.query_parameters.write(protocol_version, mw);
    }

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, br: *MessageReader) !Self {
        return Self{
            .query = try br.readLongString(allocator),
            .query_parameters = try QueryParameters.read(allocator, protocol_version, br),
        };
    }
};

test "query message: no values, no paging state" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\x08\x07\x00\x00\x00\x30\x00\x00\x00\x1b\x53\x45\x4c\x45\x43\x54\x20\x2a\x20\x46\x52\x4f\x4d\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x20\x3b\x00\x01\x34\x00\x00\x00\x64\x00\x08\x00\x05\xa2\x2c\xf0\x57\x3e\x3f";
    const envelope = try testReadEnvelope(arena.allocator(), exp);

    try checkEnvelopeHeader(4, Opcode.query, exp.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try QueryMessage.read(arena.allocator(), envelope.header.version, &mr);

    try testing.expectEqualStrings("SELECT * FROM foobar.user ;", message.query);
    try testing.expectEqual(Consistency.One, message.query_parameters.consistency_level);
    try testing.expect(message.query_parameters.values == null);
    try testing.expectEqual(@as(u32, 100), message.query_parameters.page_size.?);
    try testing.expect(message.query_parameters.paging_state == null);
    try testing.expectEqual(Consistency.Serial, message.query_parameters.serial_consistency_level.?);
    try testing.expectEqual(@as(u64, 1585688778063423), message.query_parameters.timestamp.?);
    try testing.expect(message.query_parameters.keyspace == null);
    try testing.expect(message.query_parameters.now_in_seconds == null);

    // write

    try expectSameEnvelope(QueryMessage, message, envelope.header, exp);
}

/// READY is sent by a node to indicate it is ready to process queries.
///
/// Described in the protocol spec at §4.2.2.
pub const ReadyMessage = struct {};

test "ready message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x02\x02\x00\x00\x00\x00";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.ready, data.len, envelope.header);
}

/// REGISTER is sent to register this connection to receive some types of events.
///
/// Described in the protocol spec at §4.1.8
const RegisterMessage = struct {
    event_types: []const []const u8,

    pub fn write(self: @This(), mw: *MessageWriter) !void {
        return mw.writeStringList(self.event_types);
    }

    pub fn read(allocator: mem.Allocator, br: *MessageReader) !RegisterMessage {
        return RegisterMessage{
            .event_types = try br.readStringList(allocator),
        };
    }
};

test "register message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\xc0\x0b\x00\x00\x00\x31\x00\x03\x00\x0f\x54\x4f\x50\x4f\x4c\x4f\x47\x59\x5f\x43\x48\x41\x4e\x47\x45\x00\x0d\x53\x54\x41\x54\x55\x53\x5f\x43\x48\x41\x4e\x47\x45\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45";
    const envelope = try testReadEnvelope(arena.allocator(), exp);

    try checkEnvelopeHeader(4, Opcode.register, exp.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try RegisterMessage.read(arena.allocator(), &mr);

    try testing.expectEqual(@as(usize, 3), message.event_types.len);
    try testing.expectEqualStrings("TOPOLOGY_CHANGE", message.event_types[0]);
    try testing.expectEqualStrings("STATUS_CHANGE", message.event_types[1]);
    try testing.expectEqualStrings("SCHEMA_CHANGE", message.event_types[2]);

    // write

    try expectSameEnvelope(RegisterMessage, message, envelope.header, exp);
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

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, br: *MessageReader) !Self {
        var rows = Self{
            .metadata = undefined,
            .data = undefined,
        };

        rows.metadata = try metadata.RowsMetadata.read(allocator, protocol_version, br);

        // Iterate over rows
        const rows_count = @as(usize, try br.readInt(u32));

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
                const column_data = (try br.readBytes(allocator)) orelse &[_]u8{};

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

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, br: *MessageReader) !Self {
        var prepared = Self{
            .query_id = undefined,
            .result_metadata_id = null,
            .metadata = undefined,
            .rows_metadata = undefined,
        };

        prepared.query_id = (try br.readShortBytes(allocator)) orelse return error.NoQueryIDInPreparedFrame;
        if (protocol_version.is(5)) {
            prepared.result_metadata_id = (try br.readShortBytes(allocator)) orelse return error.NoResultMetadataIDInPreparedFrame;
        }

        prepared.metadata = try metadata.PreparedMetadata.read(allocator, protocol_version, br);
        prepared.rows_metadata = try metadata.RowsMetadata.read(allocator, protocol_version, br);

        return prepared;
    }
};

pub const ResultMessage = struct {
    const Self = @This();

    result: Result,

    pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, br: *MessageReader) !ResultMessage {
        var message = Self{
            .result = undefined,
        };

        const kind: ResultKind = @enumFromInt(try br.readInt(u32));

        switch (kind) {
            .Void => message.result = Result{ .Void = {} },
            .Rows => {
                const rows = try Rows.read(allocator, protocol_version, br);
                message.result = Result{ .Rows = rows };
            },
            .SetKeyspace => {
                const keyspace = try br.readString(allocator);
                message.result = Result{ .SetKeyspace = keyspace };
            },
            .Prepared => {
                const prepared = try Prepared.read(allocator, protocol_version, br);
                message.result = Result{ .Prepared = prepared };
            },
            .SchemaChange => {
                const schema_change = try event.SchemaChange.read(allocator, br);
                message.result = Result{ .SchemaChange = schema_change };
            },
        }

        return message;
    }
};

test "result message: void" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x9d\x08\x00\x00\x00\x04\x00\x00\x00\x01";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.result, data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try ResultMessage.read(arena.allocator(), envelope.header.version, &mr);

    try testing.expect(message.result == .Void);
}

test "result message: rows" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x20\x08\x00\x00\x00\xa2\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x03\x00\x00\x00\x10\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\xd7\x77\xd5\xd7\x58\xc0\x4d\x2b\x8c\xf9\xa3\x53\xfa\x8e\x6c\x96\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\x94\xa4\x7b\xb2\x8c\xf7\x43\x3d\x97\x6e\x72\x74\xb3\xfd\xd3\x31\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.result, data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try ResultMessage.read(arena.allocator(), envelope.header.version, &mr);

    try testing.expect(message.result == .Rows);

    // check metadata

    const rows_metadata = message.result.Rows.metadata;
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

    const rows = message.result.Rows;
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

test "result message: rows, don't skip metadata" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x00\x08\x00\x00\x02\x3a\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x00\x00\x28\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x2e\x64\x62\x2e\x6d\x61\x72\x73\x68\x61\x6c\x2e\x42\x79\x74\x65\x54\x79\x70\x65\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x0d\x00\x00\x00\x10\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\x87\x0e\x45\x7f\x56\x4a\x4f\xd5\xb5\xd6\x4a\x48\x4b\xe0\x67\x67\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\xc5\xb1\x12\xf4\x0d\xea\x4d\x22\x83\x5b\xe0\x25\xef\x69\x0c\xe1\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\xd7\x77\xd5\xd7\x58\xc0\x4d\x2b\x8c\xf9\xa3\x53\xfa\x8e\x6c\x96\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\x65\x54\x02\x89\x33\xe1\x42\x73\x82\xcc\x1c\xdb\x3d\x24\x5e\x40\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\x6a\xd1\x07\x9d\x27\x23\x47\x76\x8b\x7f\x39\x4d\xe3\xb8\x97\xc5\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32\x00\x00\x00\x10\xe9\xef\xfe\xa6\xeb\xc5\x4d\xb9\xaf\xc7\xd7\xc0\x28\x43\x27\x40\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\x96\xe6\xdd\x62\x14\xc9\x4e\x7c\xa1\x2f\x98\x5e\xe9\xe0\x91\x0d\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32\x00\x00\x00\x10\xef\xd5\x5a\x9b\xec\x7f\x4c\x5c\x89\xc3\x8c\xfa\x28\xf9\x6d\xfe\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\x94\xa4\x7b\xb2\x8c\xf7\x43\x3d\x97\x6e\x72\x74\xb3\xfd\xd3\x31\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32\x00\x00\x00\x10\xe6\x02\xc7\x47\xbf\xca\x44\xbc\x9d\xc6\x6b\x04\x0f\xb7\x15\xed\x00\x00\x00\x01\x78\x00\x00\x00\x04\x48\x61\x68\x61\x00\x00\x00\x10\xac\x5e\xcc\xa8\x8e\xa1\x42\x2f\x86\xe6\xa0\x93\xbe\xd2\x73\x22\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32\x00\x00\x00\x10\xbe\x90\x37\x66\x31\xe5\x43\x93\xbc\x99\x43\xd3\x69\xf8\xe6\xba\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.result, data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try ResultMessage.read(arena.allocator(), envelope.header.version, &mr);

    try testing.expect(message.result == .Rows);

    // check metadata

    const rows_metadata = message.result.Rows.metadata;
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

    const rows = message.result.Rows;
    try testing.expectEqual(@as(usize, 13), rows.data.len);

    const row1 = rows.data[0].slice;
    try testing.expectEqualSlices(u8, "\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8", row1[0].slice);
    try testing.expectEqualSlices(u8, "\x00", row1[1].slice);
    try testing.expectEqualSlices(u8, "\x56\x69\x6e\x63\x65\x6e\x74\x30", row1[2].slice);
}

test "result message: rows, list of uuid" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x00\x08\x00\x00\x00\x58\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x02\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x0a\x61\x67\x65\x5f\x74\x6f\x5f\x69\x64\x73\x00\x03\x61\x67\x65\x00\x09\x00\x03\x69\x64\x73\x00\x20\x00\x0c\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x78\x00\x00\x00\x18\x00\x00\x00\x01\x00\x00\x00\x10\xe6\x02\xc7\x47\xbf\xca\x44\xbc\x9d\xc6\x6b\x04\x0f\xb7\x15\xed";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.result, data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try ResultMessage.read(arena.allocator(), envelope.header.version, &mr);

    try testing.expect(message.result == .Rows);

    // check metadata

    const rows_metadata = message.result.Rows.metadata;
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

    const rows = message.result.Rows;
    try testing.expectEqual(@as(usize, 1), rows.data.len);

    const row1 = rows.data[0].slice;
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x78", row1[0].slice);
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x01\x00\x00\x00\x10\xe6\x02\xc7\x47\xbf\xca\x44\xbc\x9d\xc6\x6b\x04\x0f\xb7\x15\xed", row1[1].slice);
}

test "result message: set keyspace" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x77\x08\x00\x00\x00\x0c\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.result, data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try ResultMessage.read(arena.allocator(), envelope.header.version, &mr);

    try testing.expect(message.result == .SetKeyspace);
    try testing.expectEqualStrings("foobar", message.result.SetKeyspace);
}

test "result message: prepared insert" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x80\x08\x00\x00\x00\x4f\x00\x00\x00\x04\x00\x10\x63\x7c\x1c\x1f\xd0\x13\x4a\xb8\xfc\x94\xca\x67\xf2\x88\xb2\xa3\x00\x00\x00\x01\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x04\x00\x00\x00\x00";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.result, data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try ResultMessage.read(arena.allocator(), envelope.header.version, &mr);

    try testing.expect(message.result == .Prepared);

    // check prepared metadata

    {
        const prepared_metadata = message.result.Prepared.metadata;
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
        const rows_metadata = message.result.Prepared.rows_metadata;
        try testing.expect(rows_metadata.global_table_spec == null);
        try testing.expectEqual(@as(usize, 0), rows_metadata.column_specs.len);
    }
}

test "result message: prepared select" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\xc0\x08\x00\x00\x00\x63\x00\x00\x00\x04\x00\x10\x3b\x2e\x8d\x03\x43\xf4\x3b\xfc\xad\xa1\x78\x9c\x27\x0e\xcf\xee\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x00\x00\x01\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d";
    const envelope = try testReadEnvelope(arena.allocator(), data);

    try checkEnvelopeHeader(4, Opcode.result, data.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try ResultMessage.read(arena.allocator(), envelope.header.version, &mr);

    try testing.expect(message.result == .Prepared);

    // check prepared metadata

    {
        const prepared_metadata = message.result.Prepared.metadata;
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
        const rows_metadata = message.result.Prepared.rows_metadata;
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

/// SUPPORTED is sent by a node in response to a OPTIONS message.
///
/// Described in the protocol spec at §4.2.4.
pub const SupportedMessage = struct {
    const Self = @This();

    protocol_versions: []ProtocolVersion,
    cql_versions: []CQLVersion,
    compression_algorithms: []CompressionAlgorithm,

    pub fn read(allocator: mem.Allocator, br: *MessageReader) !Self {
        var message = Self{
            .protocol_versions = &[_]ProtocolVersion{},
            .cql_versions = &[_]CQLVersion{},
            .compression_algorithms = &[_]CompressionAlgorithm{},
        };

        const options = try br.readStringMultimap(allocator);

        if (options.get("CQL_VERSION")) |values| {
            var list = std.ArrayList(CQLVersion).init(allocator);

            for (values) |value| {
                const version = try CQLVersion.fromString(value);
                _ = try list.append(version);
            }

            message.cql_versions = try list.toOwnedSlice();
        } else {
            return error.NoCQLVersion;
        }

        if (options.get("COMPRESSION")) |values| {
            var list = std.ArrayList(CompressionAlgorithm).init(allocator);

            for (values) |value| {
                const compression_algorithm = try CompressionAlgorithm.fromString(value);
                _ = try list.append(compression_algorithm);
            }

            message.compression_algorithms = try list.toOwnedSlice();
        }

        if (options.get("PROTOCOL_VERSIONS")) |values| {
            var list = std.ArrayList(ProtocolVersion).init(allocator);

            for (values) |value| {
                const version = try ProtocolVersion.fromString(value);
                _ = try list.append(version);
            }

            message.protocol_versions = try list.toOwnedSlice();
        }

        return message;
    }
};

test "supported message" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x84\x00\x00\x09\x06\x00\x00\x00\x60\x00\x03\x00\x11\x50\x52\x4f\x54\x4f\x43\x4f\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x53\x00\x03\x00\x04\x33\x2f\x76\x33\x00\x04\x34\x2f\x76\x34\x00\x09\x35\x2f\x76\x35\x2d\x62\x65\x74\x61\x00\x0b\x43\x4f\x4d\x50\x52\x45\x53\x53\x49\x4f\x4e\x00\x02\x00\x06\x73\x6e\x61\x70\x70\x79\x00\x03\x6c\x7a\x34\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x01\x00\x05\x33\x2e\x34\x2e\x34";
    const envelope = try testReadEnvelope(arena.allocator(), exp);

    try checkEnvelopeHeader(4, Opcode.supported, exp.len, envelope.header);

    var mr: MessageReader = undefined;
    mr.reset(envelope.body);
    const message = try SupportedMessage.read(arena.allocator(), &mr);

    try testing.expectEqual(@as(usize, 1), message.cql_versions.len);
    try testing.expectEqual(CQLVersion{ .major = 3, .minor = 4, .patch = 4 }, message.cql_versions[0]);

    try testing.expectEqual(@as(usize, 3), message.protocol_versions.len);
    try testing.expect(message.protocol_versions[0].is(3));
    try testing.expect(message.protocol_versions[1].is(4));
    try testing.expect(message.protocol_versions[2].is(5));

    try testing.expectEqual(@as(usize, 2), message.compression_algorithms.len);
    try testing.expectEqual(CompressionAlgorithm.Snappy, message.compression_algorithms[0]);
    try testing.expectEqual(CompressionAlgorithm.LZ4, message.compression_algorithms[1]);
}

/// Reads an enevelope from the provided buffer.
/// Only intended to be used for tests.
fn testReadEnvelope(allocator: mem.Allocator, data: []const u8) !Envelope {
    var fbs = io.fixedBufferStream(data);

    return Envelope.read(allocator, fbs.reader());
}

fn expectSameEnvelope(comptime T: type, fr: T, header: EnvelopeHeader, exp: []const u8) !void {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

    // Write message body
    var mw = try MessageWriter.init(allocator);

    const write_fn = @typeInfo(@TypeOf(T.write));
    switch (write_fn) {
        .@"fn" => |info| {
            if (info.params.len == 2) {
                try fr.write(&mw);
            } else if (info.params.len == 3) {
                try fr.write(header.version, &mw);
            }
        },
        else => unreachable,
    }

    // Write envelope
    const envelope = Envelope{
        .header = header,
        .body = mw.getWritten(),
    };

    var buf2: [1024]u8 = undefined;
    var fbs2 = io.fixedBufferStream(&buf2);
    const writer = fbs2.writer();
    var fw = EnvelopeWriter(@TypeOf(writer)).init(writer);

    try fw.write(envelope);

    if (!std.mem.eql(u8, exp, fbs2.getWritten())) {
        testutils.printHRBytes("\n==> exp   : {s}\n", exp, .{});
        testutils.printHRBytes("==> source: {s}\n", fbs2.getWritten(), .{});
        std.debug.panic("envelopes are different\n", .{});
    }
}

fn collectRows(comptime T: type, allocator: mem.Allocator, rows: Rows) !std.ArrayList(T) {
    var result = std.ArrayList(T).init(allocator);

    const Iterator = @import("iterator.zig").Iterator;
    var iter = Iterator.init(rows.metadata, rows.data);

    while (true) {
        var row_arena = std.heap.ArenaAllocator.init(allocator);
        errdefer row_arena.deinit();

        var row: T = undefined;

        // We want iteration diagnostics in case of failures.
        var iter_diags = Iterator.ScanOptions.Diagnostics{};
        const iter_options = Iterator.ScanOptions{
            .diags = &iter_diags,
        };

        const scanned = iter.scan(row_arena.allocator(), iter_options, &row) catch |err| switch (err) {
            error.IncompatibleMetadata => blk: {
                const im = iter_diags.incompatible_metadata;
                const it = im.incompatible_types;
                if (it.cql_type_name != null and it.native_type_name != null) {
                    std.debug.panic("metadata incompatible. CQL type {?s} can't be scanned into native type {?s}\n", .{
                        it.cql_type_name, it.native_type_name,
                    });
                } else {
                    std.debug.panic("metadata incompatible. columns in result: {d} fields in struct: {d}\n", .{
                        im.metadata_columns, im.struct_fields,
                    });
                }
                break :blk false;
            },
            else => return err,
        };
        if (!scanned) {
            break;
        }

        try result.append(row);
    }

    return result;
}
