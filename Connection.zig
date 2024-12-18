const std = @import("std");
const debug = std.debug;
const heap = std.heap;
const fmt = std.fmt;
const io = std.io;
const mem = std.mem;
const testing = std.testing;
const fifo = std.fifo.LinearFifo;
const build_options = @import("build_options");

const lz4 = @import("lz4");
const snappy = @import("snappy");

const protocol = @import("protocol.zig");
const Frame = protocol.Frame;
const Envelope = protocol.Envelope;
const EnvelopeFlags = protocol.EnvelopeFlags;
const EnvelopeHeader = protocol.EnvelopeHeader;
const EnvelopeWriter = protocol.EnvelopeWriter;
const CQLVersion = protocol.CQLVersion;
const CompressionAlgorithm = protocol.CompressionAlgorithm;
const MessageReader = protocol.MessageReader;
const MessageWriter = protocol.MessageWriter;
const Opcode = protocol.Opcode;
const ProtocolVersion = protocol.ProtocolVersion;
const AuthChallengeMessage = protocol.AuthChallengeMessage;
const AuthResponseMessage = protocol.AuthResponseMessage;
const AuthSuccessMessage = protocol.AuthSuccessMessage;
const AuthenticateMessage = protocol.AuthenticateMessage;
const BatchMessage = protocol.BatchMessage;
const ErrorMessage = protocol.ErrorMessage;
const EventMessage = protocol.EventMessage;
const ExecuteMessage = protocol.ExecuteMessage;
const OptionsMessage = protocol.OptionsMessage;
const PrepareMessage = protocol.PrepareMessage;
const QueryMessage = protocol.QueryMessage;
const ReadyMessage = protocol.ReadyMessage;
const RegisterMessage = protocol.RegisterMessage;
const ResultMessage = protocol.ResultMessage;
const StartupMessage = protocol.StartupMessage;
const SupportedMessage = protocol.SupportedMessage;
const QueryParameters = @import("QueryParameters.zig");
const testutils = @import("testutils.zig");

const log = std.log.scoped(.connection);

const Self = @This();

/// A tracing event that contains useful information for observability or debugging.
const TraceEvent = struct {
    message: Message,
    envelope: Envelope,
    data: []const u8,
};

/// Tracing is used in a Connection to record tracing events both when writing a message or reading a message.
/// Only enabled when the `enabled_tracing` build option is set.
///
/// This is currently only used in tests.
const Tracer = if (build_options.enable_tracing) struct {
    const EventBuffer = fifo(TraceEvent, .{ .Static = 32 });

    allocator: mem.Allocator,
    arena: heap.ArenaAllocator,
    events: EventBuffer,

    fn init(allocator: mem.Allocator) !*Tracer {
        const res = try allocator.create(Tracer);
        res.* = .{
            .allocator = allocator,
            .arena = heap.ArenaAllocator.init(allocator),
            .events = EventBuffer.init(),
        };

        return res;
    }

    fn deinit(self: *Tracer) void {
        self.arena.deinit();
        self.allocator.destroy(self);
    }

    /// Trace a new event. Make sure the data in the event will live as long as the tracing recorder.
    pub fn trace(self: *Tracer, event: TraceEvent) !void {
        try self.events.writeItem(event);
    }
} else void{};

/// TracingReader wraps a reader and depending if tracing is enabled, uses a TeeReader to write the data that is read to a writer.
///
/// This is currently only used in tests.
fn TracingReader(comptime ReaderType: type) type {
    return struct {
        const BufferType = fifo(u8, .{ .Static = 4 * 1024 * 1024 });

        const Reader = if (build_options.enable_tracing)
            TeeReader(ReaderType, BufferType.Writer).Reader
        else
            ReaderType;

        buffer: BufferType,
        underlying: if (build_options.enable_tracing)
            TeeReader(ReaderType, BufferType.Writer)
        else
            ReaderType,

        const init = if (build_options.enable_tracing)
            initTeeReader
        else
            initReader;

        fn initReader(self: *@This(), underlying: ReaderType) void {
            self.* = .{
                .buffer = undefined,
                .underlying = underlying,
            };
        }

        fn initTeeReader(self: *@This(), underlying: ReaderType) void {
            self.* = .{
                .buffer = BufferType.init(),
                .underlying = teeReader(underlying, self.buffer.writer()),
            };
        }

        fn reader(self: *@This()) Reader {
            if (comptime build_options.enable_tracing) {
                return self.underlying.reader();
            } else {
                return self.underlying;
            }
        }
    };
}

pub const Message = union(Opcode) {
    @"error": ErrorMessage,
    startup: StartupMessage,
    ready: ReadyMessage,
    authenticate: AuthenticateMessage,
    options: OptionsMessage,
    supported: SupportedMessage,
    query: QueryMessage,
    result: ResultMessage,
    prepare: PrepareMessage,
    execute: ExecuteMessage,
    register: RegisterMessage,
    event: EventMessage,
    batch: BatchMessage,
    auth_challenge: AuthChallengeMessage,
    auth_response: AuthResponseMessage,
    auth_success: AuthSuccessMessage,
};

gpa: mem.Allocator,
arena: heap.ArenaAllocator,

/// Use this arena for temporary allocations that don't outlive a function call.
scratch_arena: heap.ArenaAllocator,

/// The queue of messages that were read and correctly parsed.
queue: fifo(Message, .Dynamic),

/// The state the connection is in.
///
/// The handshake state is the first step to get a usable connection.
/// It is a multi step process and its state is tracked in `handshake_state`.
///
/// The nominal state is the state in which a connection will be the vast majority of the time.
/// In this state a connection can send queries, receive responses, register for events.
///
/// Finally the shutdown state indicates the connection will shut down.
///
/// Each call to `tick` handles this state machine.
state: enum {
    handshake,
    nominal,
    shutdown,
} = .handshake,

/// The state of the initial handshake.
///
/// This is used while a connection is in the handshake state to track _where_ in a handshake the connection is.
///
/// Each call to `tickInHandshake` (from a call to `tick`) handles this state machine.
/// See the `tickInHandshake` documentation for a sequence diagram of the handshake.
handshake_state: enum {
    options,
    supported,
    startup,
    authenticate_or_ready,
    auth_response,
    ready,
} = .options,

/// The framing state.
/// This MUST NOT be modified by the user.
///
/// Framing is specific to protocol version v5.
framing: struct {
    enabled: bool = false,
    format: Frame.Format = undefined,
} = .{},

/// The CQL version to use.
/// The protocol documentation says this must be 3.0.0 and nothing else.
cql_version: CQLVersion = CQLVersion{
    .major = 3,
    .minor = 0,
    .patch = 0,
},

/// The protocol version.
///
/// If you need to change the version, change this before any call to `tick`.
///
/// Defaults to v4.
/// The handshake will negotiate an appropriate value with the server that
/// is _at most_ this version.
protocol_version: ProtocolVersion = ProtocolVersion.v4,

/// The compression algorithm to use.
///
/// If the negotiated protocol version is <= v4, this can Snappy or LZ4.
/// If the negotiated protocol version is >  v4, this can only be LZ4.
///
/// Defaults to no compression.
compression: ?CompressionAlgorithm = null,

/// If tracing is enabled this will be used to trace messages written and read.
tracer: if (build_options.enable_tracing) *Tracer else struct {} = undefined,

/// This is the size of the memory we keep in the scratch allocator after we reset it.
///
/// 1MiB of memory is a good value to prevent too many allocations and also not increase memory usage too much.
const preferred_scratch_size = 1 * 1024 * 1024;

pub fn init(allocator: mem.Allocator) !*Self {
    var res = try allocator.create(Self);
    res.* = .{
        .gpa = allocator,
        .arena = heap.ArenaAllocator.init(allocator),
        .scratch_arena = heap.ArenaAllocator.init(allocator),
        .queue = undefined,
    };

    res.queue = fifo(Message, .Dynamic).init(res.arena.allocator());

    if (comptime build_options.enable_tracing) {
        res.tracer = try Tracer.init(allocator);
    }

    return res;
}

pub fn deinit(conn: *Self) void {
    conn.arena.deinit();
    conn.scratch_arena.deinit();
    if (comptime build_options.enable_tracing) {
        conn.tracer.deinit();
    }

    conn.gpa.destroy(conn);
}

/// Tick drives the connection state machines.
///
/// Depending on the current state it can:
/// * write new messages to `writer`
/// * read new messages from `reader`
/// The messages read are processed to drive the state machine.
///
/// There is no expectation that any message can be read from `reader`,
/// if there's nothing to read or the message is incomplete then the state
/// will simply not change and the next call to `tick` will try to make more progress.
pub fn tick(conn: *Self, writer: anytype, reader: anytype) !void {
    // TODO(vincent): diags

    switch (conn.state) {
        .handshake => {
            try conn.tickInHandshake(writer, reader);
        },
        .nominal => unreachable,
        .shutdown => unreachable,
    }
}

/// Handles the handshake state machine
///
/// The handshake looks like this:
///
/// Client                                   Server
///   |                                        |
///   |  OPTIONS                               |
///   |--------------------------------------->|
///   |                                        |
///   |  SUPPORTED                             |
///   |<---------------------------------------|
///   |                                        |
///   |  STARTUP                               |
///   |--------------------------------------->|
///   |                                        |
///   |   +-----------------------------+      |
///   |   | needs authentication        |      |
///   |   |                             |      |
///   |   |  AUTHENTICATE               |      |
///   |<--|-----------------------------|------|
///   |   |                             |      |
///   |   |  AUTH_RESPONSE              |      |
///   |---|-----------------------------|----->|
///   |   |                             |      |
///   |   |   +-------------------+     |      |
///   |   |   | opt follow up     |     |      |
///   |   |   |                   |     |      |
///   |   |   |  AUTH_CHALLENGE   |     |      |
///   |<--|---|-------------------------|------|
///   |   |   |                   |     |      |
///   |   |   |  AUTH_RESPONSE    |     |      |
///   |---|---|-------------------------|----->|
///   |   |   |                   |     |      |
///   |   |   +-------------------+     |      |
///   |   |                             |      |
///   |   |  AUTH_SUCCESS               |      |
///   |<--|-----------------------------|------|
///   |   |                             |      |
///   |   +-----------------------------+      |
///   |                                        |
///   |   +-----------------------------+      |
///   |   | else no authentication      |      |
///   |   |                             |      |
///   |   |  READY                      |      |
///   |<--|-----------------------------|------|
///   |   |                             |      |
///   |   +-----------------------------+      |
///   |                                        |
///
///
fn tickInHandshake(conn: *Self, writer: anytype, reader: anytype) !void {
    debug.assert(conn.state == .handshake);

    switch (conn.handshake_state) {
        .options => {
            try conn.appendMessage(writer, OptionsMessage{});

            conn.handshake_state = .supported;
        },
        .supported => {
            // TODO(vincent): correct allocator ?
            try conn.readMessagesNoEof(conn.arena.allocator(), reader);

            if (conn.queue.readItem()) |message| {
                const supported_message = switch (message) {
                    .supported => |tmp| tmp,
                    else => return error.UnexpectedMessageType,
                };

                conn.compression = supported_message.compression_algorithms[0];
                conn.cql_version = supported_message.cql_versions[0];

                // TODO(vincent): is this always sorted ?
                const usable_protocol_version = blk: {
                    var iter = mem.reverseIterator(supported_message.protocol_versions);
                    while (iter.next()) |tmp| {
                        if (conn.protocol_version.lessThan(tmp)) {
                            continue;
                        }

                        break :blk tmp;
                    } else return error.NoSupportedProtocolVersion;
                };
                conn.protocol_version = usable_protocol_version;

                conn.handshake_state = .startup;
            }
        },
        .startup => {
            try conn.appendMessage(writer, StartupMessage{
                .compression = conn.compression,
                .cql_version = conn.cql_version,
            });

            conn.handshake_state = .authenticate_or_ready;
        },
        .authenticate_or_ready => {
            // TODO(vincent): correct allocator ?
            try conn.readMessagesNoEof(conn.arena.allocator(), reader);
        },
        .auth_response => unreachable,
        .ready => unreachable,
    }
}

/// appendMessage writes a single message to the output
///
/// A message can be
///
/// If the .body field is present, it must me a type implementing either of the following write function:
///
///   fn write(protocol_version: ProtocolVersion, w: *MessageWriter) !void
///   fn write(w: *MessageWriter) !void
///
/// Some messages don't care about the protocol version so this is why the second signature is supported.
///
/// Additionally this method takes care of compression if enabled.
///
/// This method is not thread safe.
fn appendMessage(conn: *Self, writer: anytype, message: anytype) !void {
    const scratch_allocator = conn.scratch_arena.allocator();
    defer _ = conn.scratch_arena.reset(.{ .retain_with_limit = preferred_scratch_size });

    const MessageType = @TypeOf(message);

    //
    // Prepare the envelope
    //

    const opcode = comptime blk: {
        for (std.meta.tags(Opcode)) |tag| {
            const MessageTypeForOpcode = std.meta.TagPayload(Message, tag);
            if (MessageTypeForOpcode == MessageType) {
                break :blk tag;
            }
        } else {
            @compileError("message type " ++ @typeName(MessageType) ++ " is invalid");
        }
    };

    // TODO(vincent): handle stream and stuff
    var envelope = Envelope{
        .header = EnvelopeHeader{
            .version = conn.protocol_version,
            .flags = 0,
            .stream = 0,
            .opcode = opcode,
            .body_len = 0,
        },
        .body = &[_]u8{},
    };

    if (conn.protocol_version == .v5) {
        envelope.header.flags |= EnvelopeFlags.UseBeta;
    }

    var mw = try MessageWriter.init(scratch_allocator);
    defer mw.deinit();

    // Encode body
    if (std.meta.hasMethod(MessageType, "write")) {
        switch (@typeInfo(@TypeOf(MessageType.write))) {
            .@"fn" => |info| {
                if (info.params.len == 3) {
                    try message.write(conn.protocol_version, &mw);
                } else {
                    try message.write(&mw);
                }
            },
            else => unreachable,
        }
    }

    // This is the actual bytes of the encoded body.
    const written = mw.getWritten();

    // Default to using the uncompressed body.
    envelope.header.body_len = @intCast(written.len);
    envelope.body = written;

    // Compress the body if we can use it.
    // Only relevant for Protocol <= v4, Protocol v5 doest compression using the framing format.
    if (conn.protocol_version.lessThan(.v5)) {
        if (conn.compression) |compression| {
            switch (compression) {
                .LZ4 => {
                    const compressed_data = try lz4.compress(scratch_allocator, written);

                    envelope.header.flags |= EnvelopeFlags.Compression;
                    envelope.header.body_len = @intCast(compressed_data.len);
                    envelope.body = compressed_data;
                },
                .Snappy => {
                    const compressed_data = try snappy.compress(scratch_allocator, written);

                    envelope.header.flags |= EnvelopeFlags.Compression;
                    envelope.header.body_len = @intCast(compressed_data.len);
                    envelope.body = compressed_data;
                },
            }
        }
    }

    //
    // Write a frame if protocol v5
    // Write the envelope directly otherwise
    //

    var envelope_buffer = std.ArrayList(u8).init(scratch_allocator);
    const envelope_data = try protocol.writeEnvelope(envelope, &envelope_buffer);

    const final_payload = if (conn.framing.enabled)
        try Frame.encode(scratch_allocator, envelope_data, true, .uncompressed)
    else
        envelope_data;

    try writer.writeAll(final_payload);

    //
    // Debugging/Observability
    //

    if (comptime build_options.enable_logging) {
        log.info("[appendMessage] msg={any} data={s}", .{
            message,
            fmt.fmtSliceHexLower(final_payload),
        });
    }

    if (comptime build_options.enable_tracing) {
        const tmp = @unionInit(Message, @tagName(opcode), message);

        try conn.tracer.trace(TraceEvent{
            .message = tmp,
            .envelope = envelope,
            .data = final_payload,
        });
    }
}

fn readMessagesNoEof(conn: *Self, message_allocator: mem.Allocator, rd: anytype) !void {
    const scratch_allocator = conn.scratch_arena.allocator();
    defer _ = conn.scratch_arena.reset(.{ .retain_with_limit = preferred_scratch_size });

    while (true) {
        var reader: TracingReader(@TypeOf(rd)) = undefined;
        reader.init(rd);

        const envelope = if (conn.framing.enabled) blk: {
            const result = Frame.read(scratch_allocator, reader.reader(), conn.framing.format) catch |err| switch (err) {
                error.UnexpectedEOF => return,
                else => return err,
            };

            // TODO(vincent): handle non self contained frames
            debug.assert(result.frame.is_self_contained);
            debug.assert(result.frame.payload.len > 0);

            var fbs = io.StreamSource{ .const_buffer = io.fixedBufferStream(result.frame.payload) };
            break :blk try Envelope.read(scratch_allocator, fbs.reader());
        } else blk: {
            var envelope = Envelope.read(scratch_allocator, reader.reader()) catch |err| switch (err) {
                error.UnexpectedEOF => return,
                else => return err,
            };

            if (envelope.header.flags & EnvelopeFlags.Compression == EnvelopeFlags.Compression) {
                const compression = conn.compression orelse return error.InvalidCompressedFrame;

                switch (compression) {
                    .LZ4 => {
                        const decompressed_data = try lz4.decompress(scratch_allocator, envelope.body, envelope.body.len * 3);
                        envelope.body = decompressed_data;
                    },
                    .Snappy => {
                        const decompressed_data = try snappy.decompress(scratch_allocator, envelope.body);
                        envelope.body = decompressed_data;
                    },
                }
            }

            break :blk envelope;
        };

        const message = blk: {
            var mr = MessageReader.init(envelope.body);

            break :blk switch (envelope.header.opcode) {
                .@"error" => Message{ .@"error" = try ErrorMessage.read(message_allocator, &mr) },
                .startup => Message{ .startup = try StartupMessage.read(message_allocator, &mr) },
                .ready => Message{ .ready = ReadyMessage{} },
                .options => Message{ .options = OptionsMessage{} },
                .supported => Message{ .supported = try SupportedMessage.read(message_allocator, &mr) },
                .result => Message{ .result = try ResultMessage.read(message_allocator, envelope.header.version, &mr) },
                .register => Message{ .register = try RegisterMessage.read(message_allocator, &mr) },
                .event => Message{ .event = try EventMessage.read(message_allocator, &mr) },
                .authenticate => Message{ .authenticate = try AuthenticateMessage.read(message_allocator, &mr) },
                .auth_challenge => Message{ .auth_challenge = try AuthChallengeMessage.read(message_allocator, &mr) },
                .auth_success => Message{ .auth_success = try AuthSuccessMessage.read(message_allocator, &mr) },
                else => std.debug.panic("invalid read message {}\n", .{envelope.header.opcode}),
            };
        };
        try conn.queue.writeItem(message);

        if (comptime build_options.enable_logging) {
            const payload = reader.buffer.readableSlice(0);

            log.info("[readMessagesNoEof] msg={any} data={s}", .{
                message,
                fmt.fmtSliceHexLower(payload),
            });
        }

        if (comptime build_options.enable_tracing) {
            const payload = try conn.tracer.arena.allocator().dupe(u8, reader.buffer.readableSlice(0));
            try conn.tracer.trace(TraceEvent{
                .message = message,
                .envelope = envelope,
                .data = payload,
            });
        }
    }
}

/// TeeReader creates a reader type that wraps a reader and writes the data read to a writer.
fn TeeReader(comptime ReaderType: type, comptime WriterType: type) type {
    return struct {
        pub const Reader = std.io.Reader(*@This(), error{OutOfMemory}, readFn);

        child_reader: ReaderType,
        writer: WriterType,

        fn readFn(self: *@This(), dest: []u8) error{OutOfMemory}!usize {
            const n = try self.child_reader.read(dest);
            if (n == 0) return 0;

            const data = dest[0..n];
            try self.writer.writeAll(data);

            return n;
        }

        pub fn reader(self: *@This()) Reader {
            return .{ .context = self };
        }
    };
}

/// Create a new TeeReader reading from `reader` and writing to `writer`.
fn teeReader(reader: anytype, writer: anytype) TeeReader(@TypeOf(reader), @TypeOf(writer)) {
    return .{
        .child_reader = reader,
        .writer = writer,
    };
}

test teeReader {
    // var arena = testutils.arenaAllocator();
    // defer arena.deinit();
    // const allocator = arena.allocator();

    var fbs = io.fixedBufferStream("foobar");
    var out = fifo(u8, .{ .Static = 200 }).init();

    var tee_reader = teeReader(fbs.reader(), out.writer());
    var reader = tee_reader.reader();

    var buf: [20]u8 = undefined;
    const n = try reader.readAll(&buf);

    try testing.expectEqual(6, n);
    try testing.expectEqualStrings("foobar", buf[0..n]);
    try testing.expectEqualStrings("foobar", out.readableSlice(0));
}

fn collectTracingEvents(allocator: mem.Allocator, _: *Tracer) ![]const Message {
    var tmp = std.ArrayList(Message).init(allocator);

    return tmp.toOwnedSlice();

    // readMessagesNoEof()
}

test {
    const allocator = std.testing.allocator;

    // var messages_arena = testutils.arenaAllocator();
    // defer messages_arena.deinit();
    // const message_allocator = messages_arena.allocator();

    var conn = try Self.init(allocator);
    defer conn.deinit();
    conn.protocol_version = ProtocolVersion.v5;

    var write_buffer = fifo(u8, .Dynamic).init(allocator);
    defer write_buffer.deinit();

    var read_buffer = fifo(u8, .Dynamic).init(allocator);
    defer read_buffer.deinit();

    //
    // Handshake
    //

    // OPTIONS

    {
        try testing.expectEqual(.handshake, conn.state);
        try testing.expectEqual(.options, conn.handshake_state);

        try conn.tick(write_buffer.writer(), read_buffer.reader());

        const event = conn.tracer.events.readItem().?;
        try testing.expect(std.meta.activeTag(event.message) == .options);
    }

    // SUPPORTED
    {
        try testing.expectEqual(.handshake, conn.state);
        try testing.expectEqual(.supported, conn.handshake_state);

        const data = "\x84\x00\x00\x09\x06\x00\x00\x00\x60\x00\x03\x00\x11\x50\x52\x4f\x54\x4f\x43\x4f\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x53\x00\x03\x00\x04\x33\x2f\x76\x33\x00\x04\x34\x2f\x76\x34\x00\x09\x35\x2f\x76\x35\x2d\x62\x65\x74\x61\x00\x0b\x43\x4f\x4d\x50\x52\x45\x53\x53\x49\x4f\x4e\x00\x02\x00\x06\x73\x6e\x61\x70\x70\x79\x00\x03\x6c\x7a\x34\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x01\x00\x05\x33\x2e\x30\x2e\x30";
        try read_buffer.write(data);

        try conn.tick(write_buffer.writer(), read_buffer.reader());

        try testing.expectEqual(ProtocolVersion.v5, conn.protocol_version);
        try testing.expectEqual(0, conn.queue.readableLength());

        const message = conn.tracer.events.readItem().?.message.supported;
        try testing.expectEqualSlices(ProtocolVersion, &[_]ProtocolVersion{ .v3, .v4, .v5 }, message.protocol_versions);
        try testing.expectEqualSlices(CQLVersion, &[_]CQLVersion{CQLVersion{ .major = 3, .minor = 0, .patch = 0 }}, message.cql_versions);
        try testing.expectEqualSlices(CompressionAlgorithm, &[_]CompressionAlgorithm{ .Snappy, .LZ4 }, message.compression_algorithms);
    }

    // STARTUP
    {
        try testing.expectEqual(.handshake, conn.state);
        try testing.expectEqual(.startup, conn.handshake_state);

        try conn.tick(write_buffer.writer(), read_buffer.reader());

        const message = conn.tracer.events.readItem().?.message.startup;
        try testing.expectEqual(conn.cql_version, message.cql_version);
        try testing.expectEqual(conn.compression, message.compression);
    }

    // Read READY
    {
        try testing.expectEqual(.handshake, conn.state);
        try testing.expectEqual(.authenticate_or_ready, conn.handshake_state);

        const data = "\x84\x00\x00\x02\x02\x00\x00\x00\x00";
        try read_buffer.write(data);

        try conn.tick(write_buffer.writer(), read_buffer.reader());

        _ = conn.tracer.events.readItem().?.message.ready;
    }

    // Switch to framing
    conn.framing.enabled = true;
    conn.framing.format = .compressed;

    //
    // Querying
    //

    // Write QUERY
    // try conn.appendMessage(Message{
    //     .query = QueryMessage{
    //         .query = "SELECT * FROM foobar.user",
    //         .query_parameters = QueryParameters{
    //             .consistency_level = .All,
    //             .values = null,
    //             .skip_metadata = false,
    //             .page_size = null,
    //             .paging_state = null,
    //             .serial_consistency_level = null,
    //             .timestamp = null,
    //             .keyspace = null,
    //             .now_in_seconds = null,
    //         },
    //     },
    // });
}
