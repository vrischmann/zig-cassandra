const std = @import("std");
const debug = std.debug;
const heap = std.heap;
const fmt = std.fmt;
const io = std.io;
const mem = std.mem;
const testing = std.testing;
const fifo = std.fifo.LinearFifo;
const build_options = @import("build_options");

const lz4 = @import("lz4.zig");
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
const Iterator = @import("Iterator.zig");

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

        const Reader = if (build_options.enable_tracing or build_options.enable_logging)
            TeeReader(ReaderType, BufferType.Writer).Reader
        else
            ReaderType;

        buffer: BufferType,
        underlying: if (build_options.enable_tracing or build_options.enable_logging)
            TeeReader(ReaderType, BufferType.Writer)
        else
            ReaderType,

        const init = if (build_options.enable_tracing or build_options.enable_logging)
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
            if (comptime build_options.enable_tracing or build_options.enable_logging) {
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

fn messageFormatter(message: Message) fmt.Formatter(formatMessage) {
    return .{ .data = message };
}

fn formatMessage(message: Message, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
    switch (message) {
        .@"error" => |msg| {
            try writer.print("ERROR::[error_code={s} message={s}]", .{
                @tagName(msg.error_code),
                msg.message,
            });
        },
        .startup => |msg| {
            try writer.print("STARTUP::[cql_version={?} compression={?}]", .{
                msg.cql_version,
                msg.compression,
            });
        },
        .ready => |_| {
            try writer.print("READY::[]", .{});
        },
        .authenticate => |msg| {
            try writer.print("AUTHENTICATE::[authenticator={s}]", .{
                msg.authenticator,
            });
        },
        .options => |_| {
            try writer.print("OPTIONS::[]", .{});
        },
        .supported => |msg| {
            try writer.print("SUPPORTED::[protocol_versions={s} cql_versions={s} compression_algorithms={s}]", .{
                msg.protocol_versions,
                msg.cql_versions,
                msg.compression_algorithms,
            });
        },
        .query => |msg| {
            try writer.print("QUERY::[query={s} query_parameters={any}]", .{
                msg.query,
                msg.query_parameters,
            });
        },
        .result => |msg| switch (msg.result) {
            .void => {
                try writer.print("RESULT/void::[]", .{});
            },
            .rows => |rows| {
                try writer.print("RESULT/rows::[nb_rows={d}]", .{rows.data.len});
            },
            .set_keyspace => |keyspace| {
                try writer.print("RESULT/set_keyspace::[keyspace={s}]", .{
                    keyspace,
                });
            },
            .prepared => |prepared| {
                try writer.print("RESULT/prepared::[query_id={s}, result_metadata_id={?s}]", .{
                    prepared.query_id,
                    prepared.result_metadata_id,
                });
            },
            .schema_change => |event| {
                try writer.print("RESULT/event::[target={s} type={s} options_arguments={?s}, options_keyspace={s} options_object_name={s}]", .{
                    @tagName(event.target),
                    @tagName(event.type),
                    event.options.arguments,
                    event.options.keyspace,
                    event.options.object_name,
                });
            },
        },
        else => try writer.print("{any}", .{message}),
    }
}

gpa: mem.Allocator,
arena: heap.ArenaAllocator,

/// Use this arena for temporary allocations that don't outlive a function call.
scratch_arena: heap.ArenaAllocator,

/// This buffer contains the data to be written out to the Cassandra node.
/// TODO(vincent): does this need to be dynamic ?
write_buffer: fifo(u8, .Dynamic) = undefined,
/// This buffer contains the data that was read from the Cassandra node that still needs to be parsed.
/// TODO(vincent): does this need to be dynamic ?
read_buffer: fifo(u8, .Dynamic) = undefined,
/// The queue of messages that were read and correctly parsed.
/// TODO(vincent): does this need to be dynamic ?
queue: fifo(Message, .Dynamic) = undefined,

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
compression: CompressionAlgorithm = .none,

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
        // TODO(vincent): which allocator to use ?
        .write_buffer = fifo(u8, .Dynamic).init(allocator),
        .read_buffer = fifo(u8, .Dynamic).init(allocator),
        .queue = fifo(Message, .Dynamic).init(allocator),
    };

    if (comptime build_options.enable_tracing) {
        res.tracer = try Tracer.init(allocator);
    }

    return res;
}

pub fn deinit(conn: *Self) void {
    conn.write_buffer.deinit();
    conn.read_buffer.deinit();
    conn.queue.deinit();

    conn.arena.deinit();
    conn.scratch_arena.deinit();
    if (comptime build_options.enable_tracing) {
        conn.tracer.deinit();
    }

    conn.gpa.destroy(conn);
}

pub fn doQuery(conn: *Self, query: []const u8, query_parameters: QueryParameters) !void {
    try conn.appendMessage(QueryMessage{
        .query = query,
        .query_parameters = query_parameters,
    });
}

pub fn feedReadable(conn: *Self, data: []const u8) !void {
    try conn.read_buffer.write(data);
}

pub fn getWritable(conn: *Self) []const u8 {
    return conn.write_buffer.readableSlice(0);
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
pub fn tick(conn: *Self) !void {
    // TODO(vincent): diags

    switch (conn.state) {
        .handshake => {
            try conn.tickInHandshake();
        },
        .nominal => {
            try conn.tickNominal();
        },
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
fn tickInHandshake(conn: *Self) !void {
    debug.assert(conn.state == .handshake);

    const previous_handshake_state = conn.handshake_state;
    defer if (conn.handshake_state != previous_handshake_state) {
        log.debug("transitioning handshake from {s} to {s}", .{
            @tagName(previous_handshake_state),
            @tagName(conn.handshake_state),
        });
    };

    switch (conn.handshake_state) {
        .options => {
            try conn.appendMessage(OptionsMessage{});

            conn.handshake_state = .supported;
        },
        .supported => {
            // TODO(vincent): correct allocator ?
            try conn.readMessagesNoEof(conn.arena.allocator());

            if (conn.queue.readItem()) |message| {
                const supported_message = switch (message) {
                    .supported => |tmp| tmp,
                    else => {
                        // TODO(vincent): diags
                        return error.UnexpectedMessageType;
                    },
                };

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

                if (conn.protocol_version.isAtLeast(.v5)) {
                    // Enable framing
                    conn.framing.enabled = true;
                    conn.framing.format = .uncompressed;

                    conn.compression = .lz4;
                } else {
                    conn.compression = supported_message.compression_algorithms[0];
                }

                conn.handshake_state = .startup;
            }
        },
        .startup => {
            try conn.appendMessage(StartupMessage{
                .compression = conn.compression,
                .cql_version = conn.cql_version,
            });

            conn.handshake_state = .authenticate_or_ready;
        },
        .authenticate_or_ready => {
            // TODO(vincent): correct allocator ?
            try conn.readMessagesNoEof(conn.arena.allocator());

            if (conn.queue.readItem()) |message| {
                switch (message) {
                    .ready => |_| {
                        conn.handshake_state = .ready;
                        conn.state = .nominal;
                    },
                    .authenticate => |_| {},
                    .@"error" => |_| {
                        // TODO(vincent): diags
                        return error.UnexpectedMessageType;
                    },
                    else => {
                        // TODO(vincent): diags
                        return error.UnexpectedMessageType;
                    },
                }
            }
        },
        .auth_response => unreachable,
        .ready => {
            conn.state = .nominal;
        },
    }
}

fn tickNominal(conn: *Self) !void {
    try conn.readMessagesNoEof(conn.arena.allocator());

    while (conn.queue.readItem()) |message| {
        _ = message;
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
fn appendMessage(conn: *Self, message: anytype) !void {
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
        envelope.header.flags |= EnvelopeFlags.use_beta;
    }

    //
    // Encode body
    //

    var mw = try MessageWriter.init(scratch_allocator);
    defer mw.deinit();

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

    envelope.header.body_len = @intCast(written.len);
    envelope.body = written;

    //
    // Compress the envelope body if protocol <= v4
    // Protocol v5 does compression using the framing format.
    //
    // Compression is not allowed on OPTIONS and STARTUP message because the client and server have not yet negotiated the compression algorithm.
    //

    if (conn.protocol_version.lessThan(.v5) and opcode != .options and opcode != .startup) {
        switch (conn.compression) {
            .lz4 => {
                const compressed_data = try lz4.compress(scratch_allocator, written);

                envelope.header.flags |= EnvelopeFlags.compression;
                envelope.header.body_len = @intCast(compressed_data.len);
                envelope.body = compressed_data;
            },
            .snappy => {
                const compressed_data = try snappy.compress(scratch_allocator, written);

                envelope.header.flags |= EnvelopeFlags.compression;
                envelope.header.body_len = @intCast(compressed_data.len);
                envelope.body = compressed_data;
            },
            .none => {},
        }
    }

    //
    // Write a frame if protocol v5
    // Write the envelope directly otherwise
    //

    var envelope_buffer = std.ArrayList(u8).init(scratch_allocator);
    const envelope_data = try protocol.writeEnvelope(envelope, &envelope_buffer);

    const final_payload = if (conn.framing.enabled)
        // TODO(vincent): handle self contained
        try Frame.encode(scratch_allocator, envelope_data, true, conn.framing.format)
    else
        envelope_data;

    try conn.write_buffer.write(final_payload);

    //
    // Debugging/Observability
    //

    if (comptime build_options.enable_logging) {
        const tmp = @unionInit(Message, @tagName(opcode), message);

        // TODO(vincent): custom formatting
        log.info("[appendMessage] msg={any} envelope_data={s} data={s} framing={}", .{
            messageFormatter(tmp),
            fmt.fmtSliceHexLower(envelope_data),
            fmt.fmtSliceHexLower(final_payload),
            conn.framing.enabled,
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

fn readMessagesNoEof(conn: *Self, message_allocator: mem.Allocator) !void {
    const scratch_allocator = conn.scratch_arena.allocator();
    defer _ = conn.scratch_arena.reset(.{ .retain_with_limit = preferred_scratch_size });

    const rd = conn.read_buffer.reader();

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
            const tmp = try Envelope.read(scratch_allocator, fbs.reader(), .none);

            break :blk tmp;
        } else blk: {
            const tmp = Envelope.read(scratch_allocator, reader.reader(), conn.compression) catch |err| switch (err) {
                error.UnexpectedEOF => return,
                else => return err,
            };

            break :blk tmp;
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

        //
        // Observability / debugging
        //

        if (comptime build_options.enable_logging) {
            const payload = reader.buffer.readableSlice(0);

            log.info("[readMessagesNoEof] msg={any} data={s}", .{
                messageFormatter(message),
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
        const Error = error{OutOfMemory} || std.posix.ReadError;
        pub const Reader = std.io.Reader(*@This(), Error, readFn);

        child_reader: ReaderType,
        writer: WriterType,

        fn readFn(self: *@This(), dest: []u8) Error!usize {
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

test "protocol v4" {
    const allocator = std.testing.allocator;

    // var messages_arena = testutils.arenaAllocator();
    // defer messages_arena.deinit();
    // const message_allocator = messages_arena.allocator();

    var conn = try Self.init(allocator);
    defer conn.deinit();
    conn.protocol_version = ProtocolVersion.v4;

    //
    // Handshake
    //

    // OPTIONS

    {
        try testing.expectEqual(.handshake, conn.state);
        try testing.expectEqual(.options, conn.handshake_state);

        try conn.tick();

        const event = conn.tracer.events.readItem().?;
        try testing.expect(std.meta.activeTag(event.message) == .options);

        //

        const written = try conn.write_buffer.toOwnedSlice();
        defer allocator.free(written);

        try testing.expectEqualSlices(u8, "\x04\x00\x00\x00\x05\x00\x00\x00\x00", written);
    }

    try testing.expect(conn.read_buffer.readableLength() == 0);
    try testing.expect(conn.write_buffer.readableLength() == 0);

    // SUPPORTED
    {
        try testing.expectEqual(.handshake, conn.state);
        try testing.expectEqual(.supported, conn.handshake_state);

        const data = "\x84\x00\x00\x09\x06\x00\x00\x00\x60\x00\x03\x00\x11\x50\x52\x4f\x54\x4f\x43\x4f\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x53\x00\x03\x00\x04\x33\x2f\x76\x33\x00\x04\x34\x2f\x76\x34\x00\x09\x35\x2f\x76\x35\x2d\x62\x65\x74\x61\x00\x0b\x43\x4f\x4d\x50\x52\x45\x53\x53\x49\x4f\x4e\x00\x02\x00\x06\x73\x6e\x61\x70\x70\x79\x00\x03\x6c\x7a\x34\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x01\x00\x05\x33\x2e\x30\x2e\x30";
        try conn.feedReadable(data);

        try conn.tick();

        try testing.expectEqual(ProtocolVersion.v4, conn.protocol_version);
        try testing.expectEqual(0, conn.queue.readableLength());

        const message = conn.tracer.events.readItem().?.message.supported;
        try testing.expectEqualSlices(ProtocolVersion, &[_]ProtocolVersion{ .v3, .v4, .v5 }, message.protocol_versions);
        try testing.expectEqualSlices(CQLVersion, &[_]CQLVersion{CQLVersion{ .major = 3, .minor = 0, .patch = 0 }}, message.cql_versions);
        try testing.expectEqualSlices(CompressionAlgorithm, &[_]CompressionAlgorithm{ .snappy, .lz4 }, message.compression_algorithms);
    }

    try testing.expect(conn.read_buffer.readableLength() == 0);
    try testing.expect(conn.write_buffer.readableLength() == 0);

    // STARTUP
    {
        try testing.expectEqual(.handshake, conn.state);
        try testing.expectEqual(.startup, conn.handshake_state);

        try conn.tick();

        const message = conn.tracer.events.readItem().?.message.startup;
        try testing.expectEqual(conn.cql_version, message.cql_version);
        try testing.expectEqual(conn.compression, message.compression);

        //

        const written = try conn.write_buffer.toOwnedSlice();
        defer allocator.free(written);

        // TODO(vincent): check the actual payload
        try testing.expect(written.len > 0);
    }

    try testing.expect(conn.read_buffer.readableLength() == 0);
    try testing.expect(conn.write_buffer.readableLength() == 0);

    // Read READY
    {
        try testing.expectEqual(.handshake, conn.state);
        try testing.expectEqual(.authenticate_or_ready, conn.handshake_state);

        const data = "\x84\x01\x00\x00\x02\x00\x00\x00\x01\x00";
        try conn.feedReadable(data);

        try conn.tick();

        _ = conn.tracer.events.readItem().?.message.ready;
    }

    // Do QUERY
    {
        try testing.expectEqual(.nominal, conn.state);
        try testing.expectEqual(.ready, conn.handshake_state);

        const query = "select age from foobar.age_to_ids limit 1;";
        const query_parameters = QueryParameters{
            .consistency_level = .One,
            .values = null,
            .skip_metadata = false,
            .page_size = null,
            .paging_state = null,
            .serial_consistency_level = null,
            .timestamp = null,
            .keyspace = null,
            .now_in_seconds = null,
        };

        try conn.doQuery(query, query_parameters);
        try conn.tick();

        const message = conn.tracer.events.readItem().?.message.query;
        try testing.expectEqual(query, message.query);
        try testing.expectEqual(query_parameters, message.query_parameters);

        //

        const written = try conn.write_buffer.toOwnedSlice();
        defer allocator.free(written);

        try testing.expectEqualSlices(u8, "\x04\x01\x00\x00\x07\x00\x00\x00\x33\x31\xc0\x00\x00\x00\x2a\x73\x65\x6c\x65\x63\x74\x20\x61\x67\x65\x20\x66\x72\x6f\x6d\x20\x66\x6f\x6f\x62\x61\x72\x2e\x61\x67\x65\x5f\x74\x6f\x5f\x69\x64\x73\x20\x6c\x69\x6d\x69\x74\x20\x31\x3b\x00\x01\x00", written);
    }

    // Read RESULT
    {
        try testing.expectEqual(.nominal, conn.state);
        try testing.expectEqual(.ready, conn.handshake_state);

        const data = "\x84\x01\x00\x00\x08\x00\x00\x00\x33\x33\x1c\x00\x00\x00\x02\x00\x00\x00\x01\x05\x04\x94\x06\x66\x6f\x6f\x62\x61\x72\x00\x0a\x61\x67\x65\x5f\x74\x6f\x5f\x69\x64\x73\x00\x03\x61\x67\x65\x00\x09\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x25\xa8";
        try conn.feedReadable(data);

        try conn.tick();

        const message: ResultMessage = conn.tracer.events.readItem().?.message.result;
        const rows = message.result.rows;
        try testing.expectEqual(1, rows.data.len);

        var iterator = Iterator.init(rows.metadata, rows.data);

        const Row = struct {
            n: i64,
        };
        var row: Row = undefined;

        try testing.expect(
            try iterator.scan(allocator, .{}, &row),
        );
        try testing.expectEqual(9640, row.n);
    }
}
