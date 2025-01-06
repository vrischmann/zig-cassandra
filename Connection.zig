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
    defer if (comptime build_options.enable_logging) {
        if (conn.handshake_state != previous_handshake_state) {
            log.debug("transitioning handshake from {s} to {s}", .{
                @tagName(previous_handshake_state),
                @tagName(conn.handshake_state),
            });
        }
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

                // The connection is setup: send the STARTUP message

                try conn.appendMessage(StartupMessage{
                    .compression = conn.compression,
                    .cql_version = conn.cql_version,
                });

                conn.handshake_state = .authenticate_or_ready;
            }
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
    // TODO(vincent): how do we clear the memory after the message is consumed ??
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

    while (conn.read_buffer.readableLength() > 0) {
        // NOTE(vincent): this looks weird but it's necessary, readableSlice() doesn't work like you would think (at least like _I_ would think).
        // In some cases realignment is necessary via realign() and readableSlice() doesn't do it but readableSliceOfLen() does.
        const readable = conn.read_buffer.readableSliceOfLen(conn.read_buffer.readableLength());

        const envelope, const consumed = if (conn.framing.enabled) blk: {
            const result = Frame.decode(scratch_allocator, readable, conn.framing.format) catch |err| switch (err) {
                error.UnexpectedEOF => return,
                else => return err,
            };

            // TODO(vincent): handle non self contained frames
            debug.assert(result.frame.is_self_contained);
            debug.assert(result.frame.payload.len > 0);

            // TODO(vincent): temporary
            var fbs = io.StreamSource{ .const_buffer = io.fixedBufferStream(result.frame.payload) };

            const result2 = try Envelope.read(scratch_allocator, fbs.reader(), .none);

            break :blk .{
                result2.envelope,
                result.consumed,
            };
        } else blk: {
            // TODO(vincent): temporary
            var fbs = io.StreamSource{ .const_buffer = io.fixedBufferStream(readable) };

            const result = Envelope.read(scratch_allocator, fbs.reader(), conn.compression) catch |err| switch (err) {
                error.UnexpectedEOF => return,
                else => return err,
            };

            break :blk .{
                result.envelope,
                result.consumed,
            };
        };

        defer conn.read_buffer.discard(consumed);

        //

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
            log.info("[readMessagesNoEof] msg={any} data={s}", .{
                messageFormatter(message),
                fmt.fmtSliceHexLower(readable),
            });
        }

        if (comptime build_options.enable_tracing) {
            const payload = try conn.tracer.arena.allocator().dupe(u8, readable);
            try conn.tracer.trace(TraceEvent{
                .message = message,
                .envelope = envelope,
                .data = payload,
            });
        }
    }
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

test "split reads, compressed payload" {
    var arena = heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const data = "\x84\x01\x00\x00\x08\x00\x00\x03\xfa\xeb\x0d\x1c\x00\x00\x00\x02\x00\x00\x00\x01\x05\x04\x98\x06\x66\x6f\x6f\x62\x61\x72\x00\x0a\x61\x67\x65\x5f\x74\x6f\x5f\x69\x64\x73\x00\x03\x61\x67\x65\x00\x09\x00\x00\x00\xd8\x00\x00\x00\x04\x00\x00\x25\xa8\x00\x05\x08\x04\x12\xde\x09\x10\x04\x10\x18\x09\x08\x04\x1d\x74\x09\x08\x04\x12\x2a\x09\x08\x04\x06\x2c\x09\x08\x04\x17\x0c\x09\x08\x04\x18\xf6\x09\x08\x04\x02\x94\x09\x08\x04\x1c\xac\x09\x08\x00\x07\x0d\x10\x04\x00\x6e\x09\x10\x04\x1d\x1a\x09\x08\x04\x0b\x22\x09\x08\x04\x20\x58\x09\x08\x04\x0f\xaa\x09\x08\x04\x17\xe8\x09\x08\x04\x01\x36\x09\x08\x04\x12\x16\x09\x08\x04\x1d\xe2\x09\x08\x04\x0a\x82\x09\x08\x00\x24\x0d\x80\x04\x1b\x08\x09\x10\x04\x1a\x68\x09\x08\x04\x10\xa4\x09\x08\x04\x19\xdc\x09\x08\x00\x0d\x0d\xb0\x04\x14\x64\x09\x10\x04\x00\x78\x09\x08\x00\x05\x0d\x70\x04\x15\x54\x09\x10\x04\x15\xea\x09\x08\x04\x14\xe6\x09\x08\x04\x20\x30\x09\x08\x00\x15\x0d\x68\x04\x1f\xb8\x0d\x10\x00\xae\x09\x08\x04\x16\x76\x09\x08\x04\x05\xbe\x09\x08\x00\x23\x0d\x08\x00\x04\x0d\xa8\x04\x13\xa6\x09\x18\x04\x13\x88\x09\x08\x04\x20\xa8\x09\x08\x04\x00\x8c\x09\x08\x04\x03\x5c\x09\x08\x04\x1c\x7a\x09\x08\x04\x14\x14\x09\x08\x04\x17\x84\x09\x08\x04\x05\x28\x09\x08\x04\x14\xa0\x09\x08\x04\x20\x9e\x09\x08\x04\x0c\x12\x09\x08\x00\x02\x0d\x08\x04\x03\xc0\x09\x10\x04\x09\xb0\x09\x08\x04\x08\x8e\x09\x08\x04\x26\x66\x09\x08\x00\x21\x2d\x88\x00\x1a\x2d\x50\x04\x13\x9c\x09\x18\x04\x0f\x32\x09\x08\x00\x1e\x2d\x10\x04\x1e\x46\x09\x10\x04\x1a\x04\x09\x08\x04\x20\x1c\x09\x08\x00\x16\x0d\x68\x00\x1d\x0d\xd0\x00\x1b\x2d\xb0\x04\x16\x8a\x09\x20\x04\x0e\x60\x09\x08\x04\x15\xfe\x09\x08\x00\x20\x2d\xf0\x04\x12\x52\x09\x10\x04\x0b\x90\x09\x08\x00\x09\x2d\x18\x04\x15\xd6\x09\x10\x04\x23\x50\x09\x08\x00\x03\x41\x9f\x0c\x04\x00\x00\x26\x2d\x08\x00\x19\x2d\x98\x00\x0b\x2d\xc8\x04\x01\x7c\x09\x28\x04\x11\x6c\x09\x08\x00\x01\x2d\xb0\x04\x22\x42\x09\x10\x04\x12\xa2\x0d\x08\x00\x98\x09\x08\x00\x03\x4d\x90\x00\x20\x2d\xa0\x00\x11\x0d\xc8\x00\x05\x0d\xf0\x00\x1f\x4d\x28\x04\x1b\x62\x09\x30\x04\x22\x38\x09\x08\x00\x01\x0d\x70\x00\x19\x2d\x10\x00\x1a\x2d\xe8\x00\x10\x0d\x80\x00\x1e\x0d\xb0\x00\x14\x0d\x20\x04\x13\x10\x09\x38\x04\x0c\xb2\x09\x08\x04\x0a\xb4\x09\x08\x04\x11\xf8\x09\x08\x04\x06\x5e\x09\x08\x00\x1b\x4d\x48\x04\x01\x40\x09\x10\x04\x09\x6a\x09\x08\x00\x00\x0d\x90\x04\x0a\xd2\x09\x10\x00\x0f\x0d\x08\x04\x05\x96\x09\x10\x00\x06\x2d\x88\x04\x08\x70\x09\x10\x04\x13\xec\x09\x08\x04\x12\x48\x09\x08\x00\x04\x6d\xb0\x04\x21\xca\x09\x10\x00\x1e\x4d\xe0\x04\x1c\xfc\x09\x10\x04\x17\x3e\x0d\x08\x0d\x40\x00\x0e\x0d\x30\x04\x0c\x44\x09\x18\x00\x1e\x6d\x20\x04\x00\x0a\x09\x10\x04\x24\xfe\x09\x08\x04\x26\xf2\x09\x08\x00\x1e\x4d\x80\x00\x09\x0d\x78\x04\x09\x4c\x09\x18\x00\x0d\x4d\xb8\x00\x12\x0d\x60\x00\x0c\x4d\x10\x00\x0a\x4d\xd8\x04\x11\xd0\x09\x28\x00\x26\x6d\xb8\x00\x0b\x2d\x40\x00\x0d\x0d\xb8\x00\x22\x6d\x10\x04\x12\xd4\x09\x28\x00\x15\x0d\xe8\x04\x23\x28\x09\x10\x00\x0f\x31\x60\x2d\xe0\x00\x05\x2d\x78\x00\x25\x2d\x48\x00\x25\x2d\x70\x00\x23\x6d\x30\x04\x00\x3c\x09\x38\x00\x21\x4d\xf0\x04\x03\x34\x09\x10\x00\x24\x2d\xc0\x00\x0f\x8d\x30\x00\x23\x0d\xf0\x00\x1e\x2d\x60\x00\x19\x0d\x38\x04\x11\xda\x09\x30\x04\x12\xb6\x09\x08\x00\x21\x2d\x00\x00\x06\x4d\x28\x00\x0c\x4d\xd0\x00\x26\x8d\x98\x00\x09\x8d\xe8\x04\x0b\x9a\x09\x30\x00\x25\x2d\xf0\x00\x02\x0d\xf8\x00\x06\x8d\x48\x00\x20\x0d\x10\x00\x1c\x2d\x28\x00\x0a\x8d\x58\x04\x05\x46\x09\x38\x04\x20\x3a\x09\x08\x00\x1a\x4d\x28\x04\x01\x72\x09\x10\x00\x12\x2d\xb0\x00\x0f\x0d\xa0\x04\x01\xf4\x09\x18\x04\x22\x56\x09\x08\x00\x18\xad\x40\x00\x1a\x4d\x50\x00\x1c\x0d\xb0\x04\x1a\x86\x09\x20\x00\x22\x2d\xb0\x00\x0f\x8d\x38\x00\x25\x2d\xf0\x00\x14\x0d\x50\x00\x02\x8d\x48\x00\x16\x2d\x50\x00\x02\x0d\x88\x00\x0d\x6d\x88\x00\x01\x0d\xf8\x00\x12\x8d\x88\x00\x00\x4d\x98\x04\x23\x5a\x0d\x60\xcd\x08\x00\x21\x71\x78\x0d\x80\x00\x18\x6d\x48\x00\x0e\x0d\x20\x00\x10\x2d\x08\x04\x14\x1e\x09\x38\x04\x11\x80\x09\x08\x00\x17\x4d\x18\x04\x0e\x24\x09\x10\x00\x18\x4d\x98\x04\x23\xfa\x09\x10\x00\x01\x0d\x38\x00\x24\xb1\x70\x00\x4a\x09\x18\x00\x18\x8d\xb8\x00\x26\x0d\x98\x00\x02\x8d\x80\x24\x01\x4a\x00\x00\x00\x04\x00";
    const data2 = "\x00\x00\x80";

    var conn = try Self.init(allocator);
    conn.compression = .snappy;
    defer conn.deinit();

    try conn.feedReadable(data);
    try conn.feedReadable(data2);
    try conn.readMessagesNoEof(allocator);

    try testing.expectEqual(1, conn.tracer.events.readableLength());

    const message: ResultMessage = conn.tracer.events.readItem().?.message.result;
    const rows = message.result.rows;
    try testing.expectEqual(216, rows.data.len);
}

test "compressed payload without compression enabled" {
    var arena = heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const data = "\x84\x01\x00\x00\x08\x00\x00\x03\xfa\xeb\x0d\x1c\x00\x00\x00\x02\x00\x00\x00\x01\x05\x04\x98\x06\x66\x6f\x6f\x62\x61\x72\x00\x0a\x61\x67\x65\x5f\x74\x6f\x5f\x69\x64\x73\x00\x03\x61\x67\x65\x00\x09\x00\x00\x00\xd8\x00\x00\x00\x04\x00\x00\x25\xa8\x00\x05\x08\x04\x12\xde\x09\x10\x04\x10\x18\x09\x08\x04\x1d\x74\x09\x08\x04\x12\x2a\x09\x08\x04\x06\x2c\x09\x08\x04\x17\x0c\x09\x08\x04\x18\xf6\x09\x08\x04\x02\x94\x09\x08\x04\x1c\xac\x09\x08\x00\x07\x0d\x10\x04\x00\x6e\x09\x10\x04\x1d\x1a\x09\x08\x04\x0b\x22\x09\x08\x04\x20\x58\x09\x08\x04\x0f\xaa\x09\x08\x04\x17\xe8\x09\x08\x04\x01\x36\x09\x08\x04\x12\x16\x09\x08\x04\x1d\xe2\x09\x08\x04\x0a\x82\x09\x08\x00\x24\x0d\x80\x04\x1b\x08\x09\x10\x04\x1a\x68\x09\x08\x04\x10\xa4\x09\x08\x04\x19\xdc\x09\x08\x00\x0d\x0d\xb0\x04\x14\x64\x09\x10\x04\x00\x78\x09\x08\x00\x05\x0d\x70\x04\x15\x54\x09\x10\x04\x15\xea\x09\x08\x04\x14\xe6\x09\x08\x04\x20\x30\x09\x08\x00\x15\x0d\x68\x04\x1f\xb8\x0d\x10\x00\xae\x09\x08\x04\x16\x76\x09\x08\x04\x05\xbe\x09\x08\x00\x23\x0d\x08\x00\x04\x0d\xa8\x04\x13\xa6\x09\x18\x04\x13\x88\x09\x08\x04\x20\xa8\x09\x08\x04\x00\x8c\x09\x08\x04\x03\x5c\x09\x08\x04\x1c\x7a\x09\x08\x04\x14\x14\x09\x08\x04\x17\x84\x09\x08\x04\x05\x28\x09\x08\x04\x14\xa0\x09\x08\x04\x20\x9e\x09\x08\x04\x0c\x12\x09\x08\x00\x02\x0d\x08\x04\x03\xc0\x09\x10\x04\x09\xb0\x09\x08\x04\x08\x8e\x09\x08\x04\x26\x66\x09\x08\x00\x21\x2d\x88\x00\x1a\x2d\x50\x04\x13\x9c\x09\x18\x04\x0f\x32\x09\x08\x00\x1e\x2d\x10\x04\x1e\x46\x09\x10\x04\x1a\x04\x09\x08\x04\x20\x1c\x09\x08\x00\x16\x0d\x68\x00\x1d\x0d\xd0\x00\x1b\x2d\xb0\x04\x16\x8a\x09\x20\x04\x0e\x60\x09\x08\x04\x15\xfe\x09\x08\x00\x20\x2d\xf0\x04\x12\x52\x09\x10\x04\x0b\x90\x09\x08\x00\x09\x2d\x18\x04\x15\xd6\x09\x10\x04\x23\x50\x09\x08\x00\x03\x41\x9f\x0c\x04\x00\x00\x26\x2d\x08\x00\x19\x2d\x98\x00\x0b\x2d\xc8\x04\x01\x7c\x09\x28\x04\x11\x6c\x09\x08\x00\x01\x2d\xb0\x04\x22\x42\x09\x10\x04\x12\xa2\x0d\x08\x00\x98\x09\x08\x00\x03\x4d\x90\x00\x20\x2d\xa0\x00\x11\x0d\xc8\x00\x05\x0d\xf0\x00\x1f\x4d\x28\x04\x1b\x62\x09\x30\x04\x22\x38\x09\x08\x00\x01\x0d\x70\x00\x19\x2d\x10\x00\x1a\x2d\xe8\x00\x10\x0d\x80\x00\x1e\x0d\xb0\x00\x14\x0d\x20\x04\x13\x10\x09\x38\x04\x0c\xb2\x09\x08\x04\x0a\xb4\x09\x08\x04\x11\xf8\x09\x08\x04\x06\x5e\x09\x08\x00\x1b\x4d\x48\x04\x01\x40\x09\x10\x04\x09\x6a\x09\x08\x00\x00\x0d\x90\x04\x0a\xd2\x09\x10\x00\x0f\x0d\x08\x04\x05\x96\x09\x10\x00\x06\x2d\x88\x04\x08\x70\x09\x10\x04\x13\xec\x09\x08\x04\x12\x48\x09\x08\x00\x04\x6d\xb0\x04\x21\xca\x09\x10\x00\x1e\x4d\xe0\x04\x1c\xfc\x09\x10\x04\x17\x3e\x0d\x08\x0d\x40\x00\x0e\x0d\x30\x04\x0c\x44\x09\x18\x00\x1e\x6d\x20\x04\x00\x0a\x09\x10\x04\x24\xfe\x09\x08\x04\x26\xf2\x09\x08\x00\x1e\x4d\x80\x00\x09\x0d\x78\x04\x09\x4c\x09\x18\x00\x0d\x4d\xb8\x00\x12\x0d\x60\x00\x0c\x4d\x10\x00\x0a\x4d\xd8\x04\x11\xd0\x09\x28\x00\x26\x6d\xb8\x00\x0b\x2d\x40\x00\x0d\x0d\xb8\x00\x22\x6d\x10\x04\x12\xd4\x09\x28\x00\x15\x0d\xe8\x04\x23\x28\x09\x10\x00\x0f\x31\x60\x2d\xe0\x00\x05\x2d\x78\x00\x25\x2d\x48\x00\x25\x2d\x70\x00\x23\x6d\x30\x04\x00\x3c\x09\x38\x00\x21\x4d\xf0\x04\x03\x34\x09\x10\x00\x24\x2d\xc0\x00\x0f\x8d\x30\x00\x23\x0d\xf0\x00\x1e\x2d\x60\x00\x19\x0d\x38\x04\x11\xda\x09\x30\x04\x12\xb6\x09\x08\x00\x21\x2d\x00\x00\x06\x4d\x28\x00\x0c\x4d\xd0\x00\x26\x8d\x98\x00\x09\x8d\xe8\x04\x0b\x9a\x09\x30\x00\x25\x2d\xf0\x00\x02\x0d\xf8\x00\x06\x8d\x48\x00\x20\x0d\x10\x00\x1c\x2d\x28\x00\x0a\x8d\x58\x04\x05\x46\x09\x38\x04\x20\x3a\x09\x08\x00\x1a\x4d\x28\x04\x01\x72\x09\x10\x00\x12\x2d\xb0\x00\x0f\x0d\xa0\x04\x01\xf4\x09\x18\x04\x22\x56\x09\x08\x00\x18\xad\x40\x00\x1a\x4d\x50\x00\x1c\x0d\xb0\x04\x1a\x86\x09\x20\x00\x22\x2d\xb0\x00\x0f\x8d\x38\x00\x25\x2d\xf0\x00\x14\x0d\x50\x00\x02\x8d\x48\x00\x16\x2d\x50\x00\x02\x0d\x88\x00\x0d\x6d\x88\x00\x01\x0d\xf8\x00\x12\x8d\x88\x00\x00\x4d\x98\x04\x23\x5a\x0d\x60\xcd\x08\x00\x21\x71\x78\x0d\x80\x00\x18\x6d\x48\x00\x0e\x0d\x20\x00\x10\x2d\x08\x04\x14\x1e\x09\x38\x04\x11\x80\x09\x08\x00\x17\x4d\x18\x04\x0e\x24\x09\x10\x00\x18\x4d\x98\x04\x23\xfa\x09\x10\x00\x01\x0d\x38\x00\x24\xb1\x70\x00\x4a\x09\x18\x00\x18\x8d\xb8\x00\x26\x0d\x98\x00\x02\x8d\x80\x24\x01\x4a\x00\x00\x00\x04\x00\x00\x00\x80";

    var conn = try Self.init(allocator);
    conn.compression = .none;
    defer conn.deinit();

    try conn.feedReadable(data);
    try testing.expectError(error.EnvelopeBodyIsCompressed, conn.readMessagesNoEof(allocator));
}

test "split reads, multiple ticks" {
    var arena = heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const data = "\x84\x01\x00\x00\x08\x00\x00\x03\xe0\xbb\x0d\x1c\x00\x00\x00\x02\x00\x00\x00\x01\x05\x04\x98\x06\x66\x6f\x6f\x62\x61\x72\x00\x0a\x61\x67\x65\x5f\x74\x6f\x5f\x69\x64\x73\x00\x03\x61\x67\x65\x00\x09\x00\x00\x00\xd2\x00\x00\x00\x04\x00\x00\x25\xa8\x00\x05\x08\x04\x12\xde\x09\x10\x04\x10\x18\x09\x08\x04\x1d\x74\x09\x08\x04\x12\x2a\x09\x08\x04\x06\x2c\x09\x08\x04\x17\x0c\x09\x08\x04\x18\xf6\x09\x08\x04\x02\x94\x09\x08\x04\x1c\xac\x09\x08\x00\x07\x0d\x10\x04\x00\x6e\x09\x10\x04\x1d\x1a\x09\x08\x04\x0b\x22\x09\x08\x04\x20\x58\x09\x08\x04\x0f\xaa\x09\x08\x04\x17\xe8\x09\x08\x04\x01\x36\x09\x08\x04\x12\x16\x09\x08\x04\x1d\xe2\x09\x08\x04\x0a\x82\x09\x08\x00\x24\x0d\x80\x04\x1b\x08\x09\x10\x04\x1a\x68\x09\x08\x04\x10\xa4\x09\x08\x04\x19\xdc\x09\x08\x00\x0d\x0d\xb0\x04\x14\x64\x09\x10\x04\x00\x78\x09\x08\x00\x05\x0d\x70\x04\x15\x54\x09\x10\x04\x15\xea\x09\x08\x04\x14\xe6\x09\x08\x04\x20\x30\x09\x08\x00\x15\x0d\x68\x04\x1f\xb8\x0d\x10\x00\xae\x09\x08\x04\x16\x76\x09\x08\x04\x05\xbe\x09\x08\x00\x23\x0d\x08\x00\x04\x0d\xa8\x04\x13\xa6\x09\x18\x04\x13\x88\x09\x08\x04\x20\xa8\x09\x08\x04\x00\x8c\x09\x08\x04\x03\x5c\x09\x08\x04\x1c\x7a\x09\x08\x04\x14\x14\x09\x08\x04\x17\x84\x09\x08\x04\x05\x28\x09\x08\x04\x14\xa0\x09\x08\x04\x20\x9e\x09\x08\x04\x0c\x12\x09\x08\x00\x02\x0d\x08\x04\x03\xc0\x09\x10\x04\x09\xb0\x09\x08\x04\x08\x8e\x09\x08\x04\x26\x66\x09\x08\x00\x21\x2d\x88\x00\x1a\x2d\x50\x04\x13\x9c\x09\x18\x04\x0f\x32\x09\x08\x00\x1e\x2d\x10\x04\x1e\x46\x09\x10\x04\x1a\x04\x09\x08\x04\x20\x1c\x09\x08\x00\x16\x0d\x68\x00\x1d\x0d\xd0\x00\x1b\x2d\xb0\x04\x16\x8a\x09\x20\x04\x0e\x60\x09\x08\x04\x15\xfe\x09\x08\x00\x20\x2d\xf0\x04\x12\x52\x09\x10\x04\x0b\x90\x09\x08\x00\x09\x2d\x18\x04\x15\xd6\x09\x10\x04\x23\x50\x09\x08\x00\x03\x41\x9f\x0c\x04\x00\x00\x26\x2d\x08\x00\x19\x2d\x98\x00\x0b\x2d\xc8\x04\x01\x7c\x09\x28\x04\x11\x6c\x09\x08\x00\x01\x2d\xb0\x04\x22\x42\x09\x10\x04\x12\xa2\x0d\x08\x00\x98\x09\x08\x00\x03\x4d\x90\x00\x20\x2d\xa0\x00\x11\x0d\xc8\x00\x05\x0d\xf0\x00\x1f\x4d\x28\x04\x1b\x62\x09\x30\x04\x22\x38\x09\x08\x00\x01\x0d\x70\x00\x19\x2d\x10\x00\x1a\x2d\xe8\x00\x10\x0d\x80\x00\x1e\x0d\xb0\x00\x14\x0d\x20\x04\x13\x10\x09\x38\x04\x0c\xb2\x09\x08\x04\x0a\xb4\x09\x08\x04\x11\xf8\x09\x08\x04\x06\x5e\x09\x08\x00\x1b\x4d\x48\x04\x01\x40\x09\x10\x04\x09\x6a\x09\x08\x00\x00\x0d\x90\x00\x0a\x6d\x78\x00\x0f\x0d\x08\x04\x05\x96\x09\x20\x00\x06\x2d\x88\x04\x08\x70\x09\x10\x04\x13\xec\x09\x08\x04\x12\x48\x09\x08\x04\x04\xd8\x09\x08\x04\x21\xca\x09\x08\x00\x1e\x4d\xe0\x04\x1c\xfc\x09\x10\x04\x17\x3e\x0d\x08\x0d\x40\x00\x0e\x0d\x30\x04\x0c\x44\x09\x18\x00\x1e\x6d\x20\x04\x00\x0a\x09\x10\x04\x24\xfe\x09\x08\x04\x26\xf2\x09\x08\x00\x1e\x4d\x80\x00\x09\x0d\x78\x04\x09\x4c\x09\x18\x00\x0d\x4d\xb8\x00\x12\x0d\x60\x00\x0c\x4d\x10\x00\x0a\x4d\xd8\x04\x11\xd0\x09\x28\x00\x26\x6d\xb8\x00\x0b\x2d\x40\x00\x0d\x0d\xb8\x00\x22\x6d\x10\x04\x12\xd4\x09\x28\x00\x15\x0d\xe8\x04\x23\x28\x09\x10\x00\x0f\x31\x60\x2d\xe0\x00\x05\x2d\x78\x00\x25\x2d\x48\x00\x25\x2d\x70\x00\x23\x6d\x30\x04\x00\x3c\x09\x38\x00\x21\x4d\xf0\x04\x03\x34\x09\x10\x00\x24\x2d\xc0\x00\x0f\x8d\x30\x00\x23\x0d\xf0\x00\x1e\x2d\x60\x00\x19\x0d\x38\x04\x11\xda\x09\x30\x04\x12\xb6\x09\x08\x00\x21\x2d\x00\x00\x06\x4d\x28\x00\x0c\x4d\xd0\x00\x26\x8d\x98\x00\x09\x8d\xe8\x04\x0b\x9a\x09\x30\x00\x25\x2d\xf0\x00\x02\x0d\xf8\x00\x06\x8d\x48\x00\x20\x0d\x10\x00\x1c\x2d\x28\x00\x0a\x8d\x58\x04\x05\x46\x09\x38\x04\x20\x3a\x09\x08\x00\x1a\x4d\x28\x04\x01\x72\x09\x10\x00\x12\x2d\xb0\x00\x0f\x0d\xa0\x04\x01\xf4\x09\x18\x04\x22\x56\x09\x08\x00\x18\xad\x40\x00\x1a\x4d\x50\x00\x1c\x0d\xb0\x04\x1a\x86\x09\x20\x00\x22\x2d\xb0\x00\x0f\x8d\x38\x00\x25\x2d\xf0\x00\x14\x0d\x50\x00\x02\x8d\x48\x00\x16\x2d\x50\x00\x02\x0d\x88\x00\x0d\x6d\x88\x00\x01\x0d\xf8\x00\x12\x8d\x88\xd1\x18\x04\x23\x5a\x0d\x60\xcd\x08\x00\x21\x71\x78\x0d\x80\x00\x18\x6d\x48\x00\x0e\x0d\x20\x00\x10\x2d\x08\x04\x14\x1e\x09\x38\x04\x11\x80\x09\x08\x00\x17\x4d\x18\x04\x0e\x24\x09\x10\x00\x18\x4d\x98\x04\x23\xfa\x09\x10\x24\x01\xea\x00\x00\x00\x04\x00\x00\x24\xb8";

    var conn = try Self.init(allocator);
    conn.compression = .snappy;
    defer conn.deinit();

    // Pretend the handshake is already done
    conn.handshake_state = .ready;
    conn.state = .nominal;

    // Feed the data in chunks
    var i: usize = 0;
    while (i < data.len) : (i += 200) {
        const len = @min(data.len - i, 200);
        const part = data[i .. i + len];

        try conn.feedReadable(part);
        try conn.tick();
    }

    try testing.expectEqual(1, conn.tracer.events.readableLength());

    const message: ResultMessage = conn.tracer.events.readItem().?.message.result;
    const rows = message.result.rows;
    try testing.expectEqual(210, rows.data.len);
}
