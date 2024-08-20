const build_options = @import("build_options");
const std = @import("std");
const heap = std.heap;
const io = std.io;
const log = std.log;
const mem = std.mem;
const net = std.net;
const debug = std.debug;

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
const PrepareMessage = protocol.PrepareMessage;
const QueryMessage = protocol.QueryMessage;
const ReadyMessage = protocol.ReadyMessage;
const ResultMessage = protocol.ResultMessage;
const StartupMessage = protocol.StartupMessage;
const SupportedMessage = protocol.SupportedMessage;

const lz4 = @import("lz4");
const snappy = @import("snappy");

pub const Message = union(Opcode) {
    @"error": ErrorMessage,
    startup: StartupMessage,
    ready: ReadyMessage,
    authenticate: AuthenticateMessage,
    options: void,
    supported: SupportedMessage,
    query: QueryMessage,
    result: ResultMessage,
    prepare: PrepareMessage,
    execute: ExecuteMessage,
    register: void,
    event: EventMessage,
    batch: BatchMessage,
    auth_challenge: AuthChallengeMessage,
    auth_response: AuthResponseMessage,
    auth_success: AuthSuccessMessage,
};

pub const Connection = struct {
    const Self = @This();

    pub const InitOptions = struct {
        /// the protocl version to use.
        protocol_version: ProtocolVersion = ProtocolVersion{ .version = @as(u8, 4) },

        /// The compression algorithm to use if possible.
        compression: ?CompressionAlgorithm = null,

        /// The username to use for authentication if required.
        username: ?[]const u8 = null,
        /// The password to use for authentication if required.
        password: ?[]const u8 = null,

        /// If this is provided, init will populate some information about failures.
        /// This will provide more detail than an error can.
        diags: ?*Diagnostics = null,

        pub const Diagnostics = struct {
            /// The error message returned by the Cassandra node.
            message: []const u8 = "",
        };
    };

    const BufferedReaderType = io.BufferedReader(4096, std.net.Stream.Reader);
    const BufferedWriterType = io.BufferedWriter(4096, std.net.Stream.Writer);

    // TODO(vincent): we probably don't need the reader and writer abstractions here.
    const EnvelopeWriterType = EnvelopeWriter(std.ArrayList(u8).Writer);

    /// Contains the state that is negotiated with a node as part of the handshake.
    const NegotiatedState = struct {
        cql_version: CQLVersion,
    };

    allocator: mem.Allocator,
    options: InitOptions,

    socket: std.net.Stream,

    buffered_reader: BufferedReaderType,
    buffered_writer: BufferedWriterType,

    /// Helpers types needed to encode and decode the CQL protocol.
    envelope_writer_buffer: std.ArrayList(u8),
    envelope_writer: EnvelopeWriterType,
    message_reader: MessageReader,
    message_writer: MessageWriter,

    // connection state
    negotiated_state: NegotiatedState,
    framing: struct {
        enabled: bool = false,
        format: Frame.Format = undefined,
    },

    pub fn initIp4(self: *Self, allocator: mem.Allocator, seed_address: net.Address, options: InitOptions) !void {
        self.allocator = allocator;
        self.options = options;

        self.socket = try net.tcpConnectToAddress(seed_address);
        errdefer self.socket.close();

        self.buffered_reader = BufferedReaderType{ .unbuffered_reader = self.socket.reader() };
        self.buffered_writer = BufferedWriterType{ .unbuffered_writer = self.socket.writer() };

        self.envelope_writer_buffer = std.ArrayList(u8).init(allocator);
        self.envelope_writer = EnvelopeWriterType.init(self.envelope_writer_buffer.writer());

        MessageReader.reset(&self.message_reader, "");
        // TODO(vincent): don't use the root allocator here, limit how much memory we can use
        self.message_writer = try MessageWriter.init(allocator);

        var dummy_diags = InitOptions.Diagnostics{};
        const diags = options.diags orelse &dummy_diags;

        try self.handshake(diags);
    }

    pub fn deinit(self: *Self) void {
        self.socket.close();
        self.envelope_writer_buffer.deinit();
        self.message_writer.deinit();
    }

    fn handshake(self: *Self, diags: *InitOptions.Diagnostics) !void {
        // Sequence diagram to establish the connection:
        //
        // +---------+                 +---------+
        // | Client  |                 | Server  |
        // +---------+                 +---------+
        //      |                           |
        //      | OPTIONS                   |
        //      |-------------------------->|
        //      |                           |
        //      |                 SUPPORTED |
        //      |<--------------------------|
        //      |                           |
        //      | STARTUP                   |
        //      |-------------------------->|
        //      |                           |
        //      |      (READY|AUTHENTICATE) |
        //      |<--------------------------|
        //      |                           |
        //      | AUTH_RESPONSE             |
        //      |-------------------------->|
        //      |                           |
        var buffer: [4096]u8 = undefined;
        var fba = heap.FixedBufferAllocator.init(&buffer);

        // Write OPTIONS, expect SUPPORTED

        try self.writeMessage(
            fba.allocator(),
            .options,
            protocol.OptionsMessage{},
            .{},
        );

        fba.reset();
        switch (try self.nextMessage(fba.allocator(), .{})) {
            .supported => |fr| self.negotiated_state.cql_version = fr.cql_versions[0],
            .@"error" => |err| {
                diags.message = err.message;
                return error.HandshakeFailed;
            },
            else => return error.InvalidServerResponse,
        }

        // Write STARTUP, expect either READY or AUTHENTICATE

        fba.reset();
        try self.writeMessage(
            fba.allocator(),
            .startup,
            protocol.StartupMessage{
                .cql_version = self.negotiated_state.cql_version,
                .compression = self.options.compression,
            },
            .{},
        );
        switch (try self.nextMessage(fba.allocator(), .{})) {
            .ready => return,
            .authenticate => |fr| {
                try self.authenticate(fba.allocator(), diags, fr.authenticator);
            },
            .@"error" => |err| {
                diags.message = err.message;
                return error.HandshakeFailed;
            },
            else => return error.InvalidServerResponse,
        }

        // Enable framing if protocol v5
        if (self.options.protocol_version.isAtLeast(5)) {
            self.framing.enabled = true;
            self.framing.format = .uncompressed; // TODO(vincent): use compression
        }
    }

    fn authenticate(self: *Self, allocator: mem.Allocator, diags: *InitOptions.Diagnostics, _: []const u8) !void {
        // TODO(vincent): handle authenticator classes
        // TODO(vincent): handle auth challenges
        if (self.options.username == null) {
            return error.NoUsername;
        }
        if (self.options.password == null) {
            return error.NoPassword;
        }

        // Write AUTH_RESPONSE
        {
            var buf: [512]u8 = undefined;
            const token = try std.fmt.bufPrint(&buf, "\x00{s}\x00{s}", .{ self.options.username.?, self.options.password.? });

            try self.writeMessage(
                allocator,
                .auth_response,
                protocol.AuthResponseMessage{
                    .token = token,
                },
                .{},
            );
        }

        // Read either AUTH_CHALLENGE, AUTH_SUCCESS or ERROR
        switch (try self.nextMessage(allocator, .{})) {
            .auth_challenge => unreachable,
            .auth_success => return,
            .@"error" => |err| {
                diags.message = err.message;
                return error.AuthenticationFailed;
            },
            else => return error.InvalidServerResponse,
        }
    }

    const WriteMessageOptions = struct {};

    /// writeMessage writes a single message to the TCP connection.
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
    pub fn writeMessage(self: *Self, allocator: mem.Allocator, opcode: Opcode, message: anytype, _: WriteMessageOptions) !void {
        const MessageType = @TypeOf(message);

        self.message_writer.reset();

        //
        // Prepare the envelope
        //

        var envelope = Envelope{
            .header = EnvelopeHeader{
                .version = self.options.protocol_version,
                .flags = 0,
                .stream = 0,
                .opcode = opcode,
                .body_len = 0,
            },
            .body = &[_]u8{},
        };

        if (self.options.protocol_version.is(5)) {
            envelope.header.flags |= EnvelopeFlags.UseBeta;
        }

        if (std.meta.hasMethod(MessageType, "write")) {
            // Encode body
            switch (@typeInfo(@TypeOf(MessageType.write))) {
                .Fn => |info| {
                    if (info.params.len == 3) {
                        try message.write(self.options.protocol_version, &self.message_writer);
                    } else {
                        try message.write(&self.message_writer);
                    }
                },
                else => unreachable,
            }

            // This is the actual bytes of the encoded body.
            const written = self.message_writer.getWritten();

            // Default to using the uncompressed body.
            envelope.header.body_len = @intCast(written.len);
            envelope.body = written;

            // Compress the body if we can use it.
            // Only relevant for Protocol <= v4, Protocol v5 doest compression using the framing format.
            if (self.options.protocol_version.isAtMost(4)) {
                if (self.options.compression) |compression| {
                    switch (compression) {
                        .LZ4 => {
                            const compressed_data = try lz4.compress(allocator, written);

                            envelope.header.flags |= EnvelopeFlags.Compression;
                            envelope.header.body_len = @intCast(compressed_data.len);
                            envelope.body = compressed_data;
                        },
                        .Snappy => {
                            const compressed_data = try snappy.compress(allocator, written);

                            envelope.header.flags |= EnvelopeFlags.Compression;
                            envelope.header.body_len = @intCast(compressed_data.len);
                            envelope.body = compressed_data;
                        },
                    }
                }
            }
        }
        try self.envelope_writer.write(envelope);

        //
        // Write a frame if protocol v5
        // Write the envelope directly otherwise
        //

        if (self.options.protocol_version.isAtLeast(5)) {
            // TODO(vincent): implement framing
            debug.panic("frame writer not implemented", .{});
        } else {
            const data = try self.envelope_writer_buffer.toOwnedSlice();
            defer self.allocator.free(data);

            _ = try self.buffered_writer.write(data);
        }

        try self.buffered_writer.flush();
    }

    pub const ReadMessageOptions = struct {
        /// If set the message will be allocated using this allocator instead of the allocator
        /// passed in `nextMessage`.
        ///
        /// This is useful if you want the message to have a different lifecycle, for example
        /// if you need to store the message in a list or a map.
        message_allocator: ?mem.Allocator = null,
    };

    pub fn nextMessage(self: *Self, allocator: mem.Allocator, options: ReadMessageOptions) !Message {
        var messages = try self.readMessages(allocator, options);
        defer messages.deinit();

        debug.assert(messages.items.len == 1);

        return messages.pop();
    }

    fn readMessages(self: *Self, allocator: mem.Allocator, options: ReadMessageOptions) !std.ArrayList(Message) {
        var result = std.ArrayList(Message).init(allocator);

        if (self.options.protocol_version.isAtMost(4)) {
            try self.readMessageV4(allocator, options, &result);
        } else if (self.options.protocol_version.isAtLeast(5)) {
            try self.readMessageV4(allocator, options, &result);
        }

        return result;
    }

    fn readMessagesV5(self: *Self, allocator: mem.Allocator, options: ReadMessageOptions, messages: *std.ArrayList(Message)) !void {
        const buffer = blk: {
            var buf: [256 * 1024]u8 = undefined;
            const n = try self.buffered_reader.read(&buf);
            if (n <= 0) {
                return error.UnexpectedEOF;
            }
            break :blk buf[0..n];
        };

        const result = try Frame.decode(allocator, buffer, self.framing.format);
        debug.assert(result.consumed == buffer.len);
        debug.assert(result.frame.is_self_contained);
        debug.assert(result.frame.payload.len > 0);

        var fbs = io.StreamSource{ .const_buffer = io.fixedBufferStream(result.frame.payload) };
        const envelope = try Envelope.read(allocator, fbs.reader());

        const message = try self.decodeMessage(options.message_allocator orelse allocator, envelope);

        try messages.append(message);
    }

    fn readMessageV4(self: *Self, allocator: mem.Allocator, options: ReadMessageOptions, messages: *std.ArrayList(Message)) !void {
        const envelope = try self.readEnvelope(allocator);
        defer envelope.deinit(allocator);

        self.message_reader.reset(envelope.body);

        const message = try self.decodeMessage(options.message_allocator orelse allocator, envelope);

        try messages.append(message);
    }

    fn readEnvelope(self: *Self, allocator: mem.Allocator) !Envelope {
        var envelope = try Envelope.read(allocator, self.buffered_reader.reader());

        if (envelope.header.flags & EnvelopeFlags.Compression == EnvelopeFlags.Compression) {
            const compression = self.options.compression orelse return error.InvalidCompressedFrame;

            switch (compression) {
                .LZ4 => {
                    // TODO(vincent): this is ugly
                    const decompressed_data = try lz4.decompress(allocator, envelope.body, envelope.body.len * 3);
                    envelope.body = decompressed_data;
                },
                .Snappy => {
                    const decompressed_data = try snappy.decompress(allocator, envelope.body);
                    envelope.body = decompressed_data;
                },
            }
        }

        return envelope;
    }

    fn decodeMessage(self: *Self, message_allocator: mem.Allocator, envelope: Envelope) !Message {
        const message = switch (envelope.header.opcode) {
            .@"error" => Message{ .@"error" = try ErrorMessage.read(message_allocator, &self.message_reader) },
            .startup => Message{ .startup = try StartupMessage.read(message_allocator, &self.message_reader) },
            .ready => Message{ .ready = ReadyMessage{} },
            .options => Message{ .options = {} },
            .supported => Message{ .supported = try SupportedMessage.read(message_allocator, &self.message_reader) },
            .result => Message{ .result = try ResultMessage.read(message_allocator, self.options.protocol_version, &self.message_reader) },
            .register => Message{ .register = {} },
            .event => Message{ .event = try EventMessage.read(message_allocator, &self.message_reader) },
            .authenticate => Message{ .authenticate = try AuthenticateMessage.read(message_allocator, &self.message_reader) },
            .auth_challenge => Message{ .auth_challenge = try AuthChallengeMessage.read(message_allocator, &self.message_reader) },
            .auth_success => Message{ .auth_success = try AuthSuccessMessage.read(message_allocator, &self.message_reader) },
            else => std.debug.panic("invalid read message {}\n", .{envelope.header.opcode}),
        };

        return message;
    }
};
