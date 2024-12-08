const build_options = @import("build_options");
const std = @import("std");
const debug = std.debug;
const heap = std.heap;
const io = std.io;
const mem = std.mem;
const posix = std.posix;
const testing = std.testing;

const protocol = @import("protocol.zig");
const testutils = @import("testutils.zig");

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

const log = std.log.scoped(.connection);

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

    /// Contains the state that is negotiated with a node as part of the handshake.
    const NegotiatedState = struct {
        cql_version: CQLVersion,
    };

    options: InitOptions,

    // connection state
    negotiated_state: NegotiatedState,
    framing: struct {
        enabled: bool = false,
        format: Frame.Format = undefined,
    } = .{},

    pub fn init(options: InitOptions) Connection {
        return Connection{
            .options = options,
            .negotiated_state = undefined,
        };
    }

    pub fn deinit(_: *Self) void {}

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
    pub fn writeMessage(self: *Self, scratch_allocator: mem.Allocator, message: Message, out: *std.ArrayList(u8)) !void {
        const MessageType = @TypeOf(message);

        //
        // Prepare the envelope
        //

        var envelope = Envelope{
            .header = EnvelopeHeader{
                .version = self.options.protocol_version,
                .flags = 0,
                .stream = 0,
                .opcode = std.meta.activeTag(message),
                .body_len = 0,
            },
            .body = &[_]u8{},
        };

        if (self.options.protocol_version.is(5)) {
            envelope.header.flags |= EnvelopeFlags.UseBeta;
        }

        var mw = try MessageWriter.init(scratch_allocator);

        if (std.meta.hasMethod(MessageType, "write")) {
            // Encode body
            switch (@typeInfo(@TypeOf(MessageType.write))) {
                .@"fn" => |info| {
                    if (info.params.len == 3) {
                        try message.write(self.options.protocol_version, &mw);
                    } else {
                        try message.write(&mw);
                    }
                },
                else => unreachable,
            }

            // This is the actual bytes of the encoded body.
            const written = mw.getWritten();

            // Default to using the uncompressed body.
            envelope.header.body_len = @intCast(written.len);
            envelope.body = written;

            // Compress the body if we can use it.
            // Only relevant for Protocol <= v4, Protocol v5 doest compression using the framing format.
            if (self.options.protocol_version.isAtMost(4)) {
                if (self.options.compression) |compression| {
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
        }

        //
        // Write a frame if protocol v5
        // Write the envelope directly otherwise
        //

        var envelope_buffer = std.ArrayList(u8).init(scratch_allocator);
        const envelope_data = try protocol.writeEnvelope(envelope, &envelope_buffer);

        const final_payload = if (self.framing.enabled)
            try Frame.encode(scratch_allocator, envelope_data, true, .uncompressed)
        else
            envelope_data;

        try out.appendSlice(final_payload);
    }

    pub fn readMessages(self: *Self, scratch_allocator: mem.Allocator, allocator: mem.Allocator, data: []const u8, out: *std.ArrayList(Message)) !void {
        var data_fbs = std.io.fixedBufferStream(data);

        while (true) {
            if (self.framing.enabled) {
                const result = try Frame.read(scratch_allocator, data_fbs.reader(), self.framing.format);
                debug.assert(result.frame.is_self_contained);
                debug.assert(result.frame.payload.len > 0);

                var fbs = io.StreamSource{ .const_buffer = io.fixedBufferStream(result.frame.payload) };
                const envelope = try Envelope.read(scratch_allocator, fbs.reader());

                const message = try self.decodeMessage(allocator, envelope);

                try out.append(message);
            } else {
                var envelope = try Envelope.read(scratch_allocator, data_fbs.reader());

                if (envelope.header.flags & EnvelopeFlags.Compression == EnvelopeFlags.Compression) {
                    const compression = self.options.compression orelse return error.InvalidCompressedFrame;

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

                const message = try self.decodeMessage(allocator, envelope);

                try out.append(message);
            }
        }
    }

    fn decodeMessage(self: *Self, allocator: mem.Allocator, envelope: Envelope) !Message {
        var mr = MessageReader.init(envelope.body);

        const message = switch (envelope.header.opcode) {
            .@"error" => Message{ .@"error" = try ErrorMessage.read(allocator, &mr) },
            .startup => Message{ .startup = try StartupMessage.read(allocator, &mr) },
            .ready => Message{ .ready = ReadyMessage{} },
            .options => Message{ .options = {} },
            .supported => Message{ .supported = try SupportedMessage.read(allocator, &mr) },
            .result => Message{ .result = try ResultMessage.read(allocator, self.options.protocol_version, &mr) },
            .register => Message{ .register = {} },
            .event => Message{ .event = try EventMessage.read(allocator, &mr) },
            .authenticate => Message{ .authenticate = try AuthenticateMessage.read(allocator, &mr) },
            .auth_challenge => Message{ .auth_challenge = try AuthChallengeMessage.read(allocator, &mr) },
            .auth_success => Message{ .auth_success = try AuthSuccessMessage.read(allocator, &mr) },
            else => std.debug.panic("invalid read message {}\n", .{envelope.header.opcode}),
        };

        return message;
    }
};

test "connection: v5" {
    var connection = Connection.init(.{
        .protocol_version = try ProtocolVersion.init(5),
    });

    var arena = testutils.arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

    var write_buffer = std.ArrayList(u8).init(allocator);
    var received_messages = std.ArrayList(Message).init(allocator);

    // Handshake
    {

        // Write OPTIONS

        try connection.writeMessage(allocator, Message{ .options = {} }, &write_buffer);

        // Read SUPPORTED (2 messages here)

        {
            const data =
                "\x84\x00\x00\x09\x06\x00\x00\x00\x60\x00\x03\x00\x11\x50\x52\x4f\x54\x4f\x43\x4f\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x53\x00\x03\x00\x04\x33\x2f\x76\x33\x00\x04\x34\x2f\x76\x34\x00\x09\x35\x2f\x76\x35\x2d\x62\x65\x74\x61\x00\x0b\x43\x4f\x4d\x50\x52\x45\x53\x53\x49\x4f\x4e\x00\x02\x00\x06\x73\x6e\x61\x70\x70\x79\x00\x03\x6c\x7a\x34\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x01\x00\x05\x33\x2e\x34\x2e\x34" ++
                "\x84\x00\x00\x09\x06\x00\x00\x00\x60\x00\x03\x00\x11\x50\x52\x4f\x54\x4f\x43\x4f\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x53\x00\x03\x00\x04\x33\x2f\x76\x33\x00\x04\x34\x2f\x76\x34\x00\x09\x35\x2f\x76\x35\x2d\x62\x65\x74\x61\x00\x0b\x43\x4f\x4d\x50\x52\x45\x53\x53\x49\x4f\x4e\x00\x02\x00\x06\x73\x6e\x61\x70\x70\x79\x00\x03\x6c\x7a\x34\x00\x0b\x43\x51\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x01\x00\x05\x33\x2e\x34\x2e\x34";

            connection.readMessages(allocator, allocator, data, &received_messages) catch |err| switch (err) {
                error.UnexpectedEOF => {},
                else => return err,
            };
            try testing.expectEqual(2, received_messages.items.len);
        }

        // Write STARTUP

        try connection.writeMessage(allocator, Message{ .startup = .{} }, &write_buffer);

        // Read READY

        {
            const data = "\x84\x00\x00\x02\x02\x00\x00\x00\x00";

            received_messages.clearRetainingCapacity();
            connection.readMessages(allocator, allocator, data, &received_messages) catch |err| switch (err) {
                error.UnexpectedEOF => {},
                else => return err,
            };
            try testing.expectEqual(1, received_messages.items.len);
        }

        // Switch to framing
        if (connection.options.protocol_version.isAtLeast(5)) {
            connection.framing.enabled = true;
        }
    }

    // Query
}
