const build_options = @import("build_options");
const std = @import("std");
const heap = std.heap;
const io = std.io;
const log = std.log;
const mem = std.mem;
const net = std.net;

const frame = @import("frame.zig");

const EnvelopeFlags = frame.EnvelopeFlags;
const EnvelopeHeader = frame.EnvelopeHeader;
const Envelope = frame.Envelope;
const EnvelopeReader = frame.EnvelopeReader;
const EnvelopeWriter = frame.EnvelopeWriter;

const AuthChallengeFrame = frame.AuthChallengeFrame;
const AuthResponseFrame = frame.AuthResponseFrame;
const AuthSuccessFrame = frame.AuthSuccessFrame;
const AuthenticateFrame = frame.AuthenticateFrame;
const BatchFrame = frame.BatchFrame;
const ErrorFrame = frame.ErrorFrame;
const EventFrame = frame.EventFrame;
const ExecuteFrame = frame.ExecuteFrame;
const PrepareFrame = frame.PrepareFrame;
const QueryFrame = frame.QueryFrame;
const ReadyFrame = frame.ReadyFrame;
const ResultFrame = frame.ResultFrame;
const StartupFrame = frame.StartupFrame;
const SupportedFrame = frame.SupportedFrame;

const message = @import("message.zig");
const CQLVersion = message.CQLVersion;
const CompressionAlgorithm = message.CompressionAlgorithm;
const MessageReader = message.MessageReader;
const MessageWriter = message.MessageWriter;
const Opcode = message.Opcode;
const ProtocolVersion = message.ProtocolVersion;

const lz4 = @import("lz4.zig");
const enable_snappy = build_options.with_snappy;
const snappy = if (enable_snappy) @import("snappy.zig");

pub const Frame = union(Opcode) {
    Error: ErrorFrame,
    Startup: StartupFrame,
    Ready: ReadyFrame,
    Authenticate: AuthenticateFrame,
    Options: void,
    Supported: SupportedFrame,
    Query: QueryFrame,
    Result: ResultFrame,
    Prepare: PrepareFrame,
    Execute: ExecuteFrame,
    Register: void,
    Event: EventFrame,
    Batch: BatchFrame,
    AuthChallenge: AuthChallengeFrame,
    AuthResponse: AuthResponseFrame,
    AuthSuccess: AuthSuccessFrame,
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

    const EnvelopeReaderType = EnvelopeReader(BufferedReaderType.Reader);
    const EnvelopeWriterType = EnvelopeWriter(BufferedWriterType.Writer);

    /// Contains the state that is negotiated with a node as part of the handshake.
    const NegotiatedState = struct {
        cql_version: CQLVersion,
    };

    allocator: mem.Allocator,
    options: InitOptions,

    socket: std.net.Stream,

    buffered_reader: BufferedReaderType,
    buffered_writer: BufferedWriterType,

    /// Helpers types needed to decode the CQL protocol.
    envelope_reader: EnvelopeReaderType,
    envelope_writer: EnvelopeWriterType,
    message_reader: MessageReader,
    message_writer: MessageWriter,

    // Negotiated with the server
    negotiated_state: NegotiatedState,

    pub fn initIp4(self: *Self, allocator: mem.Allocator, seed_address: net.Address, options: InitOptions) !void {
        self.allocator = allocator;
        self.options = options;

        self.socket = try net.tcpConnectToAddress(seed_address);
        errdefer self.socket.close();

        self.buffered_reader = BufferedReaderType{ .unbuffered_reader = self.socket.reader() };
        self.buffered_writer = BufferedWriterType{ .unbuffered_writer = self.socket.writer() };

        self.envelope_reader = EnvelopeReaderType.init(self.buffered_reader.reader());
        self.envelope_writer = EnvelopeWriterType.init(self.buffered_writer.writer());
        MessageReader.reset(&self.message_reader, "");

        var dummy_diags = InitOptions.Diagnostics{};
        const diags = options.diags orelse &dummy_diags;

        try self.handshake(diags);
    }

    pub fn close(self: *Self) void {
        self.socket.close();
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
        var fba = std.heap.FixedBufferAllocator.init(&buffer);

        // Write OPTIONS, expect SUPPORTED

        try self.writeFrame(
            fba.allocator(),
            .Options,
            frame.OptionsFrame,
            .{},
            .{
                .protocol_version = self.options.protocol_version,
                .compression = null,
            },
        );

        fba.reset();
        switch (try self.readFrame(fba.allocator(), null)) {
            .Supported => |fr| self.negotiated_state.cql_version = fr.cql_versions[0],
            .Error => |err| {
                diags.message = err.message;
                return error.HandshakeFailed;
            },
            else => return error.InvalidServerResponse,
        }

        // Write STARTUP, expect either READY or AUTHENTICATE

        fba.reset();
        try self.writeFrame(
            fba.allocator(),
            .Startup,
            frame.StartupFrame,
            frame.StartupFrame{
                .cql_version = self.negotiated_state.cql_version,
                .compression = self.options.compression,
            },
            .{
                .protocol_version = self.options.protocol_version,
                .compression = self.options.compression,
            },
        );
        switch (try self.readFrame(fba.allocator(), null)) {
            .Ready => return,
            .Authenticate => |fr| {
                try self.authenticate(fba.allocator(), diags, fr.authenticator);
            },
            .Error => |err| {
                diags.message = err.message;
                return error.HandshakeFailed;
            },
            else => return error.InvalidServerResponse,
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

            try self.writeFrame(
                allocator,
                .AuthResponse,
                frame.AuthResponseFrame,
                frame.AuthResponseFrame{
                    .token = token,
                },
                .{
                    .protocol_version = self.options.protocol_version,
                    .compression = self.options.compression,
                },
            );
        }

        // Read either AUTH_CHALLENGE, AUTH_SUCCESS or ERROR
        switch (try self.readFrame(allocator, null)) {
            .AuthChallenge => unreachable,
            .AuthSuccess => return,
            .Error => |err| {
                diags.message = err.message;
                return error.AuthenticationFailed;
            },
            else => return error.InvalidServerResponse,
        }
    }

    const WriteFrameOptions = struct {
        protocol_version: ProtocolVersion,
        compression: ?CompressionAlgorithm,
    };

    /// writeFrame writes a single frame to the TCP connection.
    ///
    /// A frame can be:
    /// * an anonymous struct with just a .opcode field (therefore no frame body).
    /// * an anonymous struct with a .opcode field and a .body field.
    ///
    /// If the .body field is present, it must me a type implementing either of the following write function:
    ///
    ///   fn write(protocol_version: ProtocolVersion, w: *MessageWriter) !void
    ///   fn write(w: *MessageWriter) !void
    ///
    /// Some frames don't care about the protocol version so this is why the second signature is supported.
    ///
    /// Additionally this method takes care of compression if enabled.
    ///
    /// This method is not thread safe.
    pub fn writeFrame(self: *Self, allocator: mem.Allocator, opcode: Opcode, comptime FrameType: type, fr: FrameType, options: WriteFrameOptions) !void {
        self.message_writer.reset();

        // Prepare the raw frame
        var envelope = Envelope{
            .header = EnvelopeHeader{
                .version = options.protocol_version,
                .flags = 0,
                .stream = 0,
                .opcode = opcode,
                .body_len = 0,
            },
            .body = &[_]u8{},
        };

        if (options.protocol_version.is(5)) {
            envelope.header.flags |= EnvelopeFlags.UseBeta;
        }

        if (std.meta.hasMethod(FrameType, "write")) {
            // Encode body
            switch (@typeInfo(@TypeOf(FrameType.write))) {
                .Fn => |info| {
                    if (info.params.len == 3) {
                        try fr.write(options.protocol_version, &self.message_writer);
                    } else {
                        try fr.write(&self.message_writer);
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
            if (options.compression) |compression| {
                switch (compression) {
                    .LZ4 => {
                        const compressed_data = try lz4.compress(allocator, written);

                        envelope.header.flags |= EnvelopeFlags.Compression;
                        envelope.header.body_len = @intCast(compressed_data.len);
                        envelope.body = compressed_data;
                    },
                    .Snappy => {
                        if (comptime !enable_snappy) return error.InvalidCompressedFrame;

                        const compressed_data = try snappy.compress(allocator, written);

                        envelope.header.flags |= EnvelopeFlags.Compression;
                        envelope.header.body_len = @intCast(compressed_data.len);
                        envelope.body = compressed_data;
                    },
                }
            }
        }

        try self.envelope_writer.write(envelope);
        try self.buffered_writer.flush();
    }

    pub const ReadFrameOptions = struct {
        frame_allocator: mem.Allocator,
    };

    pub fn readFrame(self: *Self, allocator: mem.Allocator, options: ?ReadFrameOptions) !Frame {
        const envelope = try self.readEnvelope(allocator);
        defer envelope.deinit(allocator);

        self.message_reader.reset(envelope.body);

        const frame_allocator = if (options) |opts| opts.frame_allocator else allocator;

        return switch (envelope.header.opcode) {
            .Error => Frame{ .Error = try ErrorFrame.read(frame_allocator, &self.message_reader) },
            .Startup => Frame{ .Startup = try StartupFrame.read(frame_allocator, &self.message_reader) },
            .Ready => Frame{ .Ready = ReadyFrame{} },
            .Options => Frame{ .Options = {} },
            .Supported => Frame{ .Supported = try SupportedFrame.read(frame_allocator, &self.message_reader) },
            .Result => Frame{ .Result = try ResultFrame.read(frame_allocator, self.options.protocol_version, &self.message_reader) },
            .Register => Frame{ .Register = {} },
            .Event => Frame{ .Event = try EventFrame.read(frame_allocator, &self.message_reader) },
            .Authenticate => Frame{ .Authenticate = try AuthenticateFrame.read(frame_allocator, &self.message_reader) },
            .AuthChallenge => Frame{ .AuthChallenge = try AuthChallengeFrame.read(frame_allocator, &self.message_reader) },
            .AuthSuccess => Frame{ .AuthSuccess = try AuthSuccessFrame.read(frame_allocator, &self.message_reader) },
            else => std.debug.panic("invalid read frame {}\n", .{envelope.header.opcode}),
        };
    }

    fn readEnvelope(self: *Self, allocator: mem.Allocator) !Envelope {
        var envelope = try self.envelope_reader.read(allocator);

        if (envelope.header.flags & EnvelopeFlags.Compression == EnvelopeFlags.Compression) {
            const compression = self.options.compression orelse return error.InvalidCompressedFrame;

            switch (compression) {
                .LZ4 => {
                    const decompressed_data = try lz4.decompress(allocator, envelope.body);
                    envelope.body = decompressed_data;
                },
                .Snappy => {
                    if (comptime !enable_snappy) return error.InvalidCompressedFrame;

                    const decompressed_data = try snappy.decompress(allocator, envelope.body);
                    envelope.body = decompressed_data;
                },
            }
        }

        return envelope;
    }
};
