const build_options = @import("build_options");
const std = @import("std");
const heap = std.heap;
const io = std.io;
const log = std.log;
const mem = std.mem;
const net = std.net;

const FrameHeader = @import("frame.zig").FrameHeader;
const FrameFlags = @import("frame.zig").FrameFlags;
const RawFrame = @import("frame.zig").RawFrame;
const RawFrameReader = @import("frame.zig").RawFrameReader;
const RawFrameWriter = @import("frame.zig").RawFrameWriter;

const lz4 = @import("lz4.zig");

const enable_snappy = build_options.with_snappy;
const snappy = if (enable_snappy) @import("snappy.zig");

const message = @import("message.zig");
const Opcode = message.Opcode;
const ProtocolVersion = message.ProtocolVersion;
const CompressionAlgorithm = message.CompressionAlgorithm;
const CQLVersion = message.CQLVersion;
const PrimitiveReader = message.PrimitiveReader;
const PrimitiveWriter = message.PrimitiveWriter;

const frame = @import("frame.zig");
const ErrorFrame = frame.ErrorFrame;
const StartupFrame = frame.StartupFrame;
const ReadyFrame = frame.ReadyFrame;
const AuthenticateFrame = frame.AuthenticateFrame;
const SupportedFrame = frame.SupportedFrame;
const QueryFrame = frame.QueryFrame;
const ResultFrame = frame.ResultFrame;
const PrepareFrame = frame.PrepareFrame;
const ExecuteFrame = frame.ExecuteFrame;
const EventFrame = frame.EventFrame;
const BatchFrame = frame.BatchFrame;
const AuthChallengeFrame = frame.AuthChallengeFrame;
const AuthResponseFrame = frame.AuthResponseFrame;
const AuthSuccessFrame = frame.AuthSuccessFrame;

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

    const RawFrameReaderType = RawFrameReader(BufferedReaderType.Reader);
    const RawFrameWriterType = RawFrameWriter(BufferedWriterType.Writer);

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
    raw_frame_reader: RawFrameReaderType,
    raw_frame_writer: RawFrameWriterType,
    primitive_reader: PrimitiveReader,
    primitive_writer: PrimitiveWriter,

    // Negotiated with the server
    negotiated_state: NegotiatedState,

    pub fn initIp4(self: *Self, allocator: mem.Allocator, seed_address: net.Address, options: InitOptions) !void {
        self.allocator = allocator;
        self.options = options;

        self.socket = try net.tcpConnectToAddress(seed_address);
        errdefer self.socket.close();

        self.buffered_reader = BufferedReaderType{ .unbuffered_reader = self.socket.reader() };
        self.buffered_writer = BufferedWriterType{ .unbuffered_writer = self.socket.writer() };

        self.raw_frame_reader = RawFrameReaderType.init(self.buffered_reader.reader());
        self.raw_frame_writer = RawFrameWriterType.init(self.buffered_writer.writer());
        PrimitiveReader.reset(&self.primitive_reader, "");

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
    ///   fn write(protocol_version: ProtocolVersion, primitive_writer: *PrimitiveWriter) !void
    ///   fn write(primitive_writer: *PrimitiveWriter) !void
    ///
    /// Some frames don't care about the protocol version so this is why the second signature is supported.
    ///
    /// Additionally this method takes care of compression if enabled.
    ///
    /// This method is not thread safe.
    pub fn writeFrame(self: *Self, allocator: mem.Allocator, opcode: Opcode, comptime FrameType: type, fr: FrameType, options: WriteFrameOptions) !void {
        // Reset primitive writer
        // TODO(vincent): for async we probably should do something else for the primitive writer.
        try self.primitive_writer.reset(allocator);
        defer self.primitive_writer.deinit();

        // Prepare the raw frame
        var raw_frame = RawFrame{
            .header = FrameHeader{
                .version = options.protocol_version,
                .flags = 0,
                .stream = 0,
                .opcode = opcode,
                .body_len = 0,
            },
            .body = &[_]u8{},
        };

        if (options.protocol_version.is(5)) {
            raw_frame.header.flags |= FrameFlags.UseBeta;
        }

        if (std.meta.hasMethod(FrameType, "write")) {
            // Encode body
            switch (@typeInfo(@TypeOf(FrameType.write))) {
                .Fn => |info| {
                    if (info.params.len == 3) {
                        try fr.write(options.protocol_version, &self.primitive_writer);
                    } else {
                        try fr.write(&self.primitive_writer);
                    }
                },
                else => unreachable,
            }

            // This is the actual bytes of the encoded body.
            const written = self.primitive_writer.getWritten();

            // Default to using the uncompressed body.
            raw_frame.header.body_len = @intCast(written.len);
            raw_frame.body = written;

            // Compress the body if we can use it.
            if (options.compression) |compression| {
                switch (compression) {
                    .LZ4 => {
                        const compressed_data = try lz4.compress(allocator, written);

                        raw_frame.header.flags |= FrameFlags.Compression;
                        raw_frame.header.body_len = @intCast(compressed_data.len);
                        raw_frame.body = compressed_data;
                    },
                    .Snappy => {
                        if (comptime !enable_snappy) return error.InvalidCompressedFrame;

                        const compressed_data = try snappy.compress(allocator, written);

                        raw_frame.header.flags |= FrameFlags.Compression;
                        raw_frame.header.body_len = @intCast(compressed_data.len);
                        raw_frame.body = compressed_data;
                    },
                }
            }
        }

        try self.raw_frame_writer.write(raw_frame);
        try self.buffered_writer.flush();
    }

    pub const ReadFrameOptions = struct {
        frame_allocator: mem.Allocator,
    };

    pub fn readFrame(self: *Self, allocator: mem.Allocator, options: ?ReadFrameOptions) !Frame {
        const raw_frame = try self.readRawFrame(allocator);
        defer raw_frame.deinit(allocator);

        self.primitive_reader.reset(raw_frame.body);

        const frame_allocator = if (options) |opts| opts.frame_allocator else allocator;

        return switch (raw_frame.header.opcode) {
            .Error => Frame{ .Error = try ErrorFrame.read(frame_allocator, &self.primitive_reader) },
            .Startup => Frame{ .Startup = try StartupFrame.read(frame_allocator, &self.primitive_reader) },
            .Ready => Frame{ .Ready = ReadyFrame{} },
            .Options => Frame{ .Options = {} },
            .Supported => Frame{ .Supported = try SupportedFrame.read(frame_allocator, &self.primitive_reader) },
            .Result => Frame{ .Result = try ResultFrame.read(frame_allocator, self.options.protocol_version, &self.primitive_reader) },
            .Register => Frame{ .Register = {} },
            .Event => Frame{ .Event = try EventFrame.read(frame_allocator, &self.primitive_reader) },
            .Authenticate => Frame{ .Authenticate = try AuthenticateFrame.read(frame_allocator, &self.primitive_reader) },
            .AuthChallenge => Frame{ .AuthChallenge = try AuthChallengeFrame.read(frame_allocator, &self.primitive_reader) },
            .AuthSuccess => Frame{ .AuthSuccess = try AuthSuccessFrame.read(frame_allocator, &self.primitive_reader) },
            else => std.debug.panic("invalid read frame {}\n", .{raw_frame.header.opcode}),
        };
    }

    fn readRawFrame(self: *Self, allocator: mem.Allocator) !RawFrame {
        var raw_frame = try self.raw_frame_reader.read(allocator);

        if (raw_frame.header.flags & FrameFlags.Compression == FrameFlags.Compression) {
            const compression = self.options.compression orelse return error.InvalidCompressedFrame;

            switch (compression) {
                .LZ4 => {
                    const decompressed_data = try lz4.decompress(allocator, raw_frame.body);
                    raw_frame.body = decompressed_data;
                },
                .Snappy => {
                    if (comptime !enable_snappy) return error.InvalidCompressedFrame;

                    const decompressed_data = try snappy.decompress(allocator, raw_frame.body);
                    raw_frame.body = decompressed_data;
                },
            }
        }

        return raw_frame;
    }
};
