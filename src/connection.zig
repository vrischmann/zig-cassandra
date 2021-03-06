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

const PrimitiveReader = @import("primitive/reader.zig").PrimitiveReader;
const PrimitiveWriter = @import("primitive/writer.zig").PrimitiveWriter;

usingnamespace @import("primitive_types.zig");

const lz4 = @import("lz4.zig");
const snappy = comptime if (build_options.with_snappy) @import("snappy.zig");

// Ordered by Opcode
usingnamespace @import("frames/error.zig");
usingnamespace @import("frames/startup.zig");
usingnamespace @import("frames/ready.zig");
usingnamespace @import("frames/auth.zig");
usingnamespace @import("frames/options.zig");
usingnamespace @import("frames/supported.zig");
usingnamespace @import("frames/query.zig");
usingnamespace @import("frames/result.zig");
usingnamespace @import("frames/prepare.zig");
usingnamespace @import("frames/execute.zig");
usingnamespace @import("frames/register.zig");
usingnamespace @import("frames/event.zig");
usingnamespace @import("frames/batch.zig");

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

    allocator: *mem.Allocator,
    options: InitOptions,

    socket: std.net.Stream,

    buffered_reader: BufferedReaderType,
    buffered_writer: BufferedWriterType,

    read_lock: if (std.io.is_async) std.event.Lock else void,
    write_lock: if (std.io.is_async) std.event.Lock else void,

    /// Helpers types needed to decode the CQL protocol.
    raw_frame_reader: RawFrameReaderType,
    raw_frame_writer: RawFrameWriterType,
    primitive_reader: PrimitiveReader,
    primitive_writer: PrimitiveWriter,

    // Negotiated with the server
    negotiated_state: NegotiatedState,

    pub fn initIp4(self: *Self, allocator: *mem.Allocator, seed_address: net.Address, options: InitOptions) !void {
        self.allocator = allocator;
        self.options = options;

        self.socket = try net.tcpConnectToAddress(seed_address);
        errdefer self.socket.close();

        self.buffered_reader = BufferedReaderType{ .unbuffered_reader = self.socket.reader() };
        self.buffered_writer = BufferedWriterType{ .unbuffered_writer = self.socket.writer() };

        if (std.io.is_async) {
            self.read_lock = std.event.Lock{};
            self.write_lock = std.event.Lock{};
        }

        self.raw_frame_reader = RawFrameReaderType.init(self.buffered_reader.reader());
        self.raw_frame_writer = RawFrameWriterType.init(self.buffered_writer.writer());
        self.primitive_reader = PrimitiveReader.init();

        var dummy_diags = InitOptions.Diagnostics{};
        var diags = options.diags orelse &dummy_diags;

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

        try self.writeFrame(&fba.allocator, .{ .opcode = .Options });
        fba.reset();
        switch (try self.readFrame(&fba.allocator, null)) {
            .Supported => |frame| self.negotiated_state.cql_version = frame.cql_versions[0],
            .Error => |err| {
                diags.message = err.message;
                return error.HandshakeFailed;
            },
            else => return error.InvalidServerResponse,
        }

        fba.reset();

        // Write STARTUP, expect either READY or AUTHENTICATE

        const startup_frame = StartupFrame{
            .cql_version = self.negotiated_state.cql_version,
            .compression = self.options.compression,
        };
        try self.writeFrame(&fba.allocator, .{
            .opcode = .Startup,
            .no_compression = true,
            .body = startup_frame,
        });
        switch (try self.readFrame(&fba.allocator, null)) {
            .Ready => return,
            .Authenticate => |frame| {
                try self.authenticate(&fba.allocator, diags, frame.authenticator);
            },
            .Error => |err| {
                diags.message = err.message;
                return error.HandshakeFailed;
            },
            else => return error.InvalidServerResponse,
        }
    }

    fn authenticate(self: *Self, allocator: *mem.Allocator, diags: *InitOptions.Diagnostics, authenticator: []const u8) !void {
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
            var frame = AuthResponseFrame{ .token = token };

            try self.writeFrame(allocator, .{
                .opcode = .AuthResponse,
                .body = frame,
            });
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
    pub fn writeFrame(self: *Self, allocator: *mem.Allocator, frame: anytype) !void {
        // Lock the writer if necessary
        var heldWriteLock: std.event.Lock.Held = undefined;
        if (std.io.is_async) {
            heldWriteLock = self.write_lock.acquire();
        }
        defer if (std.io.is_async) heldWriteLock.release();

        // Reset primitive writer
        // TODO(vincent): for async we probably should do something else for the primitive writer.
        try self.primitive_writer.reset(allocator);
        defer self.primitive_writer.deinit(allocator);

        // Prepare the raw frame
        var raw_frame = RawFrame{
            .header = FrameHeader{
                .version = self.options.protocol_version,
                .flags = 0,
                .stream = 0,
                .opcode = frame.opcode,
                .body_len = 0,
            },
            .body = &[_]u8{},
        };

        if (self.options.protocol_version.is(5)) {
            raw_frame.header.flags |= FrameFlags.UseBeta;
        }

        const FrameType = @TypeOf(frame);

        if (@hasField(FrameType, "body")) {
            // Encode body
            const TypeOfWriteFn = @TypeOf(frame.body.write);
            const typeInfo = @typeInfo(TypeOfWriteFn);
            if (typeInfo.BoundFn.args.len == 3) {
                try frame.body.write(self.options.protocol_version, &self.primitive_writer);
            } else {
                try frame.body.write(&self.primitive_writer);
            }

            // This is the actual bytes of the encoded body.
            const written = self.primitive_writer.getWritten();

            // Default to using the uncompressed body.
            raw_frame.header.body_len = @intCast(u32, written.len);
            raw_frame.body = written;

            // Compress the body if we can use it.
            if (!@hasField(FrameType, "no_compression")) {
                if (self.options.compression) |compression| {
                    switch (compression) {
                        .LZ4 => {
                            const compressed_data = try lz4.compress(allocator, written);

                            raw_frame.header.flags |= FrameFlags.Compression;
                            raw_frame.header.body_len = @intCast(u32, compressed_data.len);
                            raw_frame.body = compressed_data;
                        },
                        .Snappy => {
                            comptime if (!build_options.with_snappy) return error.InvalidCompressedFrame;

                            const compressed_data = try snappy.compress(allocator, written);

                            raw_frame.header.flags |= FrameFlags.Compression;
                            raw_frame.header.body_len = @intCast(u32, compressed_data.len);
                            raw_frame.body = compressed_data;
                        },
                    }
                }
            }
        }

        try self.raw_frame_writer.write(raw_frame);
        try self.buffered_writer.flush();
    }

    pub const ReadFrameOptions = struct {
        frame_allocator: *mem.Allocator,
    };

    pub fn readFrame(self: *Self, allocator: *mem.Allocator, options: ?ReadFrameOptions) !Frame {
        const raw_frame = try self.readRawFrame(allocator);
        defer raw_frame.deinit(allocator);

        self.primitive_reader.reset(raw_frame.body);

        var frame_allocator = if (options) |opts| opts.frame_allocator else allocator;

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

    fn readRawFrame(self: *Self, allocator: *mem.Allocator) !RawFrame {
        var raw_frame = try self.raw_frame_reader.read(allocator);

        if (raw_frame.header.flags & FrameFlags.Compression == FrameFlags.Compression) {
            const compression = self.options.compression orelse return error.InvalidCompressedFrame;

            switch (compression) {
                .LZ4 => {
                    const decompressed_data = try lz4.decompress(allocator, raw_frame.body);
                    raw_frame.body = decompressed_data;
                },
                .Snappy => {
                    comptime if (!build_options.with_snappy) return error.InvalidCompressedFrame;

                    const decompressed_data = try snappy.decompress(allocator, raw_frame.body);
                    raw_frame.body = decompressed_data;
                },
            }
        }

        return raw_frame;
    }
};
