const std = @import("std");
const heap = std.heap;
const mem = std.mem;
const net = std.net;

const lz4 = @import("lz4.zig");

usingnamespace @import("frame.zig");
usingnamespace @import("primitive_types.zig");
usingnamespace @import("iterator.zig");
usingnamespace @import("query_parameters.zig");

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

const testing = @import("testing.zig");

pub const Client = struct {
    const Self = @This();

    const InStreamType = std.fs.File.InStream;
    const OutStreamType = std.fs.File.OutStream;
    const RawFrameReaderType = RawFrameReader(InStreamType);
    const RawFrameWriterType = RawFrameWriter(OutStreamType);

    arena: std.heap.ArenaAllocator,
    options: InitOptions,

    socket: std.fs.File,

    raw_frame_reader: RawFrameReaderType,
    raw_frame_writer: RawFrameWriterType,
    primitive_reader: PrimitiveReader,
    primitive_writer: PrimitiveWriter,

    // Negotiated with the server

    negotiated_state: NegotiatedState,

    const NegotiatedState = struct {
        protocol_version: ProtocolVersion,
        cql_version: CQLVersion,
    };

    pub const InitOptions = struct {
        /// The address of a seed node.
        seed_address: net.Address = undefined,

        /// The compression algorithm to use if possible.
        compression: ?CompressionAlgorithm = null,

        /// The default consistency to use for queries.
        consistency: Consistency = .One,

        /// The username to use for authentication if required.
        username: ?[]const u8 = null,
        /// The password to use for authentication if required.
        password: ?[]const u8 = null,
    };

    pub fn init(self: *Self, allocator: *mem.Allocator, options: InitOptions) !void {
        self.arena = std.heap.ArenaAllocator.init(allocator);
        self.options = options;

        const socket = try net.tcpConnectToAddress(self.options.seed_address);
        const socket_in_stream = socket.inStream();
        const socket_out_stream = socket.outStream();

        self.socket = socket;

        self.raw_frame_reader = RawFrameReaderType.init(allocator, socket_in_stream);
        self.raw_frame_writer = RawFrameWriterType.init(socket_out_stream);
        self.primitive_reader = PrimitiveReader.init();
        self.primitive_writer = PrimitiveWriter.init();

        _ = try self.handshake();
    }

    pub fn setUsername(self: *Self, username: []const u8) void {
        self.username = username;
    }
    pub fn setPassword(self: *Self, password: []const u8) void {
        self.password = password;
    }
    pub fn setConsistency(self: *Self, consistency: Consistency) void {
        self.consistency = consistency;
    }

    pub const QueryOptions = struct {
        /// If this is provided, cquery will populate some information about failures.
        /// This will provide more detail than an error can.
        diags: ?*Diagnostics = null,

        pub const Diagnostics = struct {
            /// The error message returned by the Cassandra node.
            message: []const u8 = "",

            unavailable_replicas: ?UnavailableReplicasError = null,
            function_failure: ?FunctionFailureError = null,
            write_timeout: ?WriteError.Timeout = null,
            read_timeout: ?ReadError.Timeout = null,
            write_failure: ?WriteError.Failure = null,
            read_failure: ?ReadError.Failure = null,
            cas_write_unknown: ?WriteError.CASUnknown = null,
            already_exists: ?AlreadyExistsError = null,
            unprepared: ?UnpreparedError = null,
        };
    };

    // TODO(vincent): maybe add not comptime equivalent ?

    pub fn cquery(self: *Self, allocator: *mem.Allocator, options: QueryOptions, comptime query_string: []const u8, args: var) !?Iterator {
        var dummy_diags = QueryOptions.Diagnostics{};
        var diags = options.diags orelse &dummy_diags;

        if (@typeInfo(@TypeOf(args)) != .Struct) {
            @compileError("Expected tuple or struct argument, found " ++ @typeName(args) ++ " of type " ++ @tagName(@typeInfo(args)));
        }

        comptime {
            const bind_markers = countBindMarkers(query_string);
            const fields = @typeInfo(@TypeOf(args)).Struct.fields.len;

            if (bind_markers != fields) {
                @compileLog("number of arguments = ", fields);
                @compileLog("number of bind markers = ", bind_markers);
                @compileError("Query string has different number of bind markers than the number of arguments provided");
            }
        }

        std.debug.assert(self.socket.handle > 0);

        var values = std.ArrayList(Value).init(allocator);

        inline for (@typeInfo(@TypeOf(args)).Struct.fields) |struct_field, i| {
            const field_type_info = @typeInfo(struct_field.field_type);
            const field_name = struct_field.name;

            const arg = @field(args, field_name);
            try appendValueToQueryParameter(allocator, &values, struct_field.field_type, arg);
        }

        // TODO(vincent): handle named values
        var parameters = QueryParameters{
            .consistency_level = self.options.consistency,
            .values = undefined,
            .skip_metadata = false,
            .page_size = null,
            .paging_state = null,
            .serial_consistency_level = null,
            .timestamp = null,
            .keyspace = null,
            .now_in_seconds = null,
        };
        parameters.values = Values{ .Normal = values.toOwnedSlice() };

        var result = try self.writeQuery(allocator, diags, query_string, parameters);
        switch (result) {
            .Rows => |rows| return Iterator.init(rows.metadata, rows.data),
            else => return null,
        }
    }

    fn handshake(self: *Self) !void {
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

        try self.writeOptions(&fba.allocator);

        var startup_response = try self.writeStartup(&fba.allocator);

        switch (startup_response) {
            .Ready => return,
            .Authenticate => |frame| {
                var auth_result = try self.authenticate(frame.authenticator);
                switch (auth_result) {
                    .Challenge => unreachable,
                    .Success => return,
                    .Error => |err| {
                        std.debug.panic("error code: {} message: {}\n", .{ err.error_code, err.message });
                    },
                }
            },
        }
    }

    fn authenticate(self: *Self, authenticator: []const u8) !AuthResult {
        // TODO(vincent): handle authenticator classes
        // TODO(vincent): handle auth challenges
        if (self.options.username == null) {
            return error.NoUsername;
        }
        if (self.options.password == null) {
            return error.NoPasswors;
        }

        return try self.writeAuthResponse(
            self.options.username.?,
            self.options.password.?,
        );
    }

    // Below are methods to write different frames.

    fn writeOptions(self: *Self, allocator: *mem.Allocator) !void {
        // Write OPTIONS
        {
            const raw_frame = RawFrame{
                .header = FrameHeader{
                    .version = ProtocolVersion{ .version = 4 },
                    .flags = 0,
                    .stream = 0,
                    .opcode = .Options,
                    .body_len = 0,
                },
                .body = &[_]u8{},
            };
            _ = try self.raw_frame_writer.write(raw_frame);
        }

        // Read SUPPORTED
        const raw_frame = try self.readRawFrame(allocator);
        defer raw_frame.deinit(allocator);

        if (raw_frame.header.opcode != .Supported) return error.InvalidResponseInHandshake;

        self.primitive_reader.reset(raw_frame.body);

        var supported_frame = try SupportedFrame.read(allocator, &self.primitive_reader);

        // TODO(vincent): handle compression
        if (supported_frame.protocol_versions.len > 0) {
            self.negotiated_state.protocol_version = supported_frame.protocol_versions[0];
        } else {
            self.negotiated_state.protocol_version = ProtocolVersion{ .version = 4 };
        }
        self.negotiated_state.cql_version = supported_frame.cql_versions[0];
    }

    fn writeStartup(self: *Self, allocator: *mem.Allocator) !StartupResponse {
        // Write STARTUP
        {
            var startup_frame = StartupFrame{
                .cql_version = self.negotiated_state.cql_version,
                .compression = self.options.compression,
            };

            // Encode body
            self.primitive_writer.reset(allocator);
            defer self.primitive_writer.deinit(allocator);
            _ = try startup_frame.write(&self.primitive_writer);

            // Write raw frame
            const raw_frame = try self.makeRawFrame(allocator, .Startup, true);
            _ = try self.raw_frame_writer.write(raw_frame);
        }

        // Read either READY or AUTHENTICATE
        const raw_frame = try self.readRawFrame(allocator);
        defer raw_frame.deinit(allocator);

        self.primitive_reader.reset(raw_frame.body);

        return switch (raw_frame.header.opcode) {
            .Ready => StartupResponse{ .Ready = .{} },
            .Authenticate => StartupResponse{
                .Authenticate = try AuthenticateFrame.read(allocator, &self.primitive_reader),
            },
            else => error.InvalidResponseToStartup,
        };
    }

    fn writeAuthResponse(self: *Self, username: []const u8, password: []const u8) !AuthResult {
        // TODO(vincent): do we need diagnostics ?
        unreachable;
    }

    fn writeQuery(self: *Self, allocator: *mem.Allocator, diags: *Client.QueryOptions.Diagnostics, query: []const u8, query_parameters: QueryParameters) !Result {
        // Write QUERY
        {
            var query_frame = QueryFrame{
                .query = query,
                .query_parameters = query_parameters,
            };

            // Encode body
            self.primitive_writer.reset(allocator);
            defer self.primitive_writer.deinit(allocator);
            _ = try query_frame.write(self.negotiated_state.protocol_version, &self.primitive_writer);

            // Write raw frame
            const raw_frame = try self.makeRawFrame(allocator, .Query, false);
            _ = try self.raw_frame_writer.write(raw_frame);
        }

        // Read RESULT
        const raw_frame = try self.readRawFrame(allocator);
        self.primitive_reader.reset(raw_frame.body);

        return switch (raw_frame.header.opcode) {
            .Result => blk: {
                var frame = try ResultFrame.read(allocator, self.negotiated_state.protocol_version, &self.primitive_reader);
                break :blk frame.result;
            },
            .Error => {
                var error_frame = try ErrorFrame.read(allocator, &self.primitive_reader);
                diags.message = error_frame.message;
                return error.QueryExecutionFailed;
            },
            else => std.debug.panic("invalid server response opcode {}\n", .{raw_frame.header.opcode}),
        };
    }

    fn readRawFrame(self: *Self, allocator: *mem.Allocator) !RawFrame {
        var raw_frame = try self.raw_frame_reader.read();

        if (raw_frame.header.flags & FrameFlags.Compression == FrameFlags.Compression) blk: {
            const decompressed_data = try lz4.decompress(allocator, raw_frame.body);
            raw_frame.body = decompressed_data;
        }

        return raw_frame;
    }

    fn makeRawFrame(self: *Self, allocator: *mem.Allocator, opcode: Opcode, force_no_compression: bool) !RawFrame {
        var raw_frame: RawFrame = undefined;

        const written = self.primitive_writer.getWritten();

        raw_frame.header = FrameHeader{
            .version = self.negotiated_state.protocol_version,
            .flags = 0,
            .stream = 0,
            .opcode = opcode,
            .body_len = @intCast(u32, written.len),
        };

        if (force_no_compression) {
            raw_frame.body = self.primitive_writer.getWritten();
        } else {
            raw_frame.body = if (self.options.compression) |compression| blk: {
                switch (compression) {
                    .LZ4 => {
                        const compressed_data = try lz4.compress(allocator, written);

                        raw_frame.header.flags |= FrameFlags.Compression;
                        raw_frame.header.body_len = @intCast(u32, compressed_data.len);

                        break :blk compressed_data;
                    },
                    else => std.debug.panic("compression algorithm {} not handled yet", .{compression}),
                }
            } else
                self.primitive_writer.getWritten();
        }

        // TODO(vincent): implement streams
        // TODO(vincent): only compress if it's useful ?

        return raw_frame;
    }
};

fn appendValueToQueryParameter(allocator: *mem.Allocator, values: *std.ArrayList(Value), comptime Type: type, arg: Type) !void {
    const type_info = @typeInfo(Type);

    var value: Value = undefined;
    switch (type_info) {
        .Int => |info| {
            const buf = try allocator.alloc(u8, info.bits / 8);

            mem.writeIntSliceBig(Type, buf, arg);
            value = Value{ .Set = buf };
        },
        else => @compileError("field type " ++ @typeName(Type) ++ " not handled yet"),
    }

    try values.append(value);
}

fn countBindMarkers(query_string: []const u8) usize {
    var pos: usize = 0;
    var count: usize = 0;

    while (mem.indexOfScalarPos(u8, query_string, pos, '?')) |i| {
        count += 1;
        pos = i + 1;
    }

    return count;
}

test "count bind markers" {
    const query_string = "select * from foobar.user where id = ? and name = ? and age < ?";
    const count = countBindMarkers(query_string);
    testing.expectEqual(@as(usize, 3), count);
}

const AuthResultTag = enum {
    Challenge,
    Success,
    Error,
};
const AuthResult = union(AuthResultTag) {
    Challenge: AuthChallengeFrame,
    Success: ReadyFrame,
    Error: ErrorFrame,
};
const StartupResponseTag = enum {
    Ready,
    Authenticate,
};
const StartupResponse = union(StartupResponseTag) {
    Ready: ReadyFrame,
    Authenticate: AuthenticateFrame,
};

test "" {
    _ = @import("lz4.zig");
}
