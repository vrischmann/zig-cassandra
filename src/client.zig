const builtin = @import("builtin");
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

    /// Contains the state that is negotiated with a node as part of the handshake.
    const NegotiatedState = struct {
        protocol_version: ProtocolVersion,
        cql_version: CQLVersion,
    };

    /// Maps a prepared statement id to the types of the arguments needed when executing it.
    const PreparedStatementsMetadata = std.HashMap([]const u8, []OptionID, std.hash_map.hashString, std.hash_map.eqlString);

    arena: std.heap.ArenaAllocator,
    options: InitOptions,

    /// The socket to talk to the Cassandra node.
    socket: std.fs.File,

    /// Helpers types needed to decode the CQL protocol.
    raw_frame_reader: RawFrameReaderType,
    raw_frame_writer: RawFrameWriterType,
    primitive_reader: PrimitiveReader,
    primitive_writer: PrimitiveWriter,

    prepared_statements_metadata: PreparedStatementsMetadata,

    // Negotiated with the server
    negotiated_state: NegotiatedState,

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

        /// If this is provided, init will populate some information about failures.
        /// This will provide more detail than an error can.
        diags: ?*Diagnostics = null,

        pub const Diagnostics = struct {
            /// The error message returned by the Cassandra node.
            message: []const u8 = "",
        };
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

        self.prepared_statements_metadata = PreparedStatementsMetadata.init(&self.arena.allocator);

        var dummy_diags = InitOptions.Diagnostics{};
        var diags = options.diags orelse &dummy_diags;

        _ = try self.handshake(diags);
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

    pub fn cprepare(self: *Self, allocator: *mem.Allocator, options: QueryOptions, comptime query_string: []const u8, args: var) ![]const u8 {
        var dummy_diags = QueryOptions.Diagnostics{};
        var diags = options.diags orelse &dummy_diags;

        // Check that the query makes sense for the arguments provided.

        comptime {
            const bind_markers = countBindMarkers(query_string);
            const fields = @typeInfo(@TypeOf(args)).Struct.fields.len;

            if (bind_markers != fields) {
                @compileLog("number of arguments = ", fields);
                @compileLog("number of bind markers = ", bind_markers);
                @compileError("Query string has different number of bind markers than the number of arguments provided");
            }
        }

        var option_ids = std.ArrayList(OptionID).init(allocator);
        try computeValues(allocator, null, &option_ids, args);

        // TODO(vincent): need to store the metadata if present so that it can later be used with EXECUTE
        // while adding the flag Skip_Metadata.
        // See §4.1.4 in the protocol spec.

        var result = try self.writePrepare(allocator, diags, query_string);
        switch (result) {
            .Prepared => |prepared| {
                const id = prepared.query_id;

                _ = try self.prepared_statements_metadata.put(id, option_ids.toOwnedSlice());
                return id;
            },
            else => return error.InvalidServerResponse,
        }
    }

    // TODO(vincent): maybe add not comptime equivalent ?

    pub fn cquery(self: *Self, allocator: *mem.Allocator, options: QueryOptions, comptime query_string: []const u8, args: var) !?Iterator {
        var dummy_diags = QueryOptions.Diagnostics{};
        var diags = options.diags orelse &dummy_diags;

        // Check that the query makes sense for the arguments provided.

        comptime {
            const bind_markers = countBindMarkers(query_string);
            const fields = @typeInfo(@TypeOf(args)).Struct.fields.len;

            if (bind_markers != fields) {
                @compileLog("number of arguments = ", fields);
                @compileLog("number of bind markers = ", bind_markers);
                @compileError("Query string has different number of bind markers than the number of arguments provided");
            }
        }

        var values = std.ArrayList(Value).init(allocator);
        try computeValues(allocator, &values, null, args);

        // TODO(vincent): handle named values
        // TODO(vincent): handle skip_metadata (see §4.1.4 in the spec
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
        return switch (result) {
            .Rows => |rows| Iterator.init(rows.metadata, rows.data),
            else => null,
        };
    }

    pub fn execute(self: *Self, allocator: *mem.Allocator, options: QueryOptions, query_id: []const u8, args: var) !?Iterator {
        var dummy_diags = QueryOptions.Diagnostics{};
        var diags = options.diags orelse &dummy_diags;

        // If the metadata doesn't exist we can't proceed.
        const prepared_option_ids = self.prepared_statements_metadata.get(query_id);
        if (prepared_option_ids == null) {
            return error.InvalidPreparedQueryID;
        }

        var values = std.ArrayList(Value).init(allocator);
        var option_ids = std.ArrayList(OptionID).init(allocator);
        defer option_ids.deinit();
        try computeValues(allocator, &values, &option_ids, args);

        // Now that we have both prepared and compute option IDs, check that they're compatible
        if (!areOptionIDsEqual(prepared_option_ids.?.value, option_ids.span())) {
            // TODO(vincent): do we want diags here ?
            return error.InvalidPreparedStatementExecuteArgs;
        }

        // Check that the values provided are compatible with the prepared statement

        // TODO(vincent): handle named values
        // TODO(vincent): handle skip_metadata (see §4.1.4 in the spec
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

        var result = try self.writeExecute(allocator, diags, query_id, parameters);
        return switch (result) {
            .Rows => |rows| Iterator.init(rows.metadata, rows.data),
            else => null,
        };
    }

    /// Compute a list of Value and OptionID for each field in the tuple or struct args.
    fn computeValues(allocator: *mem.Allocator, values: ?*std.ArrayList(Value), options: ?*std.ArrayList(OptionID), args: var) !void {
        if (@typeInfo(@TypeOf(args)) != .Struct) {
            @compileError("Expected tuple or struct argument, found " ++ @typeName(args) ++ " of type " ++ @tagName(@typeInfo(args)));
        }

        var dummy_vals = std.ArrayList(Value).init(allocator);
        defer dummy_vals.deinit();
        var vals = values orelse &dummy_vals;

        var dummy_opts = std.ArrayList(OptionID).init(allocator);
        defer dummy_opts.deinit();
        var opts = options orelse &dummy_opts;

        inline for (@typeInfo(@TypeOf(args)).Struct.fields) |struct_field, i| {
            const Type = struct_field.field_type;
            const field_type_info = @typeInfo(Type);
            const field_name = struct_field.name;

            const arg = @field(args, field_name);

            var value: Value = undefined;
            switch (field_type_info) {
                .Int => |info| {
                    switch (Type) {
                        i8, u8 => try opts.append(.Tinyint),
                        i16, u16 => try opts.append(.Smallint),
                        i32, u32 => try opts.append(.Int),
                        i64, u64 => try opts.append(.Bigint),
                        else => @compileError("field type " ++ @typeName(Type) ++ " is not compatible with CQL"),
                    }

                    const buf = try allocator.alloc(u8, info.bits / 8);

                    mem.writeIntSliceBig(struct_field.field_type, buf, arg);
                    value = Value{ .Set = buf };
                },
                else => @compileError("field type " ++ @typeName(Type) ++ " not handled yet"),
            }

            try vals.append(value);
        }
    }

    fn areOptionIDsEqual(prepared: []OptionID, computed: []OptionID) bool {
        return mem.eql(OptionID, prepared, computed);
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

        try self.writeOptions(&fba.allocator);

        var startup_response = try self.writeStartup(&fba.allocator);

        switch (startup_response) {
            .Ready => return,
            .Authenticate => |frame| {
                try self.authenticate(&fba.allocator, diags, frame.authenticator);
            },
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

        return try self.writeAuthResponse(
            allocator,
            diags,
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

        if (raw_frame.header.opcode != .Supported) {
            return error.InvalidServerResponse;
        }

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
            var frame = StartupFrame{
                .cql_version = self.negotiated_state.cql_version,
                .compression = self.options.compression,
            };

            // Encode body
            self.primitive_writer.reset(allocator);
            defer self.primitive_writer.deinit(allocator);
            _ = try frame.write(&self.primitive_writer);

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
            else => error.InvalidServerResponse,
        };
    }

    fn writeAuthResponse(self: *Self, allocator: *mem.Allocator, diags: *InitOptions.Diagnostics, username: []const u8, password: []const u8) !void {
        // Write AUTH_RESPONSE
        {
            var buf: [512]u8 = undefined;
            const token = try std.fmt.bufPrint(&buf, "\x00{}\x00{}", .{ username, password });

            var frame = AuthResponseFrame{ .token = token };

            // Encode body
            self.primitive_writer.reset(allocator);
            defer self.primitive_writer.deinit(allocator);
            _ = try frame.write(&self.primitive_writer);

            // Write raw frame
            const raw_frame = try self.makeRawFrame(allocator, .AuthResponse, false);
            _ = try self.raw_frame_writer.write(raw_frame);
        }

        // Read either AUTH_CHALLENGE, AUTH_SUCCESS or ERROR
        const raw_frame = try self.readRawFrame(allocator);
        self.primitive_reader.reset(raw_frame.body);

        switch (raw_frame.header.opcode) {
            .AuthChallenge => unreachable,
            .AuthSuccess => return,
            .Error => {
                var error_frame = try ErrorFrame.read(allocator, &self.primitive_reader);
                diags.message = error_frame.message;
                return error.AuthenticationFailed;
            },
            else => return error.InvalidServerResponse,
        }
    }

    fn writeQuery(self: *Self, allocator: *mem.Allocator, diags: *Client.QueryOptions.Diagnostics, query: []const u8, query_parameters: QueryParameters) !Result {
        // Write QUERY
        {
            var frame = QueryFrame{
                .query = query,
                .query_parameters = query_parameters,
            };

            // Encode body
            self.primitive_writer.reset(allocator);
            defer self.primitive_writer.deinit(allocator);
            _ = try frame.write(self.negotiated_state.protocol_version, &self.primitive_writer);

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
            else => return error.InvalidServerResponse,
        };
    }

    fn writePrepare(self: *Self, allocator: *mem.Allocator, diags: *Client.QueryOptions.Diagnostics, query: []const u8) !Result {
        // Write PREPARE
        {
            var frame = PrepareFrame{
                .query = query,
                .keyspace = null,
            };

            // Encode body
            self.primitive_writer.reset(allocator);
            defer self.primitive_writer.deinit(allocator);
            _ = try frame.write(self.negotiated_state.protocol_version, &self.primitive_writer);

            // Write raw frame
            const raw_frame = try self.makeRawFrame(allocator, .Prepare, false);
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
                return error.QueryPreparationFailed;
            },
            else => return error.InvalidServerResponse,
        };
    }

    fn writeExecute(self: *Self, allocator: *mem.Allocator, diags: *Client.QueryOptions.Diagnostics, query_id: []const u8, query_parameters: QueryParameters) !Result {
        // Write EXECUTE
        {
            var frame = ExecuteFrame{
                .query_id = query_id,
                .result_metadata_id = null,
                .query_parameters = query_parameters,
            };

            // Encode body
            self.primitive_writer.reset(allocator);
            defer self.primitive_writer.deinit(allocator);
            _ = try frame.write(self.negotiated_state.protocol_version, &self.primitive_writer);

            // Write raw frame
            const raw_frame = try self.makeRawFrame(allocator, .Execute, false);
            _ = try self.raw_frame_writer.write(raw_frame);
        }

        // Read RESULT
        // TODO(vincent): this is the same as in writeQuery, can we DRY it up ?
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
            else => return error.InvalidServerResponse,
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
