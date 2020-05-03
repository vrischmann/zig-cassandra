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

pub const QueryOptions = struct {
    /// If this is provided the client will try to limit the size of the resultset.
    /// Note that even if the query is paged, cassandra doesn't guarantee that there will
    /// be at most `page_size` rows, it can be slightly smaller or bigger.
    ///
    /// If page_size is not null, after execution the `paging_state` field in this struct will be
    /// populated if paging is necessary, otherwise it will be null.
    page_size: ?u32 = null,

    /// If this is provided it will be used to page the result of the query.
    /// Additionally, this will be populated by the client with the next paging state if any.
    paging_state: ?[]const u8 = null,

    /// If this is provided it will be populated in case of failures.
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

pub const TCPClient = struct {
    const Self = @This();
    const ClientType = Client(std.fs.File.InStream, std.fs.File.OutStream);

    socket: std.fs.File,

    u: ClientType,

    pub fn init(self: *Self, allocator: *mem.Allocator, options: InitOptions) !void {
        self.socket = try net.tcpConnectToAddress(options.seed_address);

        _ = try self.u.init(
            allocator,
            options,
            self.socket.inStream(),
            self.socket.outStream(),
        );
    }

    pub inline fn prepare(self: *Self, allocator: *mem.Allocator, options: QueryOptions, comptime query_string: []const u8, args: var) ![]const u8 {
        return self.u.prepare(allocator, options, query_string, args);
    }

    pub inline fn execute(self: *Self, allocator: *mem.Allocator, options: QueryOptions, query_id: []const u8, args: var) !?Iterator {
        return self.u.execute(allocator, options, query_id, args);
    }

    pub inline fn query(self: *Self, allocator: *mem.Allocator, options: QueryOptions, comptime query_string: []const u8, args: var) !?Iterator {
        return self.u.query(allocator, options, query_string, args);
    }
};

pub fn Client(comptime InStreamType: type, comptime OutStreamType: type) type {
    return struct {
        const Self = @This();

        const RawFrameReaderType = RawFrameReader(InStreamType);
        const RawFrameWriterType = RawFrameWriter(OutStreamType);

        /// Contains the state that is negotiated with a node as part of the handshake.
        const NegotiatedState = struct {
            protocol_version: ProtocolVersion,
            cql_version: CQLVersion,
        };

        const PreparedStatementMetadataValue = struct {
            metadata: PreparedMetadata,
            rows_metadata: RowsMetadata,
        };

        /// Maps a prepared statement id to the types of the arguments needed when executing it.
        const PreparedStatementsMetadata = std.HashMap([]const u8, PreparedStatementMetadataValue, std.hash_map.hashString, std.hash_map.eqlString);

        allocator: *mem.Allocator,

        options: InitOptions,

        /// Helpers types needed to decode the CQL protocol.
        raw_frame_reader: RawFrameReaderType,
        raw_frame_writer: RawFrameWriterType,
        primitive_reader: PrimitiveReader,
        primitive_writer: PrimitiveWriter,

        /// TODO(vincent): need to implement some sort of TLL or size limit for this.
        prepared_statements_metadata: PreparedStatementsMetadata,

        // Negotiated with the server
        negotiated_state: NegotiatedState,

        pub fn init(self: *Self, allocator: *mem.Allocator, options: InitOptions, in_stream: InStreamType, out_stream: OutStreamType) !void {
            self.allocator = allocator;
            self.options = options;

            self.raw_frame_reader = RawFrameReaderType.init(in_stream);
            self.raw_frame_writer = RawFrameWriterType.init(out_stream);
            self.primitive_reader = PrimitiveReader.init();
            self.primitive_writer = PrimitiveWriter.init();

            self.prepared_statements_metadata = PreparedStatementsMetadata.init(allocator);

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

        pub fn prepare(self: *Self, allocator: *mem.Allocator, options: QueryOptions, comptime query_string: []const u8, args: var) ![]const u8 {
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

            var result = try self.writePrepare(self.allocator, allocator, diags, query_string);
            switch (result) {
                .Prepared => |prepared| {
                    const id = prepared.query_id;

                    // Store the metadata for later use with `execute`.

                    const gop = try self.prepared_statements_metadata.getOrPut(id);
                    if (gop.found_existing) {
                        gop.kv.value.metadata.deinit(self.allocator);
                        gop.kv.value.rows_metadata.deinit(self.allocator);
                    }

                    gop.kv.value = undefined;
                    gop.kv.value.metadata = prepared.metadata;
                    gop.kv.value.rows_metadata = prepared.rows_metadata;

                    return id;
                },
                else => return error.InvalidServerResponse,
            }
        }

        // TODO(vincent): maybe add not comptime equivalent ?

        pub fn query(self: *Self, allocator: *mem.Allocator, options: QueryOptions, comptime query_string: []const u8, args: var) !?Iterator {
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
                .page_size = options.page_size,
                .paging_state = options.paging_state,
                .serial_consistency_level = null,
                .timestamp = null,
                .keyspace = null,
                .now_in_seconds = null,
            };
            parameters.values = Values{ .Normal = values.toOwnedSlice() };

            var result = try self.writeQuery(allocator, diags, query_string, parameters);
            return switch (result) {
                .Rows => |rows| blk: {
                    break :blk Iterator.init(rows.metadata, rows.data);
                },
                else => null,
            };
        }

        pub fn execute(self: *Self, allocator: *mem.Allocator, options: QueryOptions, query_id: []const u8, args: var) !?Iterator {
            var dummy_diags = QueryOptions.Diagnostics{};
            var diags = options.diags orelse &dummy_diags;

            // If the metadata doesn't exist we can't proceed.
            const prepared_statement_metadata_kv = self.prepared_statements_metadata.get(query_id);
            if (prepared_statement_metadata_kv == null) {
                return error.InvalidPreparedQueryID;
            }

            var values = std.ArrayList(Value).init(allocator);
            var option_ids = std.ArrayList(OptionID).init(allocator);
            defer option_ids.deinit();
            try computeValues(allocator, &values, &option_ids, args);

            // Now that we have both prepared and compute option IDs, check that they're compatible
            if (!areOptionIDsEqual(prepared_statement_metadata_kv.?.value.metadata.column_specs, option_ids.span())) {
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
                .page_size = options.page_size,
                .paging_state = options.paging_state,
                .serial_consistency_level = null,
                .timestamp = null,
                .keyspace = null,
                .now_in_seconds = null,
            };
            parameters.values = Values{ .Normal = values.toOwnedSlice() };

            var result = try self.writeExecute(allocator, diags, query_id, parameters);
            return switch (result) {
                .Rows => |rows| blk: {
                    break :blk Iterator.init(rows.metadata, rows.data);
                },
                else => null,
            };
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

        fn writeQuery(self: *Self, allocator: *mem.Allocator, diags: *QueryOptions.Diagnostics, query_string: []const u8, query_parameters: QueryParameters) !Result {
            // Write QUERY
            {
                var frame = QueryFrame{
                    .query = query_string,
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

        fn writePrepare(self: *Self, allocator: *mem.Allocator, metadata_allocator: *mem.Allocator, diags: *QueryOptions.Diagnostics, query_string: []const u8) !Result {
            // Write PREPARE
            {
                var frame = PrepareFrame{
                    .query = query_string,
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
                    // Since we want to retain the metadata from the result frame for later use we can't use the default allocator which will only be valid per-call.
                    // Instead take a dedicated metadata_allocator for this.
                    var frame = try ResultFrame.read(metadata_allocator, self.negotiated_state.protocol_version, &self.primitive_reader);
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

        fn writeExecute(self: *Self, allocator: *mem.Allocator, diags: *QueryOptions.Diagnostics, query_id: []const u8, query_parameters: QueryParameters) !Result {
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
            var raw_frame = try self.raw_frame_reader.read(allocator);

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
}

/// Compute a list of Value and OptionID for each field in the tuple or struct args.
///
/// TODO(vincent): it's not clear to the caller that data in `args` must outlive `values` because we don't duplicating memory
/// unless absolutely necessary in the case of arrays.
/// Think of a way to communicate that.
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
            .Bool => {
                var buf = try allocator.alloc(u8, 1);
                errdefer allocator.free(buf);

                buf[0] = if (arg) 0x01 else 0x00;

                try opts.append(.Boolean);
                value = Value{ .Set = buf };
                try vals.append(value);
            },
            .Float => |info| {
                switch (Type) {
                    f32 => try opts.append(.Float),
                    f64 => try opts.append(.Double),
                    else => @compileError("field type " ++ @typeName(Type) ++ " is not compatible with CQL"),
                }

                var buf = try allocator.alloc(u8, info.bits / 8);
                errdefer allocator.free(buf);

                @ptrCast(*align(1) Type, buf).* = arg;

                value = Value{ .Set = buf };
                try vals.append(value);
            },
            .Int => |info| {
                switch (Type) {
                    i8, u8 => try opts.append(.Tinyint),
                    i16, u16 => try opts.append(.Smallint),
                    i32, u32 => try opts.append(.Int),
                    i64, u64 => try opts.append(.Bigint),
                    else => @compileError("field type " ++ @typeName(Type) ++ " is not compatible with CQL"),
                }

                var buf = try allocator.alloc(u8, info.bits / 8);
                errdefer allocator.free(buf);

                // THe std lib provides writIngBig and writeIntSliceBig which we could use, but:
                // * the first one takes a pointer to N bytes which requires a @ptrCast anyway
                //   since we need to use buf.
                // * the second doesn't work with signed integers right now.

                switch (builtin.endian) {
                    .Little => @ptrCast(*align(1) Type, buf).* = arg,
                    .Big => @ptrCast(*align(1) Type, buf).* = @byteSwap(Type, arg),
                }

                value = Value{ .Set = buf };
                try vals.append(value);
            },
            .Pointer => |pointer| switch (pointer.size) {
                .One => {
                    try computeValues(allocator, values, options, .{arg.*});
                    continue;
                },
                .Slice => {
                    // Special case []const u8 because it's used for strings.
                    if (pointer.is_const and pointer.child == u8) {
                        try opts.append(.Varchar);
                        // TODO(vincent): should we make a copy ?
                        value = Value{ .Set = arg };
                        try vals.append(value);

                        continue;
                    }

                    // Otherwise it's a list or a set, encode a new list of values.

                    var inner_values = std.ArrayList(Value).init(allocator);
                    for (arg) |item| {
                        try computeValues(allocator, &inner_values, null, .{item});
                    }

                    // TODO(vincent): how de we know it's a set or list ?
                    try opts.append(.Set);
                    value = Value{ .Set = try concatenateValues(allocator, inner_values.toOwnedSlice()) };
                    try vals.append(value);
                },
                else => @compileError("invalid pointer size " ++ @tagName(pointer.size)),
            },
            .Array => |array| {
                // Special case [16]u8 since we consider it a UUID.
                if (array.len == 16 and array.child == u8) {
                    try opts.append(.UUID);
                    value = Value{ .Set = try mem.dupe(allocator, u8, &arg) };
                    try vals.append(value);

                    continue;
                }

                // Otherwise it's a list or a set, encode a new list of values.

                var inner_values = std.ArrayList(Value).init(allocator);
                for (arg) |item| {
                    try computeValues(allocator, &inner_values, null, .{item});
                }

                // TODO(vincent): how de we know it's a set or list ?
                try opts.append(.Set);
                value = Value{ .Set = try concatenateValues(allocator, inner_values.toOwnedSlice()) };
                try vals.append(value);
            },
            else => @compileError("field type " ++ @typeName(Type) ++ " not handled yet"),
        }
    }
}

fn concatenateValues(allocator: *mem.Allocator, values: []const Value) ![]const u8 {
    var size: usize = 0;
    for (values) |value| {
        switch (value) {
            .Set => |v| size += v.len,
            else => {},
        }
    }

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    var idx: usize = 0;
    for (values) |value| {
        switch (value) {
            .Set => |v| {
                mem.copy(u8, buf[idx..], v);
                idx += v.len;
            },
            else => {},
        }
    }

    return buf;
}

test "concatenate values" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();

    var v1 = Value{ .Set = "foobar" };
    var v2 = Value{ .Set = "barbaz" };

    var data = try concatenateValues(&arenaAllocator.allocator, &[_]Value{ v1, v2 });
    testing.expectEqual(@as(usize, 12), data.len);
    testing.expectEqualStrings("foobarbarbaz", data);
}

test "compute values: ints" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = std.ArrayList(OptionID).init(allocator);

    const my_u64 = @as(u64, 20000);

    _ = try computeValues(allocator, &values, &options, .{
        .i_tinyint = @as(i8, 0x7f),
        .u_tinyint = @as(u8, 0xff),
        .i_smallint = @as(i16, 0x7fff),
        .u_smallint = @as(u16, 0xdedf),
        .i_int = @as(i32, 0x7fffffff),
        .u_int = @as(u32, 0xabababaf),
        .i_bigint = @as(i64, 0x7fffffffffffffff),
        .u_bigint = @as(u64, 0xdcdcdcdcdcdcdcdf),
        .u_bigint_ptr = &my_u64,
    });

    var v = values.span();
    var o = options.span();

    testing.expectEqual(@as(usize, 9), v.len);
    testing.expectEqual(@as(usize, 9), o.len);

    testing.expectEqualSlices(u8, "\x7f", v[0].Set);
    testing.expectEqual(OptionID.Tinyint, o[0]);
    testing.expectEqualSlices(u8, "\xff", v[1].Set);
    testing.expectEqual(OptionID.Tinyint, o[1]);

    testing.expectEqualSlices(u8, "\xff\x7f", v[2].Set);
    testing.expectEqual(OptionID.Smallint, o[2]);
    testing.expectEqualSlices(u8, "\xdf\xde", v[3].Set);
    testing.expectEqual(OptionID.Smallint, o[3]);

    testing.expectEqualSlices(u8, "\xff\xff\xff\x7f", v[4].Set);
    testing.expectEqual(OptionID.Int, o[4]);
    testing.expectEqualSlices(u8, "\xaf\xab\xab\xab", v[5].Set);
    testing.expectEqual(OptionID.Int, o[5]);

    testing.expectEqualSlices(u8, "\xff\xff\xff\xff\xff\xff\xff\x7f", v[6].Set);
    testing.expectEqual(OptionID.Bigint, o[6]);
    testing.expectEqualSlices(u8, "\xdf\xdc\xdc\xdc\xdc\xdc\xdc\xdc", v[7].Set);
    testing.expectEqual(OptionID.Bigint, o[7]);

    testing.expectEqualSlices(u8, "\x20\x4e\x00\x00\x00\x00\x00\x00", v[8].Set);
    testing.expectEqual(OptionID.Bigint, o[8]);
}

test "compute values: floats" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = std.ArrayList(OptionID).init(allocator);

    const my_f64 = @as(f64, 402.240);

    _ = try computeValues(allocator, &values, &options, .{
        .f32 = @as(f32, 0.002),
        .f64 = @as(f64, 245601.000240305603),
        .f64_ptr = &my_f64,
    });

    var v = values.span();
    var o = options.span();

    testing.expectEqual(@as(usize, 3), v.len);
    testing.expectEqual(@as(usize, 3), o.len);

    testing.expectEqualSlices(u8, "\x6f\x12\x03\x3b", v[0].Set);
    testing.expectEqual(OptionID.Float, o[0]);
    testing.expectEqualSlices(u8, "\x46\xfd\x7d\x00\x08\xfb\x0d\x41", v[1].Set);
    testing.expectEqual(OptionID.Double, o[1]);
    testing.expectEqualSlices(u8, "\xa4\x70\x3d\x0a\xd7\x23\x79\x40", v[2].Set);
    testing.expectEqual(OptionID.Double, o[2]);
}

test "compute values: strings" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = std.ArrayList(OptionID).init(allocator);

    _ = try computeValues(allocator, &values, &options, .{
        .string = @as([]const u8, try mem.dupe(allocator, u8, "foobar")),
    });

    var v = values.span();
    var o = options.span();

    testing.expectEqual(@as(usize, 1), v.len);
    testing.expectEqual(@as(usize, 1), o.len);

    testing.expectEqualStrings("foobar", v[0].Set);
    testing.expectEqual(OptionID.Varchar, o[0]);
}

test "compute values: bool" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = std.ArrayList(OptionID).init(allocator);

    _ = try computeValues(allocator, &values, &options, .{
        .bool1 = true,
        .bool2 = false,
    });

    var v = values.span();
    var o = options.span();

    testing.expectEqual(@as(usize, 2), v.len);
    testing.expectEqual(@as(usize, 2), o.len);

    testing.expectEqualSlices(u8, "\x01", v[0].Set);
    testing.expectEqual(OptionID.Boolean, o[0]);
    testing.expectEqualSlices(u8, "\x00", v[1].Set);
    testing.expectEqual(OptionID.Boolean, o[1]);
}

test "compute values: list" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = std.ArrayList(OptionID).init(allocator);

    _ = try computeValues(allocator, &values, &options, .{
        .string = &[_]u16{ 0x01, 0x2050 },
        .string2 = @as([]const u16, &[_]u16{ 0x01, 0x2050 }),
    });

    var v = values.span();
    var o = options.span();

    testing.expectEqual(@as(usize, 2), v.len);
    testing.expectEqual(@as(usize, 2), o.len);

    testing.expectEqualSlices(u8, "\x01\x00\x50\x20", v[0].Set);
    testing.expectEqual(OptionID.Set, o[0]);

    testing.expectEqualSlices(u8, "\x01\x00\x50\x20", v[1].Set);
    testing.expectEqual(OptionID.Set, o[1]);
}

test "compute values: uuid" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = std.ArrayList(OptionID).init(allocator);

    _ = try computeValues(allocator, &values, &options, .{
        .uuid = [16]u8{
            0x55, 0x94, 0xd5, 0xb1,
            0xef, 0x84, 0x41, 0xc4,
            0xb2, 0x4e, 0x68, 0x48,
            0x8d, 0xcf, 0xa1, 0xc9,
        },
    });

    var v = values.span();
    var o = options.span();

    testing.expectEqual(@as(usize, 1), v.len);
    testing.expectEqual(@as(usize, 1), o.len);

    testing.expectEqualSlices(u8, "\x55\x94\xd5\xb1\xef\x84\x41\xc4\xb2\x4e\x68\x48\x8d\xcf\xa1\xc9", v[0].Set);
    testing.expectEqual(OptionID.UUID, o[0]);
}

fn areOptionIDsEqual(prepared: []const ColumnSpec, computed: []const OptionID) bool {
    if (prepared.len != computed.len) return false;

    for (prepared) |column_spec, i| {
        if (column_spec.option != computed[i]) return false;
    }

    return true;
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
