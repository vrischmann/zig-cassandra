const builtin = @import("builtin");
const build_options = @import("build_options");
const std = @import("std");
const big = std.math.big;
const heap = std.heap;
const io = std.io;
const log = std.log;
const mem = std.mem;
const net = std.net;

const lz4 = @import("lz4.zig");

const FrameHeader = @import("frame.zig").FrameHeader;
const FrameFlags = @import("frame.zig").FrameFlags;
const RawFrame = @import("frame.zig").RawFrame;
const RawFrameReader = @import("frame.zig").RawFrameReader;
const RawFrameWriter = @import("frame.zig").RawFrameWriter;

const PrimitiveReader = @import("primitive/reader.zig").PrimitiveReader;
const PrimitiveWriter = @import("primitive/writer.zig").PrimitiveWriter;

const PreparedMetadata = @import("metadata.zig").PreparedMetadata;
const RowsMetadata = @import("metadata.zig").RowsMetadata;
const ColumnSpec = @import("metadata.zig").ColumnSpec;

const bigint = @import("bigint.zig");

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
const casstest = @import("casstest.zig");

pub const InitOptions = struct {
    /// the protocl version to use.
    protocol_version: ProtocolVersion = ProtocolVersion{ .version = @as(u8, 4) },

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

        execute: Execute = .{},

        const Execute = struct {
            not_enough_args: ?bool = null,
            first_incompatible_arg: ?ExecuteIncompatibleArg = null,
            const ExecuteIncompatibleArg = struct {
                position: usize = 0,
                prepared: ColumnSpec = .{},
                argument: ?OptionID = null,
            };

            pub fn format(value: Execute, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
                var buf: [1024]u8 = undefined;
                var fbs = io.fixedBufferStream(&buf);
                var fbw = fbs.writer();

                if (value.not_enough_args) |v| {
                    if (v) {
                        try std.fmt.format(fbw, "not enough args, ", .{});
                    }
                }
                if (value.first_incompatible_arg) |v| {
                    try std.fmt.format(fbw, "first incompatible arg: {{position: {}, prepared: (name: {}, type: {}), argument: {}}}", .{
                        v.position,
                        v.prepared.name,
                        v.prepared.option,
                        v.argument,
                    });
                }

                try writer.writeAll(fbs.getWritten());
            }
        };
    };
};

pub const Client = struct {
    const BufferedReaderType = io.BufferedReader(4096, std.fs.File.Reader);
    const BufferedWriterType = io.BufferedWriter(4096, std.fs.File.Writer);

    const RawFrameReaderType = RawFrameReader(BufferedReaderType.Reader);
    const RawFrameWriterType = RawFrameWriter(BufferedWriterType.Writer);

    /// Contains the state that is negotiated with a node as part of the handshake.
    const NegotiatedState = struct {
        cql_version: CQLVersion,
    };

    const PreparedStatementMetadataValue = struct {
        result_metadata_id: ?[]const u8,
        metadata: PreparedMetadata,
        rows_metadata: RowsMetadata,
    };

    /// Maps a prepared statement id to the types of the arguments needed when executing it.
    const PreparedStatementsMetadata = std.HashMap([]const u8, PreparedStatementMetadataValue, std.hash_map.hashString, std.hash_map.eqlString, std.hash_map.DefaultMaxLoadPercentage);

    allocator: *mem.Allocator,
    options: InitOptions,

    socket: std.fs.File,

    buffered_reader: BufferedReaderType,
    buffered_writer: BufferedWriterType,

    read_lock: if (std.io.is_async) std.event.Lock else void,
    write_lock: if (std.io.is_async) std.event.Lock else void,

    /// Helpers types needed to decode the CQL protocol.
    raw_frame_reader: RawFrameReaderType,
    raw_frame_writer: RawFrameWriterType,
    primitive_reader: PrimitiveReader,
    primitive_writer: PrimitiveWriter,

    /// TODO(vincent): need to implement some sort of TLL or size limit for this.
    prepared_statements_metadata: PreparedStatementsMetadata,

    // Negotiated with the server
    negotiated_state: NegotiatedState,

    pub fn initIp4(self: *Client, allocator: *mem.Allocator, seed_address: net.Address, options: InitOptions) !void {
        self.allocator = allocator;
        self.options = options;

        self.socket = try net.tcpConnectToAddress(seed_address);
        errdefer self.socket.close();

        self.buffered_reader = BufferedReaderType{ .unbuffered_reader = self.socket.reader() };
        self.buffered_writer = BufferedWriterType{ .unbuffered_writer = self.socket.writer() };

        if (std.io.is_async) {
            self.read_lock = std.event.Lock.init();
            self.write_lock = std.event.Lock.init();
        }

        self.raw_frame_reader = RawFrameReaderType.init(self.buffered_reader.reader());
        self.raw_frame_writer = RawFrameWriterType.init(self.buffered_writer.writer());
        self.primitive_reader = PrimitiveReader.init();

        self.prepared_statements_metadata = PreparedStatementsMetadata.init(allocator);

        var dummy_diags = InitOptions.Diagnostics{};
        var diags = options.diags orelse &dummy_diags;

        _ = try self.handshake(diags);
    }

    pub fn close(self: *Client) void {
        self.socket.close();
    }

    pub fn prepare(self: *Client, allocator: *mem.Allocator, options: QueryOptions, comptime query_string: []const u8, args: anytype) ![]const u8 {
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

        var option_ids = OptionIDArrayList{};
        try computeValues(allocator, null, &option_ids, args);

        // Write PREPARE, expect RESULT
        {
            var prepare_frame = PrepareFrame{
                .query = query_string,
                .keyspace = null,
            };

            try self.writeFrame(allocator, .{
                .opcode = .Prepare,
                .body = prepare_frame,
            });
        }

        var read_frame = try self.readFrame(allocator, ReadFrameOptions{
            .frame_allocator = self.allocator,
        });
        switch (read_frame) {
            .Result => |frame| switch (frame.result) {
                .Prepared => |prepared| {
                    const id = prepared.query_id;

                    // Store the metadata for later use with `execute`.

                    const gop = try self.prepared_statements_metadata.getOrPut(id);
                    if (gop.found_existing) {
                        if (gop.entry.value.result_metadata_id) |result_metadata_id| {
                            self.allocator.free(result_metadata_id);
                        }
                        gop.entry.value.metadata.deinit(self.allocator);
                        gop.entry.value.rows_metadata.deinit(self.allocator);
                    }

                    gop.entry.value = undefined;
                    gop.entry.value.result_metadata_id = prepared.result_metadata_id;
                    gop.entry.value.metadata = prepared.metadata;
                    gop.entry.value.rows_metadata = prepared.rows_metadata;

                    return id;
                },
                else => return error.InvalidServerResponse,
            },
            .Error => |err| {
                diags.message = err.message;
                return error.QueryPreparationFailed;
            },
            else => return error.InvalidServerResponse,
        }
    }

    // TODO(vincent): maybe add not comptime equivalent ?

    pub fn query(self: *Client, allocator: *mem.Allocator, options: QueryOptions, comptime query_string: []const u8, args: anytype) !?Iterator {
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
        // TODO(vincent): handle skip_metadata (see §4.1.4 in the spec)
        var query_parameters = QueryParameters{
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
        query_parameters.values = Values{ .Normal = values.toOwnedSlice() };

        // Write QUERY
        {
            var query_frame = QueryFrame{
                .query = query_string,
                .query_parameters = query_parameters,
            };
            try self.writeFrame(allocator, .{
                .opcode = .Query,
                .body = query_frame,
            });
        }

        // Read either RESULT or ERROR
        return switch (try self.readFrame(allocator, null)) {
            .Result => |frame| {
                return switch (frame.result) {
                    .Rows => |rows| blk: {
                        break :blk Iterator.init(rows.metadata, rows.data);
                    },
                    else => null,
                };
            },
            .Error => |err| {
                diags.message = err.message;
                return error.QueryExecutionFailed;
            },
            else => return error.InvalidServerResponse,
        };
    }

    pub fn execute(self: *Client, allocator: *mem.Allocator, options: QueryOptions, query_id: []const u8, args: anytype) !?Iterator {
        var dummy_diags = QueryOptions.Diagnostics{};
        var diags = options.diags orelse &dummy_diags;

        // If the metadata doesn't exist we can't proceed.
        const prepared_statement_metadata_kv = self.prepared_statements_metadata.get(query_id);
        if (prepared_statement_metadata_kv == null) {
            return error.InvalidPreparedQueryID;
        }

        const ps_result_metadata_id = prepared_statement_metadata_kv.?.result_metadata_id;
        const ps_metadata = prepared_statement_metadata_kv.?.metadata;
        const ps_rows_metadata = prepared_statement_metadata_kv.?.rows_metadata;

        var values = try std.ArrayList(Value).initCapacity(allocator, 16);
        var option_ids = OptionIDArrayList{};
        try computeValues(allocator, &values, &option_ids, args);

        // Now that we have both prepared and compute option IDs, check that they're compatible
        // If not compatible we produce a diagnostic.
        {
            const prepared = ps_metadata.column_specs;
            const computed = option_ids.span();

            if (prepared.len != computed.len) {
                diags.execute.not_enough_args = true;
                return error.InvalidPreparedStatementExecuteArgs;
            }

            for (prepared) |column_spec, i| {
                if (computed[i]) |option| {
                    if (column_spec.option != option) {
                        diags.execute.first_incompatible_arg = .{
                            .position = i,
                            .prepared = prepared[i],
                            .argument = computed[i],
                        };
                        return error.InvalidPreparedStatementExecuteArgs;
                    }
                }
            }
        }

        // Check that the values provided are compatible with the prepared statement

        // TODO(vincent): handle named values
        var query_parameters = QueryParameters{
            .consistency_level = self.options.consistency,
            .values = undefined,
            .skip_metadata = true,
            .page_size = options.page_size,
            .paging_state = options.paging_state,
            .serial_consistency_level = null,
            .timestamp = null,
            .keyspace = null,
            .now_in_seconds = null,
        };
        query_parameters.values = Values{ .Normal = values.toOwnedSlice() };

        // Write EXECUTE
        {
            var execute_frame = ExecuteFrame{
                .query_id = query_id,
                .result_metadata_id = ps_result_metadata_id,
                .query_parameters = query_parameters,
            };
            try self.writeFrame(allocator, .{
                .opcode = .Execute,
                .body = execute_frame,
            });
        }

        // Read either RESULT or ERROR
        return switch (try self.readFrame(allocator, null)) {
            .Result => |frame| {
                return switch (frame.result) {
                    .Rows => |rows| blk: {
                        const metadata = if (rows.metadata.column_specs.len > 0)
                            rows.metadata
                        else
                            ps_rows_metadata;

                        break :blk Iterator.init(metadata, rows.data);
                    },
                    else => null,
                };
            },
            .Error => |err| {
                diags.message = err.message;
                return error.QueryExecutionFailed;
            },
            else => return error.InvalidServerResponse,
        };
    }

    fn handshake(self: *Client, diags: *InitOptions.Diagnostics) !void {
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

    fn authenticate(self: *Client, allocator: *mem.Allocator, diags: *InitOptions.Diagnostics, authenticator: []const u8) !void {
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
            const token = try std.fmt.bufPrint(&buf, "\x00{}\x00{}", .{ self.options.username.?, self.options.password.? });
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
    fn writeFrame(self: *Client, allocator: *mem.Allocator, frame: anytype) !void {
        // Lock the writer if necessary
        var heldWriteLock: std.event.Lock.Held = undefined;
        if (std.io.is_async) {
            heldWriteLock = self.write_lock.acquire();
        }
        defer if (std.io.is_async) {
            heldWriteLock.release();
        };

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
                            if (!build_options.with_snappy) {
                                std.debug.panic("snappy compression is not available, make sure you built the client correctly");
                            }

                            const snappy = @import("snappy.zig");

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

    const ReadFrameOptions = struct {
        frame_allocator: *mem.Allocator,
    };

    fn readFrame(self: *Client, allocator: *mem.Allocator, options: ?ReadFrameOptions) !Frame {
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

    fn readRawFrame(self: *Client, allocator: *mem.Allocator) !RawFrame {
        var raw_frame = try self.raw_frame_reader.read(allocator);

        if (raw_frame.header.flags & FrameFlags.Compression == FrameFlags.Compression) {
            const decompressed_data = try lz4.decompress(allocator, raw_frame.body);
            raw_frame.body = decompressed_data;
        }

        return raw_frame;
    }

    fn makeRawFrame(self: *Client, allocator: *mem.Allocator, opcode: Opcode, force_no_compression: bool) !RawFrame {
        var raw_frame: RawFrame = undefined;

        const written = self.primitive_writer.getWritten();

        raw_frame.header = FrameHeader{
            .version = self.options.protocol_version,
            .flags = 0,
            .stream = 0,
            .opcode = opcode,
            .body_len = @intCast(u32, written.len),
        };

        if (force_no_compression) {
            raw_frame.body = written;
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
            } else written;
        }

        // TODO(vincent): implement streams
        // TODO(vincent): only compress if it's useful ?

        return raw_frame;
    }
};

test "client: insert then query" {
    if (!build_options.with_cassandra) return error.SkipZigTest;

    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var harness = try casstest.Harness.init(
        &arena.allocator,
        build_options.compression_algorithm,
        build_options.protocol_version,
    );
    defer harness.deinit();

    // Insert some data

    const nb_rows = 2;

    try harness.insertTestData(.AgeToIDs, nb_rows);
    try harness.insertTestData(.User, nb_rows);

    // Read and validate the data for the age_to_ids table

    {
        const Callback = struct {
            pub fn do(h: *casstest.Harness, i: usize, row: *casstest.Row.AgeToIDs) !bool {
                testing.expectEqual(row.age, 0);
                testing.expectEqualSlices(u8, &[_]u8{ 0, 2, 4, 8 }, row.ids);
                testing.expectEqualStrings("Vincent 0", row.name);
                testing.expect(h.positive_varint.toConst().eq(row.balance));
                return true;
            }
        };

        const res = try harness.selectAndScan(
            casstest.Row.AgeToIDs,
            "SELECT age, name, ids, balance FROM foobar.age_to_ids WHERE age = ?",
            .{
                @intCast(u32, 0),
            },
            Callback.do,
        );
        testing.expect(res);
    }

    {
        const Callback = struct {
            pub fn do(h: *casstest.Harness, i: usize, row: *casstest.Row.AgeToIDs) !bool {
                testing.expectEqual(@as(u32, 1), row.age);
                testing.expectEqualSlices(u8, &[_]u8{ 0, 2, 4, 8 }, row.ids);
                testing.expectEqualStrings("", row.name);
                testing.expect(h.negative_varint.toConst().eq(row.balance));
                return true;
            }
        };

        const res = try harness.selectAndScan(
            casstest.Row.AgeToIDs,
            "SELECT age, name, ids, balance FROM foobar.age_to_ids WHERE age = ?",
            .{
                @intCast(u32, 1),
            },
            Callback.do,
        );
        testing.expect(res);
    }

    // Read and validate the data for the user table

    {
        const Callback = struct {
            pub fn do(h: *casstest.Harness, i: usize, row: *casstest.Row.User) !bool {
                testing.expectEqual(@as(u64, 2000), row.id);
                testing.expectEqual(i + 25, row.secondary_id);
                return true;
            }
        };

        const res = try harness.selectAndScan(
            casstest.Row.User,
            "SELECT id, secondary_id FROM foobar.user WHERE id = 2000",
            .{},
            Callback.do,
        );
        testing.expect(res);
    }
}

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

const OptionIDArrayList = struct {
    const Self = @This();

    items: [128]?OptionID = undefined,
    pos: usize = 0,

    pub fn append(self: *Self, option_id: ?OptionID) !void {
        self.items[self.pos] = option_id;
        self.pos += 1;
    }

    pub fn span(self: *Self) []?OptionID {
        return self.items[0..self.pos];
    }
};

test "option id array list" {
    var option_ids = OptionIDArrayList{};

    try option_ids.append(.Tinyint);
    try option_ids.append(.Smallint);

    const items = option_ids.span();
    testing.expectEqual(@as(usize, 2), items.len);
    testing.expectEqual(OptionID.Tinyint, items[0].?);
    testing.expectEqual(OptionID.Smallint, items[1].?);
}

/// Compute a list of Value and OptionID for each field in the tuple or struct args.
/// It resolves the values recursively too.
///
/// TODO(vincent): it's not clear to the caller that data in `args` must outlive `values` because we don't duplicating memory
/// unless absolutely necessary in the case of arrays.
/// Think of a way to communicate that.
fn computeValues(allocator: *mem.Allocator, values: ?*std.ArrayList(Value), options: ?*OptionIDArrayList, args: anytype) !void {
    if (@typeInfo(@TypeOf(args)) != .Struct) {
        @compileError("Expected tuple or struct argument, found " ++ @typeName(args) ++ " of type " ++ @tagName(@typeInfo(args)));
    }

    var dummy_vals = try std.ArrayList(Value).initCapacity(allocator, 16);
    defer dummy_vals.deinit();
    var vals = values orelse &dummy_vals;

    var dummy_opts = OptionIDArrayList{};
    var opts = options orelse &dummy_opts;

    inline for (@typeInfo(@TypeOf(args)).Struct.fields) |struct_field, i| {
        const Type = struct_field.field_type;

        const arg = @field(args, struct_field.name);

        try computeSingleValue(allocator, vals, opts, Type, arg);
    }
}

fn resolveOption(comptime Type: type) OptionID {
    // Special case [16]u8 since we consider it a UUID.
    if (Type == [16]u8) return .UUID;

    // Special case []const u8 because it's used for strings.
    if (Type == []const u8) return .Varchar;

    // Special case big.int types because it's used for varint.
    if (Type == big.int.Mutable or Type == big.int.Const) return .Varint;

    const type_info = @typeInfo(Type);
    switch (type_info) {
        .Bool => return .Boolean,
        .Int => |info| switch (Type) {
            i8, u8 => return .Tinyint,
            i16, u16 => return .Smallint,
            i32, u32 => return .Int,
            i64, u64 => return .Bigint,
            else => @compileError("field type " ++ @typeName(Type) ++ " is not compatible with CQL"),
        },
        .Float => |info| switch (Type) {
            f32 => return .Float,
            f64 => return .Double,
            else => @compileError("field type " ++ @typeName(Type) ++ " is not compatible with CQL"),
        },
        .Pointer => |pointer| switch (pointer.size) {
            .One => {
                return resolveOption(pointer.child);
            },
            else => @compileError("invalid pointer size " ++ @tagName(pointer.size)),
        },
        .Optional => |optional| {
            return resolveOption(optional.child);
        },
        else => @compileError("field type " ++ @typeName(Type) ++ " not handled yet (type id: " ++ @tagName(type_info) ++ ")"),
    }
}

fn computeSingleValue(allocator: *mem.Allocator, values: *std.ArrayList(Value), options: *OptionIDArrayList, comptime Type: type, arg: Type) !void {
    const type_info = @typeInfo(Type);

    var value: Value = undefined;

    // Special case [16]u8 since we consider it a UUID.
    if (Type == [16]u8) {
        try options.append(.UUID);
        value = Value{ .Set = try mem.dupe(allocator, u8, &arg) };
        try values.append(value);

        return;
    }

    // Special case []const u8 because it's used for strings.
    if (Type == []const u8) {
        try options.append(.Varchar);
        // TODO(vincent): should we make a copy ?
        value = Value{ .Set = arg };
        try values.append(value);

        return;
    }

    // Special case big.int types because it's used for varint.
    if (Type == big.int.Const) {
        try options.append(.Varint);
        value = Value{ .Set = try bigint.toBytes(allocator, arg) };
        try values.append(value);

        return;
    }

    // The NotSet struct allows the caller to not set a value, which according to the
    // protocol will not result in any change to the existing value.
    if (Type == NotSet) {
        try options.append(resolveOption(arg.type));
        value = Value{ .NotSet = {} };
        try values.append(value);

        return;
    }

    switch (type_info) {
        .Bool => {
            var buf = try allocator.alloc(u8, 1);
            errdefer allocator.free(buf);

            buf[0] = if (arg) 0x01 else 0x00;

            try options.append(.Boolean);
            value = Value{ .Set = buf };
            try values.append(value);
        },
        .Int => |info| {
            try options.append(resolveOption(Type));

            var buf = try allocator.alloc(u8, info.bits / 8);
            errdefer allocator.free(buf);

            mem.writeIntBig(Type, @ptrCast(*[info.bits / 8]u8, buf), arg);

            value = Value{ .Set = buf };
            try values.append(value);
        },
        .Float => |info| {
            try options.append(resolveOption(Type));

            var buf = try allocator.alloc(u8, info.bits / 8);
            errdefer allocator.free(buf);

            @ptrCast(*align(1) Type, buf).* = arg;

            value = Value{ .Set = buf };
            try values.append(value);
        },
        .Pointer => |pointer| switch (pointer.size) {
            .One => {
                try computeValues(allocator, values, options, .{arg.*});
                return;
            },
            .Slice => {
                // Otherwise it's a list or a set, encode a new list of values.
                var inner_values = std.ArrayList(Value).init(allocator);
                for (arg) |item| {
                    try computeValues(allocator, &inner_values, null, .{item});
                }

                try options.append(null);
                value = Value{ .Set = try serializeValues(allocator, inner_values.toOwnedSlice()) };
                try values.append(value);
            },
            else => @compileError("invalid pointer size " ++ @tagName(pointer.size)),
        },
        .Array => |array| {

            // Otherwise it's a list or a set, encode a new list of values.
            var inner_values = std.ArrayList(Value).init(allocator);
            for (arg) |item| {
                try computeValues(allocator, &inner_values, null, .{item});
            }

            try options.append(null);
            value = Value{ .Set = try serializeValues(allocator, inner_values.toOwnedSlice()) };
            try values.append(value);
        },
        .Optional => |optional| {
            if (arg) |a| {
                try computeSingleValue(allocator, values, options, optional.child, a);
            } else {
                try options.append(resolveOption(optional.child));
                value = Value{ .Null = {} };
                try values.append(value);
            }
        },
        else => @compileError("field type " ++ @typeName(Type) ++ " not handled yet (type id: " ++ @tagName(type_info) ++ ")"),
    }
}

fn serializeValues(allocator: *mem.Allocator, values: []const Value) ![]const u8 {
    var pw: PrimitiveWriter = undefined;
    try pw.reset(allocator);

    try pw.writeInt(u32, @intCast(u32, values.len));

    for (values) |value| {
        switch (value) {
            .Set => |v| {
                try pw.writeBytes(v);
            },
            else => {},
        }
    }

    return pw.toOwnedSlice();
}

test "serialize values" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();

    var v1 = Value{ .Set = "foobar" };
    var v2 = Value{ .Set = "barbaz" };

    var data = try serializeValues(&arenaAllocator.allocator, &[_]Value{ v1, v2 });
    testing.expectEqual(@as(usize, 24), data.len);
    testing.expectEqualSlices(u8, "\x00\x00\x00\x02", data[0..4]);
    testing.expectEqualStrings("\x00\x00\x00\x06foobar\x00\x00\x00\x06barbaz", data[4..]);
}

test "compute values: ints" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = OptionIDArrayList{};

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
    testing.expectEqual(OptionID.Tinyint, o[0].?);
    testing.expectEqualSlices(u8, "\xff", v[1].Set);
    testing.expectEqual(OptionID.Tinyint, o[1].?);

    testing.expectEqualSlices(u8, "\x7f\xff", v[2].Set);
    testing.expectEqual(OptionID.Smallint, o[2].?);
    testing.expectEqualSlices(u8, "\xde\xdf", v[3].Set);
    testing.expectEqual(OptionID.Smallint, o[3].?);

    testing.expectEqualSlices(u8, "\x7f\xff\xff\xff", v[4].Set);
    testing.expectEqual(OptionID.Int, o[4].?);
    testing.expectEqualSlices(u8, "\xab\xab\xab\xaf", v[5].Set);
    testing.expectEqual(OptionID.Int, o[5].?);

    testing.expectEqualSlices(u8, "\x7f\xff\xff\xff\xff\xff\xff\xff", v[6].Set);
    testing.expectEqual(OptionID.Bigint, o[6].?);
    testing.expectEqualSlices(u8, "\xdc\xdc\xdc\xdc\xdc\xdc\xdc\xdf", v[7].Set);
    testing.expectEqual(OptionID.Bigint, o[7].?);

    testing.expectEqualSlices(u8, "\x00\x00\x00\x00\x00\x00\x4e\x20", v[8].Set);
    testing.expectEqual(OptionID.Bigint, o[8].?);
}

test "compute values: floats" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = OptionIDArrayList{};

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
    testing.expectEqual(OptionID.Float, o[0].?);
    testing.expectEqualSlices(u8, "\x46\xfd\x7d\x00\x08\xfb\x0d\x41", v[1].Set);
    testing.expectEqual(OptionID.Double, o[1].?);
    testing.expectEqualSlices(u8, "\xa4\x70\x3d\x0a\xd7\x23\x79\x40", v[2].Set);
    testing.expectEqual(OptionID.Double, o[2].?);
}

test "compute values: strings" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = OptionIDArrayList{};

    _ = try computeValues(allocator, &values, &options, .{
        .string = @as([]const u8, try mem.dupe(allocator, u8, "foobar")),
    });

    var v = values.span();
    var o = options.span();

    testing.expectEqual(@as(usize, 1), v.len);
    testing.expectEqual(@as(usize, 1), o.len);

    testing.expectEqualStrings("foobar", v[0].Set);
    testing.expectEqual(OptionID.Varchar, o[0].?);
}

test "compute values: bool" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = OptionIDArrayList{};

    _ = try computeValues(allocator, &values, &options, .{
        .bool1 = true,
        .bool2 = false,
    });

    var v = values.span();
    var o = options.span();

    testing.expectEqual(@as(usize, 2), v.len);
    testing.expectEqual(@as(usize, 2), o.len);

    testing.expectEqualSlices(u8, "\x01", v[0].Set);
    testing.expectEqual(OptionID.Boolean, o[0].?);
    testing.expectEqualSlices(u8, "\x00", v[1].Set);
    testing.expectEqual(OptionID.Boolean, o[1].?);
}

test "compute values: set/list" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = OptionIDArrayList{};

    _ = try computeValues(allocator, &values, &options, .{
        .string = &[_]u16{ 0x01, 0x2050 },
        .string2 = @as([]const u16, &[_]u16{ 0x01, 0x2050 }),
    });

    var v = values.span();
    var o = options.span();

    testing.expectEqual(@as(usize, 2), v.len);
    testing.expectEqual(@as(usize, 2), o.len);

    testing.expectEqualSlices(u8, "\x00\x00\x00\x02\x00\x00\x00\x02\x00\x01\x00\x00\x00\x02\x20\x50", v[0].Set);
    testing.expect(o[0] == null);

    testing.expectEqualSlices(u8, "\x00\x00\x00\x02\x00\x00\x00\x02\x00\x01\x00\x00\x00\x02\x20\x50", v[1].Set);
    testing.expect(o[1] == null);
}

test "compute values: uuid" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = OptionIDArrayList{};

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
    testing.expectEqual(OptionID.UUID, o[0].?);
}

test "compute values: not set and null" {
    var arenaAllocator = testing.arenaAllocator();
    defer arenaAllocator.deinit();
    var allocator = &arenaAllocator.allocator;

    var values = std.ArrayList(Value).init(allocator);
    var options = OptionIDArrayList{};

    const Args = struct {
        not_set: NotSet,
        nullable: ?u64,
    };

    _ = try computeValues(allocator, &values, &options, Args{
        .not_set = NotSet{ .type = i32 },
        .nullable = null,
    });

    var v = values.span();
    var o = options.span();

    testing.expectEqual(@as(usize, 2), v.len);
    testing.expectEqual(@as(usize, 2), o.len);

    testing.expect(v[0] == .NotSet);
    testing.expectEqual(OptionID.Int, o[0].?);

    testing.expect(v[1] == .Null);
    testing.expectEqual(OptionID.Bigint, o[1].?);
}

fn areOptionIDsEqual(prepared: []const ColumnSpec, computed: []const ?OptionID) bool {}

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

test "" {
    _ = @import("bigint.zig");

    if (build_options.with_snappy) {
        _ = @import("snappy.zig");
    }
    _ = @import("lz4.zig");
}
