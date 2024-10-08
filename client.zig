const builtin = @import("builtin");
const build_options = @import("build_options");
const std = @import("std");
const big = std.math.big;
const heap = std.heap;
const io = std.io;
const mem = std.mem;
const net = std.net;
const testing = std.testing;

const Connection = @import("connection.zig").Connection;

const PreparedMetadata = @import("metadata.zig").PreparedMetadata;
const RowsMetadata = @import("metadata.zig").RowsMetadata;
const ColumnSpec = @import("metadata.zig").ColumnSpec;

const bigint = @import("bigint.zig");

const protocol = @import("protocol.zig");
const CompressionAlgorithm = protocol.CompressionAlgorithm;
const Consistency = protocol.Consistency;
const NotSet = protocol.NotSet;
const OptionID = protocol.OptionID;
const ProtocolVersion = protocol.ProtocolVersion;
const Value = protocol.Value;
const Values = protocol.Values;
const MessageWriter = protocol.MessageWriter;

const ExecuteMessage = protocol.ExecuteMessage;
const PrepareMessage = protocol.PrepareMessage;
const QueryMessage = protocol.QueryMessage;

const AlreadyExistsError = protocol.AlreadyExistsError;
const FunctionFailureError = protocol.FunctionFailureError;
const ReadError = protocol.ReadError;
const UnavailableReplicasError = protocol.UnavailableReplicasError;
const UnpreparedError = protocol.UnpreparedError;
const WriteError = protocol.WriteError;

const Iterator = @import("iterator.zig").Iterator;
const QueryParameters = @import("QueryParameters.zig");

const testutils = @import("testutils.zig");
const casstest = @import("casstest.zig");

const PreparedStatementMetadataValue = struct {
    result_metadata_id: ?[]const u8,
    metadata: PreparedMetadata,
    rows_metadata: RowsMetadata,

    fn deinit(self: *PreparedStatementMetadataValue, allocator: mem.Allocator) void {
        if (self.result_metadata_id) |result_metadata_id| {
            allocator.free(result_metadata_id);
        }
        self.metadata.deinit(allocator);
        self.rows_metadata.deinit(allocator);
    }
};

/// Maps a prepared statement id to the types of the arguments needed when executing it.
const PreparedStatementsMetadata = std.StringHashMap(PreparedStatementMetadataValue);

pub const Client = struct {
    const Self = @This();

    pub const InitOptions = struct {
        /// The default consistency to use for queries.
        consistency: Consistency = .One,
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
                    prepared: ColumnSpec,
                    argument: ?OptionID = null,
                };

                pub fn format(value: Execute, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
                    var buf: [1024]u8 = undefined;
                    var fbs = io.fixedBufferStream(&buf);
                    const fbw = fbs.writer();

                    if (value.not_enough_args) |v| {
                        if (v) {
                            try std.fmt.format(fbw, "not enough args, ", .{});
                        }
                    }
                    if (value.first_incompatible_arg) |v| {
                        try std.fmt.format(fbw, "first incompatible arg: {{position: {d}, prepared: (name: {s}, type: {?}), argument: {any}}}", .{
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

    allocator: mem.Allocator,
    connection: *Connection,
    options: InitOptions,

    /// TODO(vincent): need to implement some sort of TLL or size limit for this.
    prepared_statements_metadata: PreparedStatementsMetadata,

    pub fn initWithConnection(allocator: mem.Allocator, connection: *Connection, options: InitOptions) Self {
        var self: Self = undefined;
        self.allocator = allocator;
        self.connection = connection;
        self.options = options;

        self.prepared_statements_metadata = PreparedStatementsMetadata.init(self.allocator);

        return self;
    }

    pub fn deinit(self: *Self) void {
        {
            var iter = self.prepared_statements_metadata.iterator();
            while (iter.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                entry.value_ptr.deinit(self.allocator);
            }

            self.prepared_statements_metadata.deinit();
        }

        self.connection.deinit();
    }

    pub fn prepare(self: *Self, allocator: mem.Allocator, options: QueryOptions, comptime query_string: []const u8, args: anytype) ![]const u8 {
        var dummy_diags = QueryOptions.Diagnostics{};
        var diags = options.diags orelse &dummy_diags;

        // Check that the query makes sense for the arguments provided.

        comptime {
            const bind_markers = countBindMarkers(query_string);
            const fields = @typeInfo(@TypeOf(args)).@"struct".fields.len;

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
            try self.connection.writeMessage(
                allocator,
                .prepare,
                PrepareMessage{
                    .query = query_string,
                    .keyspace = null,
                },
                .{},
            );
        }

        const read_message = try self.connection.nextMessage(allocator, .{
            .message_allocator = self.allocator,
        });
        switch (read_message) {
            .result => |result_message| switch (result_message.result) {
                .Prepared => |prepared| {
                    // Store the metadata for later use with `execute`.

                    const gop = try self.prepared_statements_metadata.getOrPut(prepared.query_id);
                    if (gop.found_existing) {
                        self.allocator.free(prepared.query_id);
                        gop.value_ptr.deinit(self.allocator);
                    }

                    gop.value_ptr.* = .{
                        .result_metadata_id = prepared.result_metadata_id,
                        .metadata = prepared.metadata,
                        .rows_metadata = prepared.rows_metadata,
                    };

                    return gop.key_ptr.*;
                },
                else => return error.InvalidServerResponse,
            },
            .@"error" => |err| {
                diags.message = err.message;
                return error.QueryPreparationFailed;
            },
            else => return error.InvalidServerResponse,
        }
    }

    // TODO(vincent): maybe add not comptime equivalent ?

    pub fn query(self: *Self, allocator: mem.Allocator, options: QueryOptions, comptime query_string: []const u8, args: anytype) !?Iterator {
        var dummy_diags = QueryOptions.Diagnostics{};
        var diags = options.diags orelse &dummy_diags;

        // Check that the query makes sense for the arguments provided.

        comptime {
            const bind_markers = countBindMarkers(query_string);
            const fields = @typeInfo(@TypeOf(args)).@"struct".fields.len;

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
        query_parameters.values = Values{ .Normal = try values.toOwnedSlice() };

        // Write QUERY
        {
            try self.connection.writeMessage(
                allocator,
                .query,
                QueryMessage{
                    .query = query_string,
                    .query_parameters = query_parameters,
                },
                .{},
            );
        }

        // Read either RESULT or ERROR
        return switch (try self.connection.nextMessage(allocator, .{})) {
            .result => |result_message| {
                return switch (result_message.result) {
                    .Rows => |rows| blk: {
                        break :blk Iterator.init(rows.metadata, rows.data);
                    },
                    else => null,
                };
            },
            .@"error" => |err| {
                diags.message = err.message;
                return error.QueryExecutionFailed;
            },
            else => return error.InvalidServerResponse,
        };
    }

    pub fn execute(self: *Self, allocator: mem.Allocator, options: QueryOptions, query_id: []const u8, args: anytype) !?Iterator {
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
            const computed = option_ids.getItems();

            if (prepared.len != computed.len) {
                diags.execute.not_enough_args = true;
                return error.InvalidPreparedStatementExecuteArgs;
            }

            for (prepared, 0..) |column_spec, i| {
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
        query_parameters.values = Values{ .Normal = try values.toOwnedSlice() };

        // Write EXECUTE
        {
            try self.connection.writeMessage(
                allocator,
                .execute,
                ExecuteMessage{
                    .query_id = query_id,
                    .result_metadata_id = ps_result_metadata_id,
                    .query_parameters = query_parameters,
                },
                .{},
            );
        }

        // Read either RESULT or ERROR
        return switch (try self.connection.nextMessage(allocator, .{})) {
            .result => |result_message| {
                return switch (result_message.result) {
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
            .@"error" => |err| {
                diags.message = err.message;
                return error.QueryExecutionFailed;
            },
            else => return error.InvalidServerResponse,
        };
    }
};

fn testWithCassandra(harness: *casstest.Harness) !void {
    // Insert some data
    const nb_rows = 2;

    try harness.insertTestData(.AgeToIDs, nb_rows);
    try harness.insertTestData(.User, nb_rows);

    // Read and validate the data for the age_to_ids table

    {
        const Callback = struct {
            pub fn do(h: *casstest.Harness, _: usize, row: *casstest.Row.AgeToIDs) !bool {
                try testing.expectEqual(row.age, 0);
                try testing.expectEqualSlices(u8, &[_]u8{ 0, 2, 4, 8 }, row.ids);
                try testing.expectEqualStrings("Vincent 0", row.name);
                try testing.expect(h.positive_varint.toConst().eq(row.balance));
                return true;
            }
        };

        const res = try harness.selectAndScan(
            casstest.Row.AgeToIDs,
            "SELECT age, name, ids, balance FROM foobar.age_to_ids WHERE age = ?",
            .{
                @as(u32, @intCast(0)),
            },
            Callback.do,
        );
        try testing.expect(res);
    }

    {
        const Callback = struct {
            pub fn do(h: *casstest.Harness, _: usize, row: *casstest.Row.AgeToIDs) !bool {
                try testing.expectEqual(@as(u32, 1), row.age);
                try testing.expectEqualSlices(u8, &[_]u8{ 0, 2, 4, 8 }, row.ids);
                try testing.expectEqualStrings("", row.name);
                try testing.expect(h.negative_varint.toConst().eq(row.balance));
                return true;
            }
        };

        const res = try harness.selectAndScan(
            casstest.Row.AgeToIDs,
            "SELECT age, name, ids, balance FROM foobar.age_to_ids WHERE age = ?",
            .{
                @as(u32, @intCast(1)),
            },
            Callback.do,
        );
        try testing.expect(res);
    }

    // Read and validate the data for the user table

    {
        const Callback = struct {
            pub fn do(_: *casstest.Harness, i: usize, row: *casstest.Row.User) !bool {
                try testing.expectEqual(@as(u64, 2000), row.id);
                try testing.expectEqual(i + 25, row.secondary_id);
                return true;
            }
        };

        const res = try harness.selectAndScan(
            casstest.Row.User,
            "SELECT id, secondary_id FROM foobar.user WHERE id = 2000",
            .{},
            Callback.do,
        );
        try testing.expect(res);
    }
}

test "client: insert then query" {
    if (!build_options.with_cassandra) return error.SkipZigTest;

    const testParameters = struct {
        const Self = @This();

        compression: ?CompressionAlgorithm,
        protocol_version: ?ProtocolVersion,

        pub fn init(s: []const u8) !Self {
            var self: Self = undefined;

            var it = mem.tokenize(s, ",");
            while (true) {
                const token = it.next() orelse break;

                var it2 = mem.tokenize(token, ":");
                const key = it2.next() orelse continue;
                const value = it2.next() orelse std.debug.panic("invalid token {s}\n", .{token});

                if (mem.eql(u8, "compression", key)) {
                    self.compression = try CompressionAlgorithm.fromString(value);
                } else if (mem.eql(u8, "protocol", key)) {
                    self.protocol_version = try ProtocolVersion.fromString(value);
                }
            }

            return self;
        }
    };

    const params = try testParameters.init(build_options.with_cassandra.?);

    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var harness: casstest.Harness = undefined;
    try harness.init(
        &arena.allocator,
        params.compression,
        params.protocol_version,
    );
    defer harness.deinit();

    try testWithCassandra(&harness);
}

const OptionIDArrayList = struct {
    const Self = @This();

    items: [128]?OptionID = undefined,
    pos: usize = 0,

    pub fn append(self: *Self, option_id: ?OptionID) !void {
        self.items[self.pos] = option_id;
        self.pos += 1;
    }

    pub fn getItems(self: *Self) []?OptionID {
        return self.items[0..self.pos];
    }
};

test "option id array list" {
    var option_ids = OptionIDArrayList{};

    try option_ids.append(.Tinyint);
    try option_ids.append(.Smallint);

    const items = option_ids.getItems();
    try testing.expectEqual(@as(usize, 2), items.len);
    try testing.expectEqual(OptionID.Tinyint, items[0].?);
    try testing.expectEqual(OptionID.Smallint, items[1].?);
}

/// Compute a list of Value and OptionID for each field in the tuple or struct args.
/// It resolves the values recursively too.
///
/// TODO(vincent): it's not clear to the caller that data in `args` must outlive `values` because we don't duplicating memory
/// unless absolutely necessary in the case of arrays.
/// Think of a way to communicate that.
fn computeValues(allocator: mem.Allocator, values: ?*std.ArrayList(Value), options: ?*OptionIDArrayList, args: anytype) !void {
    if (@typeInfo(@TypeOf(args)) != .@"struct") {
        @compileError("Expected tuple or struct argument, found " ++ @typeName(args) ++ " of type " ++ @tagName(@typeInfo(args)));
    }

    var dummy_vals = try std.ArrayList(Value).initCapacity(allocator, 16);
    defer dummy_vals.deinit();
    const vals = values orelse &dummy_vals;

    var dummy_opts = OptionIDArrayList{};
    const opts = options orelse &dummy_opts;

    inline for (@typeInfo(@TypeOf(args)).@"struct".fields) |struct_field| {
        const Type = struct_field.type;

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
        .bool => return .Boolean,
        .int => |_| switch (Type) {
            i8, u8 => return .Tinyint,
            i16, u16 => return .Smallint,
            i32, u32 => return .Int,
            i64, u64 => return .Bigint,
            else => @compileError("field type " ++ @typeName(Type) ++ " is not compatible with CQL"),
        },
        .float => |_| switch (Type) {
            f32 => return .Float,
            f64 => return .Double,
            else => @compileError("field type " ++ @typeName(Type) ++ " is not compatible with CQL"),
        },
        .pointer => |pointer| switch (pointer.size) {
            .One => {
                return resolveOption(pointer.child);
            },
            else => @compileError("invalid pointer size " ++ @tagName(pointer.size)),
        },
        .optional => |optional| {
            return resolveOption(optional.child);
        },
        else => @compileError("field type " ++ @typeName(Type) ++ " not handled yet (type id: " ++ @tagName(type_info) ++ ")"),
    }
}

fn computeSingleValue(allocator: mem.Allocator, values: *std.ArrayList(Value), options: *OptionIDArrayList, comptime Type: type, arg: Type) !void {
    const type_info = @typeInfo(Type);

    var value: Value = undefined;

    // Special case [16]u8 since we consider it a UUID.
    if (Type == [16]u8) {
        try options.append(.UUID);
        value = Value{ .Set = try allocator.dupe(u8, &arg) };
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
        .bool => {
            var buf = try allocator.alloc(u8, 1);
            errdefer allocator.free(buf);

            buf[0] = if (arg) 0x01 else 0x00;

            try options.append(.Boolean);
            value = Value{ .Set = buf };
            try values.append(value);
        },
        .int => |info| {
            try options.append(resolveOption(Type));

            const buf = try allocator.alloc(u8, info.bits / 8);
            errdefer allocator.free(buf);

            mem.writeInt(Type, @ptrCast(buf), arg, .big);

            value = Value{ .Set = buf };
            try values.append(value);
        },
        .float => |info| {
            try options.append(resolveOption(Type));

            const buf = try allocator.alloc(u8, info.bits / 8);
            errdefer allocator.free(buf);

            const arg_ptr: *align(1) Type = @ptrCast(buf);
            arg_ptr.* = arg;

            value = Value{ .Set = buf };
            try values.append(value);
        },
        .pointer => |pointer| switch (pointer.size) {
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
                value = Value{ .Set = try serializeValues(allocator, try inner_values.toOwnedSlice()) };
                try values.append(value);
            },
            else => @compileError("invalid pointer size " ++ @tagName(pointer.size)),
        },
        .array => |_| {

            // Otherwise it's a list or a set, encode a new list of values.
            var inner_values = std.ArrayList(Value).init(allocator);
            for (arg) |item| {
                try computeValues(allocator, &inner_values, null, .{item});
            }

            try options.append(null);
            value = Value{ .Set = try serializeValues(allocator, try inner_values.toOwnedSlice()) };
            try values.append(value);
        },
        .optional => |optional| {
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

fn serializeValues(allocator: mem.Allocator, values: []const Value) ![]const u8 {
    var mw = try MessageWriter.init(allocator);

    try mw.writeInt(u32, @intCast(values.len));

    for (values) |value| {
        switch (value) {
            .Set => |v| {
                try mw.writeBytes(v);
            },
            else => {},
        }
    }

    return mw.toOwnedSlice();
}

test "serialize values" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    const v1 = Value{ .Set = "foobar" };
    const v2 = Value{ .Set = "barbaz" };

    var data = try serializeValues(arena.allocator(), &[_]Value{ v1, v2 });
    try testing.expectEqual(@as(usize, 24), data.len);
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x02", data[0..4]);
    try testing.expectEqualStrings("\x00\x00\x00\x06foobar\x00\x00\x00\x06barbaz", data[4..]);
}

test "compute values: ints" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

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

    const v = values.items;
    const o = options.getItems();

    try testing.expectEqual(@as(usize, 9), v.len);
    try testing.expectEqual(@as(usize, 9), o.len);

    try testing.expectEqualSlices(u8, "\x7f", v[0].Set);
    try testing.expectEqual(OptionID.Tinyint, o[0].?);
    try testing.expectEqualSlices(u8, "\xff", v[1].Set);
    try testing.expectEqual(OptionID.Tinyint, o[1].?);

    try testing.expectEqualSlices(u8, "\x7f\xff", v[2].Set);
    try testing.expectEqual(OptionID.Smallint, o[2].?);
    try testing.expectEqualSlices(u8, "\xde\xdf", v[3].Set);
    try testing.expectEqual(OptionID.Smallint, o[3].?);

    try testing.expectEqualSlices(u8, "\x7f\xff\xff\xff", v[4].Set);
    try testing.expectEqual(OptionID.Int, o[4].?);
    try testing.expectEqualSlices(u8, "\xab\xab\xab\xaf", v[5].Set);
    try testing.expectEqual(OptionID.Int, o[5].?);

    try testing.expectEqualSlices(u8, "\x7f\xff\xff\xff\xff\xff\xff\xff", v[6].Set);
    try testing.expectEqual(OptionID.Bigint, o[6].?);
    try testing.expectEqualSlices(u8, "\xdc\xdc\xdc\xdc\xdc\xdc\xdc\xdf", v[7].Set);
    try testing.expectEqual(OptionID.Bigint, o[7].?);

    try testing.expectEqualSlices(u8, "\x00\x00\x00\x00\x00\x00\x4e\x20", v[8].Set);
    try testing.expectEqual(OptionID.Bigint, o[8].?);
}

test "compute values: floats" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

    var values = std.ArrayList(Value).init(allocator);
    var options = OptionIDArrayList{};

    const my_f64 = @as(f64, 402.240);

    _ = try computeValues(allocator, &values, &options, .{
        .f32 = @as(f32, 0.002),
        .f64 = @as(f64, 245601.000240305603),
        .f64_ptr = &my_f64,
    });

    const v = values.items;
    const o = options.getItems();

    try testing.expectEqual(@as(usize, 3), v.len);
    try testing.expectEqual(@as(usize, 3), o.len);

    try testing.expectEqualSlices(u8, "\x6f\x12\x03\x3b", v[0].Set);
    try testing.expectEqual(OptionID.Float, o[0].?);
    try testing.expectEqualSlices(u8, "\x46\xfd\x7d\x00\x08\xfb\x0d\x41", v[1].Set);
    try testing.expectEqual(OptionID.Double, o[1].?);
    try testing.expectEqualSlices(u8, "\xa4\x70\x3d\x0a\xd7\x23\x79\x40", v[2].Set);
    try testing.expectEqual(OptionID.Double, o[2].?);
}

test "compute values: strings" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

    var values = std.ArrayList(Value).init(allocator);
    var options = OptionIDArrayList{};

    _ = try computeValues(allocator, &values, &options, .{
        .string = @as([]const u8, try allocator.dupe(u8, "foobar")),
    });

    const v = values.items;
    const o = options.getItems();

    try testing.expectEqual(@as(usize, 1), v.len);
    try testing.expectEqual(@as(usize, 1), o.len);

    try testing.expectEqualStrings("foobar", v[0].Set);
    try testing.expectEqual(OptionID.Varchar, o[0].?);
}

test "compute values: bool" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

    var values = std.ArrayList(Value).init(allocator);
    var options = OptionIDArrayList{};

    _ = try computeValues(allocator, &values, &options, .{
        .bool1 = true,
        .bool2 = false,
    });

    const v = values.items;
    const o = options.getItems();

    try testing.expectEqual(@as(usize, 2), v.len);
    try testing.expectEqual(@as(usize, 2), o.len);

    try testing.expectEqualSlices(u8, "\x01", v[0].Set);
    try testing.expectEqual(OptionID.Boolean, o[0].?);
    try testing.expectEqualSlices(u8, "\x00", v[1].Set);
    try testing.expectEqual(OptionID.Boolean, o[1].?);
}

test "compute values: set/list" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

    var values = std.ArrayList(Value).init(allocator);
    var options = OptionIDArrayList{};

    _ = try computeValues(allocator, &values, &options, .{
        .string = &[_]u16{ 0x01, 0x2050 },
        .string2 = @as([]const u16, &[_]u16{ 0x01, 0x2050 }),
    });

    const v = values.items;
    const o = options.getItems();

    try testing.expectEqual(@as(usize, 2), v.len);
    try testing.expectEqual(@as(usize, 2), o.len);

    try testing.expectEqualSlices(u8, "\x00\x00\x00\x02\x00\x00\x00\x02\x00\x01\x00\x00\x00\x02\x20\x50", v[0].Set);
    try testing.expect(o[0] == null);

    try testing.expectEqualSlices(u8, "\x00\x00\x00\x02\x00\x00\x00\x02\x00\x01\x00\x00\x00\x02\x20\x50", v[1].Set);
    try testing.expect(o[1] == null);
}

test "compute values: uuid" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

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

    const v = values.items;
    const o = options.getItems();

    try testing.expectEqual(@as(usize, 1), v.len);
    try testing.expectEqual(@as(usize, 1), o.len);

    try testing.expectEqualSlices(u8, "\x55\x94\xd5\xb1\xef\x84\x41\xc4\xb2\x4e\x68\x48\x8d\xcf\xa1\xc9", v[0].Set);
    try testing.expectEqual(OptionID.UUID, o[0].?);
}

test "compute values: not set and null" {
    var arena = testutils.arenaAllocator();
    defer arena.deinit();
    const allocator = arena.allocator();

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

    const v = values.items;
    const o = options.getItems();

    try testing.expectEqual(@as(usize, 2), v.len);
    try testing.expectEqual(@as(usize, 2), o.len);

    try testing.expect(v[0] == .NotSet);
    try testing.expectEqual(OptionID.Int, o[0].?);

    try testing.expect(v[1] == .Null);
    try testing.expectEqual(OptionID.Bigint, o[1].?);
}

fn areOptionIDsEqual(_: []const ColumnSpec, _: []const ?OptionID) bool {}

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
    try testing.expectEqual(@as(usize, 3), count);
}

test {
    _ = @import("bigint.zig");
}
