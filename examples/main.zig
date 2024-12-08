const std = @import("std");
const heap = std.heap;
const mem = std.mem;
const net = std.net;
const big = std.math.big;

const cassandra = @import("cassandra");
const casstest = @import("../casstest.zig");

const log = std.log.scoped(.main);

/// Runs a single SELECT reading all data from the age_to_ids table.
///
/// This function demonstrates multiple things:
///  * executing a query without preparation
///  * iterating over the result iterator
///  * using the paging state and page size
fn doQuery(allocator: mem.Allocator, client: *cassandra.Client) !void {
    // We want query diagonistics in case of failure.
    var diags = cassandra.Client.QueryOptions.Diagnostics{};
    errdefer {
        log.warn("diags: {}", .{diags});
    }

    var paging_state_buffer: [1024]u8 = undefined;
    var paging_state_allocator = std.heap.FixedBufferAllocator.init(&paging_state_buffer);

    // Read max 48 rows per query.
    var options = cassandra.Client.QueryOptions{
        .page_size = 48,
        .paging_state = null,
        .diags = &diags,
    };

    var total: usize = 0;

    {
        // Demonstrate how to USE a keyspace.
        // All following queries which don't provide the keyspace directly
        // will assume the keyspace is "foobar".
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        _ = try client.query(arena.allocator(), options, "USE foobar", .{});
    }

    // Execute queries as long as there's more data available.

    while (true) {
        // Use an arena per query.
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        var iter = (try client.query(
            arena.allocator(),
            options,
            "SELECT age, name, ids, balance FROM age_to_ids",
            .{},
        )).?;

        const count = try iterate(arena.allocator(), &iter);
        total += count;

        // If there's more data Caassandra will respond with a valid paging state.
        // If there's no paging state we know we're done.
        if (iter.metadata.paging_state) |paging_state| {
            paging_state_allocator.reset();
            options.paging_state = try paging_state_allocator.allocator().dupe(u8, paging_state);
        } else {
            break;
        }
    }

    log.info("read {} rows", .{total});
}

fn doPrepare(parent_allocator: mem.Allocator, client: *cassandra.Client, n: usize) ![]const u8 {
    var arena = std.heap.ArenaAllocator.init(parent_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    log.info("preparing {d} times", .{n});

    //

    var query_id: []const u8 = undefined;

    var i: usize = 0;
    while (i < n) : (i += 1) {
        // We want query diagonistics in case of failure.
        var diags = cassandra.Client.QueryOptions.Diagnostics{};
        const options = cassandra.Client.QueryOptions{
            .diags = &diags,
        };

        query_id = client.prepare(
            allocator,
            options,
            "SELECT ids, age, name FROM foobar.age_to_ids WHERE age in (?, ?)",
            .{
                .age1 = @as(u32, 0),
                .age2 = @as(u32, 0),
            },
        ) catch |err| switch (err) {
            error.QueryPreparationFailed => {
                std.debug.panic("query preparation failed, received cassandra error: {s}\n", .{diags.message});
            },
            else => return err,
        };

        log.info("prepared query id is {s}", .{std.fmt.fmtSliceHexLower(query_id)});
    }

    return query_id;
}

fn doExecute(allocator: mem.Allocator, client: *cassandra.Client, query_id: []const u8) !void {
    var result_arena = std.heap.ArenaAllocator.init(allocator);
    defer result_arena.deinit();
    const result_allocator = result_arena.allocator();

    // We want query diagonistics in case of failure.
    var diags = cassandra.Client.QueryOptions.Diagnostics{};
    const options = cassandra.Client.QueryOptions{
        .diags = &diags,
    };

    var iter = (try client.execute(
        result_allocator,
        options,
        query_id,
        .{
            .age1 = @as(u32, 120),
            .age2 = @as(u32, 124),
        },
    )).?;

    _ = try iterate(allocator, &iter);
}

fn doPrepareThenExec(allocator: mem.Allocator, client: *cassandra.Client, n: usize) !void {
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const query_id = try doPrepare(allocator, client, n);
        try doExecute(allocator, client, query_id);
    }
}

fn doPrepareOnceThenExec(allocator: mem.Allocator, client: *cassandra.Client, n: usize) !void {
    const query_id = try doPrepare(allocator, client, n);

    var i: usize = 0;
    while (i < n) : (i += 1) {
        try doExecute(allocator, client, query_id);
    }
}

fn doInsert(parent_allocator: mem.Allocator, client: *cassandra.Client, n: usize) !void {
    var arena = heap.ArenaAllocator.init(parent_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    log.info("inserting {d} times", .{n});

    // We want query diagonistics in case of failure.
    var diags = cassandra.Client.QueryOptions.Diagnostics{};
    const options = cassandra.Client.QueryOptions{
        .diags = &diags,
    };

    const query_id = client.prepare(
        allocator,
        options,
        "INSERT INTO foobar.age_to_ids(age, name, ids, balance) VALUES(?, ?, ?, ?)",
        casstest.Args.AgeToIDs{},
    ) catch |err| switch (err) {
        error.QueryPreparationFailed => {
            std.debug.panic("query preparation failed, received cassandra error: {s}\n", .{diags.message});
        },
        else => return err,
    };

    var positive_varint = try big.int.Managed.init(allocator);
    try positive_varint.setString(10, "40502020");
    var negative_varint = try big.int.Managed.init(allocator);
    try negative_varint.setString(10, "-350956306");

    var buffer: [16384]u8 = undefined;
    var fba = heap.FixedBufferAllocator.init(&buffer);

    var i: usize = 0;

    while (i < n) : (i += 1) {
        fba.reset();

        const args = casstest.Args.AgeToIDs{
            .age = @as(u32, @intCast(i)) * @as(u32, 10),
            .name = if (i % 2 == 0)
                @as([]const u8, try std.fmt.allocPrint(fba.allocator(), "Vincent {}", .{i}))
            else
                null,
            .ids = [_]u8{ 0, 2, 4, 8 },
            .balance = if (i % 2 == 0) positive_varint.toConst() else negative_varint.toConst(),
        };

        _ = client.execute(fba.allocator(), options, query_id, args) catch |err| switch (err) {
            error.QueryExecutionFailed => {
                log.warn("error message: {s}\n", .{diags.message});
            },
            error.InvalidPreparedStatementExecuteArgs => {
                log.warn("execute diags: ({s})\n", .{diags.execute});
            },
            else => |e| return e,
        };
    }
}

/// Iterate over every row in the iterator provided.
fn iterate(allocator: mem.Allocator, iter: *cassandra.Iterator) !usize {
    var row: casstest.Row.AgeToIDs = undefined;

    // Just for formatting here
    const IDs = struct {
        slice: []u8,

        pub fn format(self: @This(), comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
            try writer.writeByte('[');
            for (self.slice, 0..) |item, i| {
                if (i > 0) try writer.writeAll(", ");
                try std.fmt.format(writer, "{d}", .{item});
            }
            try writer.writeByte(']');
        }
    };

    var count: usize = 0;

    while (true) : (count += 1) {
        // Use a single arena per iteration.
        // This makes it easy to discard all memory allocated while scanning the current row.
        var row_arena = std.heap.ArenaAllocator.init(allocator);
        defer row_arena.deinit();

        // We want iteration diagnostics in case of failures.
        var iter_diags = cassandra.Iterator.ScanOptions.Diagnostics{};
        const iter_options = cassandra.Iterator.ScanOptions{
            .diags = &iter_diags,
        };

        const scanned = iter.scan(row_arena.allocator(), iter_options, &row) catch |err| switch (err) {
            error.IncompatibleMetadata => blk: {
                const im = iter_diags.incompatible_metadata;
                const it = im.incompatible_types;
                if (it.cql_type_name != null and it.native_type_name != null) {
                    std.debug.panic("metadata incompatible. CQL type {?s} can't be scanned into native type {?s}\n", .{
                        it.cql_type_name, it.native_type_name,
                    });
                } else {
                    std.debug.panic("metadata incompatible. columns in result: {d} fields in struct: {d}\n", .{
                        im.metadata_columns, im.struct_fields,
                    });
                }
                break :blk false;
            },
            else => return err,
        };
        if (!scanned) {
            break;
        }

        const ids = IDs{ .slice = row.ids };

        log.debug("age: {} id: {} name: {s} balance: {}", .{ row.age, ids, row.name, row.balance });
    }

    return count;
}

const usage =
    \\usage: cli <command> [options]
    \\
    \\Commands:
    \\
    \\    insert
    \\    query
    \\    prepare
    \\    prepare-then-exec <iterations>
    \\    prepare-once-then-exec <iterations>
    \\
;

fn parseArg(comptime T: type, arg: []const u8) !T {
    switch (@typeInfo(T)) {
        .int => return std.fmt.parseInt(T, arg, 10),
        .optional => |p| {
            if (arg.len == 0) return null;
            return try parseArg(p.child, arg);
        },
        .pointer => |p| {
            switch (p.size) {
                .Slice => {
                    if (p.child == u8) return arg;
                },
                else => @compileError("invalid type " ++ @typeName(T)),
            }
        },
        else => @compileError("invalid type " ++ @typeName(T)),
    }
}

fn findArg(comptime T: type, args: []const []const u8, key: []const u8, default: T) !T {
    for (args) |arg| {
        var it = mem.tokenizeScalar(u8, arg, 't');
        const k = it.next().?;
        const v = it.next() orelse return default;

        if (!mem.eql(u8, key, k)) {
            continue;
        }

        if (T == []const u8) {
            return v;
        }

        return parseArg(T, v) catch default;
    }
    return default;
}

pub const std_options: std.Options = .{
    .log_level = .debug,
    .log_scope_levels = &[_]std.log.ScopeLevel{
        .{ .scope = .connection, .level = .err },
    },
};

pub fn main() anyerror!void {
    var gpa = heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);

    const allocator = gpa.allocator();

    const stderr = std.io.getStdErr().writer();

    const all_args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, all_args);

    if (all_args.len <= 1) {
        try stderr.writeAll("expected command argument\n\n");
        try stderr.writeAll(usage);
        std.process.exit(1);
    }

    const cmd = all_args[1];
    const args = all_args[2..];

    //
    // Connect to the seed node
    //

    // Define the seed node we will connect to. We use localhost:9042.
    const address = net.Address.initIp4([_]u8{ 127, 0, 0, 1 }, 9042);

    // The struct InitOptions can be used to control some aspects of the CQL client,
    // such as the protocol version, if compression is enabled, etc.

    var init_options = cassandra.Connection.InitOptions{};
    // init_options.protocol_version = cassandra.ProtocolVersion{ .version = try findArg(u8, args, "protocol_version", 4) };
    init_options.protocol_version = try cassandra.ProtocolVersion.init(5);
    init_options.compression = blk: {
        const tmp = try findArg(?[]const u8, args, "compression", null);
        if (tmp == null) break :blk null;
        break :blk try cassandra.CompressionAlgorithm.fromString(tmp.?);
    };
    init_options.username = "cassandra";
    init_options.password = "cassandra";

    // Additionally a Diagnostics struct can be provided.
    // If initialization fails for some reason, this struct will be populated.
    var init_diags = cassandra.Connection.InitOptions.Diagnostics{};
    init_options.diags = &init_diags;

    var connection: cassandra.Connection = undefined;
    connection.initIp4(allocator, address, init_options) catch |err| switch (err) {
        error.NoUsername, error.NoPassword => {
            std.debug.panic("the server requires authentication, please set the username and password", .{});
        },
        error.AuthenticationFailed => {
            std.debug.panic("server authentication failed, error was: {s}", .{init_diags.message});
        },
        error.HandshakeFailed => {
            std.debug.panic("server handshake failed, error was: {s}", .{init_diags.message});
        },
        else => return err,
    };

    //
    // Connection established, create the client and do the thing.
    //

    var client = cassandra.Client.initWithConnection(allocator, &connection, .{});
    defer client.deinit();

    // Try to create the keyspace and table.
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        var options = cassandra.Client.QueryOptions{};
        var diags = cassandra.Client.QueryOptions.Diagnostics{};
        options.diags = &diags;

        inline for (casstest.DDL) |query| {
            _ = try client.query(arena.allocator(), options, query, .{});
        }
    }

    // Parse the command and run it.

    if (mem.eql(u8, cmd, "query")) {
        return doQuery(allocator, &client);
    } else if (mem.eql(u8, cmd, "prepare")) {
        const n = if (args.len >= 1)
            try std.fmt.parseInt(usize, args[0], 10)
        else
            1;

        _ = try doPrepare(allocator, &client, n);
    } else if (mem.eql(u8, cmd, "insert")) {
        const n = if (args.len >= 1)
            try std.fmt.parseInt(usize, args[0], 10)
        else
            1;

        return doInsert(allocator, &client, n);
    } else if (mem.eql(u8, cmd, "prepare-then-exec")) {
        if (args.len < 1) {
            try std.fmt.format(stderr, "Usage: {s} prepared-then-exec <iterations>\n", .{args[0]});
            std.process.exit(1);
        }

        const n = try std.fmt.parseInt(usize, args[0], 10);

        return doPrepareThenExec(allocator, &client, n);
    } else if (mem.eql(u8, cmd, "prepare-once-then-exec")) {
        if (args.len < 1) {
            try std.fmt.format(stderr, "Usage: {s} prepared-then-exec <iterations>\n", .{args[0]});
            std.process.exit(1);
        }

        const n = try std.fmt.parseInt(usize, args[0], 10);

        return doPrepareOnceThenExec(allocator, &client, n);
    } else {
        try stderr.writeAll("expected command argument\n\n");
        try stderr.writeAll(usage);
        std.process.exit(1);
    }
}
