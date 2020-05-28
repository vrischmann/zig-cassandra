const std = @import("std");
const heap = std.heap;
const mem = std.mem;
const net = std.net;

const cql = @import("lib.zig");

fn doQuery(allocator: *mem.Allocator, client: *cql.TCPClient) !void {
    // We want query diagonistics in case of failure.
    var diags = cql.QueryOptions.Diagnostics{};
    errdefer {
        std.debug.warn("diags: {}\n", .{diags});
    }

    var paging_state_buffer: [1024]u8 = undefined;
    var paging_state_allocator = std.heap.FixedBufferAllocator.init(&paging_state_buffer);

    var options = cql.QueryOptions{
        .page_size = 48,
        .paging_state = null,
        .diags = &diags,
    };

    var total: usize = 0;
    var has_more = true;

    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        _ = try client.query(&arena.allocator, options, "USE foobar", .{});
    }

    while (has_more) {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        var iter = (try client.query(
            &arena.allocator,
            options,
            "SELECT ids, age, name FROM age_to_ids",
            .{},
        )).?;

        const count = try iterate(&arena.allocator, &iter);
        total += count;

        if (iter.metadata.paging_state) |paging_state| {
            paging_state_allocator.reset();
            options.paging_state = try mem.dupe(&paging_state_allocator.allocator, u8, paging_state);
        } else {
            break;
        }
    }

    std.debug.warn("read {} rows\n", .{total});
}

fn doPrepare(allocator: *mem.Allocator, client: *cql.TCPClient) ![]const u8 {
    // We want query diagonistics in case of failure.
    var diags = cql.QueryOptions.Diagnostics{};
    var options = cql.QueryOptions{
        .diags = &diags,
    };

    const query_id = client.prepare(
        allocator,
        options,
        "SELECT ids, age, name FROM foobar.age_to_ids WHERE age in (?, ?)",
        .{
            .age1 = @as(u32, 0),
            .age2 = @as(u32, 0),
        },
    ) catch |err| switch (err) {
        error.QueryPreparationFailed => {
            std.debug.panic("query preparation failed, received cassandra error: {}\n", .{diags.message});
        },
        else => return err,
    };

    std.debug.warn("prepared query id is {x}\n", .{query_id});

    return query_id;
}

fn doExecute(allocator: *mem.Allocator, client: *cql.TCPClient, query_id: []const u8) !void {
    var result_arena = std.heap.ArenaAllocator.init(allocator);
    defer result_arena.deinit();
    const result_allocator = &result_arena.allocator;

    // We want query diagonistics in case of failure.
    var diags = cql.QueryOptions.Diagnostics{};
    var options = cql.QueryOptions{
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

fn doPrepareThenExec(allocator: *mem.Allocator, client: *cql.TCPClient, n: usize) !void {
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const query_id = try doPrepare(allocator, client);
        try doExecute(allocator, client, query_id);
    }
}

fn doPrepareOnceThenExec(allocator: *mem.Allocator, client: *cql.TCPClient, n: usize) !void {
    const query_id = try doPrepare(allocator, client);

    var i: usize = 0;
    while (i < n) : (i += 1) {
        try doExecute(allocator, client, query_id);
    }
}

fn doInsert(allocator: *mem.Allocator, client: *cql.TCPClient, n: usize) !void {
    // We want query diagonistics in case of failure.
    var diags = cql.QueryOptions.Diagnostics{};
    var options = cql.QueryOptions{
        .diags = &diags,
    };

    const Args = struct {
        age: u32 = 0,
        ids: [4]u8 = undefined,
        name: ?[]const u8 = null,
    };
    var empty_args = Args{};

    const query_id = try client.prepare(
        allocator,
        options,
        "INSERT INTO foobar.age_to_ids(age, ids, name) VALUES(?, ?, ?)",
        empty_args,
    );

    var buffer: [16384]u8 = undefined;
    var fba = heap.FixedBufferAllocator.init(&buffer);

    var i: usize = 0;

    while (i < n) : (i += 1) {
        fba.reset();

        const args = Args{
            .age = @intCast(u32, i) * @as(u32, 10),
            .ids = [_]u8{ 0, 2, 4, 8 },
            .name = if (i % 2 == 0) @as([]const u8, try std.fmt.allocPrint(&fba.allocator, "Vincent {}", .{i})) else null,
        };

        _ = client.execute(
            &fba.allocator,
            options,
            query_id,
            args,
        ) catch |err| switch (err) {
            error.QueryExecutionFailed => {
                std.debug.warn("error message: {}\n", .{diags.message});
            },
            else => |e| return e,
        };
    }
}

/// Iterate over every row in the iterator provided.
fn iterate(allocator: *mem.Allocator, iter: *cql.Iterator) !usize {
    // Define a Row struct with a 1:1 mapping with the fields selected.
    const Row = struct {
        ids: []u8,
        age: u32,
        name: []const u8,
    };
    var row: Row = undefined;

    // Just for formatting here
    const IDs = struct {
        slice: []u8,

        pub fn format(self: @This(), comptime fmt: []const u8, options: std.fmt.FormatOptions, out_stream: var) !void {
            try out_stream.writeByte('[');
            for (self.slice) |item, i| {
                if (i > 0) try out_stream.writeAll(", ");
                try std.fmt.format(out_stream, "{d}", .{item});
            }
            try out_stream.writeByte(']');
        }
    };

    var count: usize = 0;

    while (true) : (count += 1) {
        // Use a single arena per iteration.
        // This makes it easy to discard all memory allocated while scanning the current row.
        var row_arena = std.heap.ArenaAllocator.init(allocator);
        defer row_arena.deinit();

        // We want iteration diagnostics in case of failures.
        var iter_diags = cql.Iterator.ScanOptions.Diagnostics{};
        var iter_options = cql.Iterator.ScanOptions{
            .diags = &iter_diags,
        };

        const scanned = iter.scan(&row_arena.allocator, iter_options, &row) catch |err| switch (err) {
            error.IncompatibleMetadata => blk: {
                const im = iter_diags.incompatible_metadata;
                const it = im.incompatible_types;
                if (it.cql_type_name != null and it.native_type_name != null) {
                    std.debug.panic("metadata incompatible. CQL type {} can't be scanned into native type {}\n", .{
                        it.cql_type_name, it.native_type_name,
                    });
                } else {
                    std.debug.panic("metadata incompatible. columns in result: {} fields in struct: {}\n", .{
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

        std.debug.warn("age: {} id: {} name: {} {x}\n", .{ row.age, ids, row.name, row.name });
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

pub fn main() anyerror!void {
    const allocator = std.heap.page_allocator;

    const stderr = std.io.getStdErr().outStream();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len <= 1) {
        try stderr.writeAll("expected command argument\n\n");
        try stderr.writeAll(usage);
        std.process.exit(1);
    }

    var address = net.Address.initIp4([_]u8{ 127, 0, 0, 1 }, 9042);

    // Connect to the seed node

    var init_options = cql.InitOptions{};
    // init_options.compression = cql.CompressionAlgorithm.LZ4;
    init_options.username = "cassandra";
    init_options.password = "cassandra";

    var init_diags = cql.InitOptions.Diagnostics{};
    init_options.diags = &init_diags;

    var client: cql.TCPClient = undefined;

    client.init(
        allocator,
        address,
        init_options,
    ) catch |err| switch (err) {
        error.NoUsername, error.NoPassword => {
            std.debug.panic("the server requires authentication, please set the username and password", .{});
        },
        error.AuthenticationFailed => {
            std.debug.panic("server authentication failed, error was: {}", .{init_diags.message});
        },
        error.HandshakeFailed => {
            std.debug.panic("server handshake failed, error was: {}", .{init_diags.message});
        },
        else => return err,
    };

    const cmd = args[1];
    if (mem.eql(u8, cmd, "query")) {
        return doQuery(allocator, &client);
    } else if (mem.eql(u8, cmd, "prepare")) {
        _ = try doPrepare(allocator, &client);
    } else if (mem.eql(u8, cmd, "insert")) {
        if (args.len < 3) {
            try std.fmt.format(stderr, "Usage: {} insert <iterations>\n", .{args[0]});
            std.process.exit(1);
        }

        const n = try std.fmt.parseInt(usize, args[2], 10);

        return doInsert(allocator, &client, n);
    } else if (mem.eql(u8, cmd, "prepare-then-exec")) {
        if (args.len < 3) {
            try std.fmt.format(stderr, "Usage: {} prepared-then-exec <iterations>\n", .{args[0]});
            std.process.exit(1);
        }

        const n = try std.fmt.parseInt(usize, args[2], 10);

        return doPrepareThenExec(allocator, &client, n);
    } else if (mem.eql(u8, cmd, "prepare-once-then-exec")) {
        if (args.len < 3) {
            try std.fmt.format(stderr, "Usage: {} prepared-then-exec <iterations>\n", .{args[0]});
            std.process.exit(1);
        }

        const n = try std.fmt.parseInt(usize, args[2], 10);

        return doPrepareOnceThenExec(allocator, &client, n);
    } else {
        try stderr.writeAll("expected command argument\n\n");
        try stderr.writeAll(usage);
        std.process.exit(1);
    }
}
