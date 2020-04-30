const std = @import("std");
const mem = std.mem;
const net = std.net;

const cql = @import("lib.zig");

fn doQuery(allocator: *mem.Allocator, client: *cql.Client) !void {
    var result_arena = std.heap.ArenaAllocator.init(allocator);
    defer result_arena.deinit();
    const result_allocator = &result_arena.allocator;

    // We want query diagonistics in case of failure.
    var diags = cql.Client.QueryOptions.Diagnostics{};
    var options = cql.Client.QueryOptions{
        .diags = &diags,
    };

    var iter = client.cquery(
        result_allocator,
        options,
        "SELECT ids, age, name FROM foobar.age_to_ids WHERE age in (?, ?)",
        .{
            .age1 = @as(u32, 120),
            .age2 = @as(u32, 124),
        },
    ) catch |err| switch (err) {
        error.QueryExecutionFailed => {
            std.debug.panic("query execution failed, received cassandra error: {}\n", .{diags.message});
        },
        else => return err,
    };

    try iterate(allocator, &(iter.?));
}

fn doPrepare(allocator: *mem.Allocator, client: *cql.Client) ![]const u8 {
    // We want query diagonistics in case of failure.
    var diags = cql.Client.QueryOptions.Diagnostics{};
    var options = cql.Client.QueryOptions{
        .diags = &diags,
    };

    const query_id = client.cprepare(
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

fn doExecute(allocator: *mem.Allocator, client: *cql.Client, query_id: []const u8) !void {
    var result_arena = std.heap.ArenaAllocator.init(allocator);
    defer result_arena.deinit();
    const result_allocator = &result_arena.allocator;

    // We want query diagonistics in case of failure.
    var diags = cql.Client.QueryOptions.Diagnostics{};
    var options = cql.Client.QueryOptions{
        .diags = &diags,
    };

    var iter = client.execute(
        result_allocator,
        options,
        query_id,
        .{
            .age1 = @as(u32, 120),
            .age2 = @as(u32, 124),
        },
    ) catch |err| switch (err) {
        error.QueryExecutionFailed => {
            std.debug.panic("query execution failed, received cassandra error: {}\n", .{diags.message});
        },
        else => return err,
    };

    try iterate(allocator, &(iter.?));
}

fn doPrepareThenExec(allocator: *mem.Allocator, client: *cql.Client, n: usize) !void {
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const query_id = try doPrepare(allocator, client);
        try doExecute(allocator, client, query_id);
    }
}

/// Iterate over every row in the iterator provided.
fn iterate(allocator: *mem.Allocator, iter: *cql.Iterator) !void {
    // Define a Row struct with a 1:1 mapping with the fields selected.
    const Row = struct {
        ids: []u8,
        age: u32,
        name: []const u8,
    };
    var row: Row = undefined;

    while (true) {
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

        std.debug.warn("age: {} id: {x} name: {} {x}\n", .{ row.age, row.ids, row.name, row.name });
    }
}

const usage =
    \\usage: cli <command> [options]
    \\
    \\Commands:
    \\
    \\    query
    \\    prepare
    \\    execute [query id]
    \\
;

pub fn main() anyerror!void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = &arena.allocator;

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

    var init_options = cql.Client.InitOptions{};
    init_options.seed_address = address;
    init_options.compression = cql.CompressionAlgorithm.LZ4;
    init_options.username = "cassandra";
    init_options.password = "cassandra";

    var init_diags = cql.Client.InitOptions.Diagnostics{};
    init_options.diags = &init_diags;

    var client: cql.Client = undefined;
    client.init(allocator, init_options) catch |err| switch (err) {
        error.ConnectionRefused => {
            std.debug.panic("connection refused to {}\n", .{address});
        },
        error.NoUsername, error.NoPassword => {
            std.debug.panic("the server requires authentication, please set the username and password", .{});
        },
        error.AuthenticationFailed => {
            std.debug.panic("server authentication failed, error was: {}", .{init_diags.message});
        },
        else => return err,
    };

    const cmd = args[1];
    if (mem.eql(u8, cmd, "query")) {
        return doQuery(allocator, &client);
    } else if (mem.eql(u8, cmd, "prepare")) {
        _ = try doPrepare(allocator, &client);
    } else if (mem.eql(u8, cmd, "prepare-then-exec")) {
        if (args.len < 3) {
            try std.fmt.format(stderr, "Usage: {} prepared-then-exec <iterations>\n", .{args[0]});
            std.process.exit(1);
        }

        const n = try std.fmt.parseInt(usize, args[2], 10);

        return doPrepareThenExec(allocator, &client, n);
    } else {
        try stderr.writeAll("expected command argument\n\n");
        try stderr.writeAll(usage);
        std.process.exit(1);
    }
}
