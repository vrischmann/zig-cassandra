const std = @import("std");
const net = std.net;

const cql = @import("lib.zig");

pub fn main() anyerror!void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = &arena.allocator;

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

    // Execute a query

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

    // Iterate over every row in the result.
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

        const scanned = iter.?.scan(&row_arena.allocator, iter_options, &row) catch |err| switch (err) {
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
