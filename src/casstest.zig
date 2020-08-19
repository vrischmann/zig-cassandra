const std = @import("std");
const mem = std.mem;

usingnamespace @import("primitive_types.zig");
usingnamespace @import("client.zig");

// This files provides helpers to test the client with a real cassandra node.

pub const DDL = [_][]const u8{
    \\ CREATE KEYSPACE IF NOT EXISTS foobar WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
,
    \\ CREATE TABLE IF NOT EXISTS foobar.age_to_ids(
    \\ 	age int,
    \\ 	name text,
    \\ 	ids set<tinyint>,
    \\ 	PRIMARY KEY ((age))
    \\ );
,
    \\ CREATE TABLE IF NOT EXISTS foobar.user(
    \\ 	id bigint,
    \\ 	secondary_id int,
    \\ 	PRIMARY KEY ((id), secondary_id)
    \\ );
};

pub const Truncate = [_][]const u8{
    \\ TRUNCATE TABLE foobar.age_to_ids;
,
    \\ TRUNCATE TABLE foobar.user;
};

pub const Args = struct {
    pub const AgeToIDs = struct {
        age: u32 = 0,
        ids: [4]u8 = undefined,
        name: ?[]const u8 = null,
    };

    pub const User = struct {
        id: u64 = 0,
        secondary_id: u32 = 0,
    };
};

// Define a Row struct with a 1:1 mapping with the fields selected.
pub const Row = struct {
    pub const AgeToIDs = struct {
        ids: []u8,
        age: u32,
        name: []const u8,
    };

    pub const User = struct {
        id: u64,
        secondary_id: u32,
    };
};

pub fn initTestClient(allocator: *mem.Allocator, protocol_version: ProtocolVersion) !*Client {
    var address = std.net.Address.initIp4([_]u8{ 127, 0, 0, 1 }, 9042);

    var init_options = InitOptions{};
    init_options.protocol_version = protocol_version;
    init_options.compression = CompressionAlgorithm.LZ4;
    init_options.username = "cassandra";
    init_options.password = "cassandra";

    var init_diags = InitOptions.Diagnostics{};
    init_options.diags = &init_diags;

    var client = try allocator.create(Client);
    try client.initIp4(allocator, address, init_options);

    // Create the keyspace and tables if necessary.

    inline for (DDL) |query| {
        _ = try client.query(allocator, QueryOptions{}, query, .{});
    }
    inline for (Truncate) |query| {
        _ = try client.query(allocator, QueryOptions{}, query, .{});
    }

    return client;
}

pub const Table = enum {
    AgeToIDs,
    User,
};

pub fn insertTestData(allocator: *mem.Allocator, client: *Client, comptime table: Table, n: usize) !void {
    var buffer: [16384]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buffer);

    var options = QueryOptions{};

    switch (table) {
        .AgeToIDs => {
            const query_id = try client.prepare(
                allocator,
                options,
                "INSERT INTO foobar.age_to_ids(age, ids, name) VALUES(?, ?, ?)",
                Args.AgeToIDs{},
            );

            var i: usize = 0;
            while (i < n) : (i += 1) {
                fba.reset();

                _ = try client.execute(
                    &fba.allocator,
                    options,
                    query_id,
                    Args.AgeToIDs{
                        .age = @intCast(u32, i),
                        .ids = [_]u8{ 0, 2, 4, 8 },
                        .name = if (i % 2 == 0)
                            @as([]const u8, try std.fmt.allocPrint(&fba.allocator, "Vincent {}", .{i}))
                        else
                            null,
                    },
                );
            }
        },

        .User => {
            const query_id = try client.prepare(
                allocator,
                options,
                "INSERT INTO foobar.user(id, secondary_id) VALUES(?, ?)",
                Args.User{},
            );

            var i: usize = 0;
            while (i < n) : (i += 1) {
                fba.reset();

                _ = try client.execute(
                    &fba.allocator,
                    options,
                    query_id,
                    Args.User{
                        .id = 2000,
                        .secondary_id = @intCast(u32, i + 25),
                    },
                );
            }
        },
    }
}
