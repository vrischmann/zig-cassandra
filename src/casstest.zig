const std = @import("std");
const big = std.math.big;
const mem = std.mem;
const testing = std.testing;

const message = @import("message.zig");
const CompressionAlgorithm = message.CompressionAlgorithm;
const ProtocolVersion = message.ProtocolVersion;

const Client = @import("client.zig").Client;
const Connection = @import("connection.zig").Connection;
const Iterator = @import("iterator.zig").Iterator;

// This files provides helpers to test the client with a real cassandra node.

pub const DDL = [_][]const u8{
    \\ CREATE KEYSPACE IF NOT EXISTS foobar WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    ,
    \\ CREATE TABLE IF NOT EXISTS foobar.age_to_ids(
    \\ 	age int,
    \\ 	name text,
    \\ 	ids set<tinyint>,
    \\ 	balance varint,
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
        name: ?[]const u8 = null,
        ids: [4]u8 = undefined,
        balance: ?big.int.Const = null,
    };

    pub const User = struct {
        id: u64 = 0,
        secondary_id: u32 = 0,
    };
};

// Define a Row struct with a 1:1 mapping with the fields selected.
pub const Row = struct {
    pub const AgeToIDs = struct {
        age: u32,
        name: []const u8,
        ids: []u8,
        balance: big.int.Const,
    };

    pub const User = struct {
        id: u64,
        secondary_id: u32,
    };
};

pub const Harness = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    positive_varint: big.int.Managed = undefined,
    negative_varint: big.int.Managed = undefined,

    connection: Connection,
    client: Client,

    pub fn init(self: *Self, allocator: *mem.Allocator, compression_algorithm: ?CompressionAlgorithm, protocol_version: ?ProtocolVersion) !void {
        self.allocator = allocator;
        self.connection = undefined;
        self.client = undefined;

        // Create the varints.

        self.positive_varint = try big.int.Managed.init(allocator);
        try self.positive_varint.setString(10, "3405245950896869895938539859386968968953285938539111111111111111111111111111111111111111122222222222222222222222222222222");
        self.negative_varint = try big.int.Managed.init(allocator);
        try self.negative_varint.setString(10, "-3405245950896869895938539859386968968953285938539111111111111111111111111111111111111111122222222222222222222222222222222");

        // Create the client.

        const address = std.net.Address.initIp4([_]u8{ 127, 0, 0, 1 }, 9042);

        var init_options = Connection.InitOptions{};
        init_options.protocol_version = protocol_version orelse ProtocolVersion{ .version = @as(u8, 4) };
        init_options.compression = compression_algorithm;
        init_options.username = "cassandra";
        init_options.password = "cassandra";
        var init_diags = Connection.InitOptions.Diagnostics{};
        init_options.diags = &init_diags;

        std.debug.print("protocol version provided: {} (using {}) compression algorithm: {}\n", .{ protocol_version, init_options.protocol_version, compression_algorithm });

        self.connection.initIp4(allocator, address, init_options) catch |err| switch (err) {
            error.HandshakeFailed => {
                std.debug.panic("unable to handhsake, error: {s}", .{init_diags.message});
            },
            else => return err,
        };

        self.client = Client.initWithConnection(allocator, &self.connection, .{});

        // Create the keyspace and tables if necessary.

        inline for (DDL) |query| {
            _ = try self.client.query(allocator, .{}, query, .{});
        }
        inline for (Truncate) |query| {
            _ = try self.client.query(allocator, .{}, query, .{});
        }
    }

    pub fn deinit(self: *Self) void {
        self.positive_varint.deinit();
        self.negative_varint.deinit();
        self.client.deinit();
        self.connection.close();
    }

    pub fn insertTestData(self: *Self, comptime table: Table, n: usize) !void {
        var buffer: [16384]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buffer);

        var options = Client.QueryOptions{};
        var diags = Client.QueryOptions.Diagnostics{};
        options.diags = &diags;

        switch (table) {
            .AgeToIDs => {
                const query_id = self.client.prepare(
                    self.allocator,
                    options,
                    "INSERT INTO foobar.age_to_ids(age, name, ids, balance) VALUES(?, ?, ?, ?)",
                    Args.AgeToIDs{},
                ) catch |err| switch (err) {
                    error.QueryPreparationFailed => {
                        std.debug.panic("query preparation failed, received cassandra error: {s}\n", .{diags.message});
                    },
                    else => return err,
                };

                var i: usize = 0;
                while (i < n) : (i += 1) {
                    fba.reset();

                    const name = try std.fmt.allocPrint(&fba.allocator, "Vincent {}", .{i});

                    var balance = if (i % 2 == 0) self.positive_varint else self.negative_varint;

                    _ = self.client.execute(
                        &fba.allocator,
                        options,
                        query_id,
                        Args.AgeToIDs{
                            .age = @intCast(i),
                            .ids = [_]u8{ 0, 2, 4, 8 },
                            .name = if (i % 2 == 0) @as([]const u8, name) else null,
                            .balance = balance.toConst(),
                        },
                    ) catch |err| switch (err) {
                        error.QueryExecutionFailed => {
                            std.debug.panic("query execution failed, received cassandra error: {s}\n", .{diags.message});
                        },
                        else => return err,
                    };
                }
            },

            .User => {
                const query_id = self.client.prepare(
                    self.allocator,
                    options,
                    "INSERT INTO foobar.user(id, secondary_id) VALUES(?, ?)",
                    Args.User{},
                ) catch |err| switch (err) {
                    error.QueryPreparationFailed => {
                        std.debug.panic("query preparation failed, received cassandra error: {s}\n", .{diags.message});
                    },
                    else => return err,
                };

                var i: usize = 0;
                while (i < n) : (i += 1) {
                    fba.reset();

                    _ = self.client.execute(
                        &fba.allocator,
                        options,
                        query_id,
                        Args.User{
                            .id = 2000,
                            .secondary_id = @intCast(i + 25),
                        },
                    ) catch |err| switch (err) {
                        error.QueryExecutionFailed => {
                            std.debug.panic("query preparation failed, received cassandra error: {s}\n", .{diags.message});
                        },
                        else => return err,
                    };
                }
            },
        }
    }

    pub fn selectAndScan(
        self: *Self,
        comptime RowType: type,
        comptime query: []const u8,
        args: anytype,
        callback: fn (harness: *Self, i: usize, row: *RowType) anyerror!bool,
    ) !bool {
        // Use an arena per query.
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        var diags = Client.QueryOptions.Diagnostics{};
        const options = Client.QueryOptions{
            .diags = &diags,
        };

        var iter = (self.client.query(&arena.allocator, options, query, args) catch |err| switch (err) {
            error.QueryExecutionFailed => {
                std.debug.panic("query preparation failed, received cassandra error: {s}\n", .{diags.message});
            },
            else => return err,
        }).?;

        var row: RowType = undefined;

        var i: usize = 0;
        while (true) : (i += 1) {
            // Use a single arena per iteration.
            var row_arena = std.heap.ArenaAllocator.init(&arena.allocator);
            defer row_arena.deinit();

            // We want iteration diagnostics in case of failures.
            var iter_diags = Iterator.ScanOptions.Diagnostics{};
            const scan_options = Iterator.ScanOptions{
                .diags = &iter_diags,
            };

            const scanned = iter.scan(&arena.allocator, scan_options, &row) catch |err| switch (err) {
                error.IncompatibleMetadata => blk: {
                    const im = iter_diags.incompatible_metadata;
                    const it = im.incompatible_types;
                    if (it.cql_type_name != null and it.native_type_name != null) {
                        std.debug.panic("metadata incompatible. CQL type {s} can't be scanned into native type {s}\n", .{
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

            const res = try callback(self, i, &row);
            if (!res) {
                return res;
            }
        }

        return true;
    }
};

pub const Table = enum {
    AgeToIDs,
    User,
};
