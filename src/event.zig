const std = @import("std");
const mem = std.mem;
const net = std.net;

const PrimitiveReader = @import("primitive/reader.zig").PrimitiveReader;
const PrimitiveWriter = @import("primitive/writer.zig").PrimitiveWriter;

const testing = @import("testing.zig");

pub const TopologyChangeType = enum {
    NEW_NODE,
    REMOVED_NODE,
};

pub const StatusChangeType = enum {
    UP,
    DOWN,
};

pub const SchemaChangeType = enum {
    CREATED,
    UPDATED,
    DROPPED,
};

pub const SchemaChangeTarget = enum {
    KEYSPACE,
    TABLE,
    TYPE,
    FUNCTION,
    AGGREGATE,
};

pub const SchemaChangeOptions = struct {
    keyspace: []const u8,
    object_name: []const u8,
    arguments: ?[]const []const u8,

    pub fn init() SchemaChangeOptions {
        return SchemaChangeOptions{
            .keyspace = &[_]u8{},
            .object_name = &[_]u8{},
            .arguments = null,
        };
    }
};

pub const TopologyChange = struct {
    type: TopologyChangeType,
    node_address: net.Address,
};

pub const StatusChange = struct {
    type: StatusChangeType,
    node_address: net.Address,
};

pub const SchemaChange = struct {
    const Self = @This();

    type: SchemaChangeType,
    target: SchemaChangeTarget,
    options: SchemaChangeOptions,

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !Self {
        var change = Self{
            .type = undefined,
            .target = undefined,
            .options = undefined,
        };

        change.type = std.meta.stringToEnum(SchemaChangeType, (try pr.readString(allocator))) orelse return error.InvalidSchemaChangeType;
        change.target = std.meta.stringToEnum(SchemaChangeTarget, (try pr.readString(allocator))) orelse return error.InvalidSchemaChangeTarget;

        change.options = SchemaChangeOptions.init();

        switch (change.target) {
            .KEYSPACE => {
                change.options.keyspace = try pr.readString(allocator);
            },
            .TABLE, .TYPE => {
                change.options.keyspace = try pr.readString(allocator);
                change.options.object_name = try pr.readString(allocator);
            },
            .FUNCTION, .AGGREGATE => {
                change.options.keyspace = try pr.readString(allocator);
                change.options.object_name = try pr.readString(allocator);
                change.options.arguments = try pr.readStringList(allocator);
            },
        }

        return change;
    }
};

pub const EventType = enum {
    TOPOLOGY_CHANGE,
    STATUS_CHANGE,
    SCHEMA_CHANGE,
};

pub const Event = union(EventType) {
    TOPOLOGY_CHANGE: TopologyChange,
    STATUS_CHANGE: StatusChange,
    SCHEMA_CHANGE: SchemaChange,
};

test "schema change options" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var options = SchemaChangeOptions.init();

    options.keyspace = try mem.dupe(&arena.allocator, u8, "foobar");
    options.object_name = try mem.dupe(&arena.allocator, u8, "barbaz");
    var arguments = try arena.allocator.alloc([]const u8, 4);
    var i: usize = 0;
    while (i < arguments.len) : (i += 1) {
        arguments[i] = try mem.dupe(&arena.allocator, u8, "hello");
    }
    options.arguments = arguments;
}
