const std = @import("std");
const mem = std.mem;
const testing = std.testing;
const net = std.net;

const message = @import("message.zig");
const MessageReader = message.MessageReader;
const PrimitiveWriter = message.PrimitiveWriter;

const testutils = @import("testutils.zig");

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

    pub fn read(allocator: mem.Allocator, mr: *MessageReader) !Self {
        var change = Self{
            .type = undefined,
            .target = undefined,
            .options = undefined,
        };

        change.type = std.meta.stringToEnum(SchemaChangeType, (try mr.readString(allocator))) orelse return error.InvalidSchemaChangeType;
        change.target = std.meta.stringToEnum(SchemaChangeTarget, (try mr.readString(allocator))) orelse return error.InvalidSchemaChangeTarget;

        change.options = SchemaChangeOptions.init();

        switch (change.target) {
            .KEYSPACE => {
                change.options.keyspace = try mr.readString(allocator);
            },
            .TABLE, .TYPE => {
                change.options.keyspace = try mr.readString(allocator);
                change.options.object_name = try mr.readString(allocator);
            },
            .FUNCTION, .AGGREGATE => {
                change.options.keyspace = try mr.readString(allocator);
                change.options.object_name = try mr.readString(allocator);
                change.options.arguments = try mr.readStringList(allocator);
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
    var arena = testutils.arenaAllocator();
    defer arena.deinit();

    var options = SchemaChangeOptions.init();

    options.keyspace = try arena.allocator().dupe(u8, "foobar");
    options.object_name = try arena.allocator().dupe(u8, "barbaz");
    var arguments = try arena.allocator().alloc([]const u8, 4);
    var i: usize = 0;
    while (i < arguments.len) : (i += 1) {
        arguments[i] = try arena.allocator().dupe(u8, "hello");
    }
    options.arguments = arguments;
}
