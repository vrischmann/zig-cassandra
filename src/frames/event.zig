const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// EVENT is an event pushed by the server.
///
/// Described in the protocol spec at §4.2.6.
const EventFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    event: Event,

    pub fn deinit(self: *const Self) void {
        switch (self.event) {
            .TOPOLOGY_CHANGE, .STATUS_CHANGE => return,
            .SCHEMA_CHANGE => |ev| {
                ev.deinit();
            },
        }
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .event = undefined,
        };

        const event_type_string = try framer.readString();
        defer allocator.free(event_type_string);

        const event_type = meta.stringToEnum(EventType, event_type_string) orelse return error.InvalidEventType;

        switch (event_type) {
            .TOPOLOGY_CHANGE => {
                var change = TopologyChange{
                    .type = undefined,
                    .node_address = undefined,
                };

                const type_string = try framer.readString();
                defer allocator.free(type_string);

                change.type = meta.stringToEnum(TopologyChangeType, type_string) orelse return error.InvalidTopologyChangeType;
                change.node_address = try framer.readInet();

                frame.event = Event{ .TOPOLOGY_CHANGE = change };

                return frame;
            },
            .STATUS_CHANGE => {
                var change = StatusChange{
                    .type = undefined,
                    .node_address = undefined,
                };

                const type_string = try framer.readString();
                defer allocator.free(type_string);

                change.type = meta.stringToEnum(StatusChangeType, type_string) orelse return error.InvalidStatusChangeType;
                change.node_address = try framer.readInet();

                frame.event = Event{ .STATUS_CHANGE = change };

                return frame;
            },
            .SCHEMA_CHANGE => {
                var change = SchemaChange{
                    .type = undefined,
                    .target = undefined,
                    .options = undefined,
                };

                const type_string = try framer.readString();
                defer allocator.free(type_string);

                const target_string = try framer.readString();
                defer allocator.free(target_string);

                change.type = meta.stringToEnum(SchemaChangeType, type_string) orelse return error.InvalidSchemaChangeType;
                change.target = meta.stringToEnum(SchemaChangeTarget, target_string) orelse return error.InvalidSchemaChangeTarget;

                change.options = SchemaChangeOptions.init(allocator);

                switch (change.target) {
                    .KEYSPACE => {
                        change.options.keyspace = try framer.readString();
                    },
                    .TABLE, .TYPE => {
                        change.options.keyspace = try framer.readString();
                        change.options.object_name = try framer.readString();
                    },
                    .FUNCTION, .AGGREGATE => {
                        change.options.keyspace = try framer.readString();
                        change.options.object_name = try framer.readString();
                        change.options.arguments = (try framer.readStringList()).toOwnedSlice();
                    },
                }

                frame.event = Event{ .SCHEMA_CHANGE = change };

                return frame;
            },
        }
    }
};

test "event frame: topology change" {
    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x24\x00\x0f\x54\x4f\x50\x4f\x4c\x4f\x47\x59\x5f\x43\x48\x41\x4e\x47\x45\x00\x08\x4e\x45\x57\x5f\x4e\x4f\x44\x45\x04\x7f\x00\x00\x04\x00\x00\x23\x52";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Event, data.len, framer.header);

    const frame = try EventFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expect(frame.event == .TOPOLOGY_CHANGE);

    const topology_change = frame.event.TOPOLOGY_CHANGE;
    testing.expectEqual(TopologyChangeType.NEW_NODE, topology_change.type);

    const localhost = net.Address.initIp4([4]u8{ 0x7f, 0x00, 0x00, 0x04 }, 9042);
    testing.expect(net.Address.eql(localhost, topology_change.node_address));
}

test "event frame: status change" {
    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x1e\x00\x0d\x53\x54\x41\x54\x55\x53\x5f\x43\x48\x41\x4e\x47\x45\x00\x04\x44\x4f\x57\x4e\x04\x7f\x00\x00\x01\x00\x00\x23\x52";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Event, data.len, framer.header);

    const frame = try EventFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expect(frame.event == .STATUS_CHANGE);

    const status_change = frame.event.STATUS_CHANGE;
    testing.expectEqual(StatusChangeType.DOWN, status_change.type);

    const localhost = net.Address.initIp4([4]u8{ 0x7f, 0x00, 0x00, 0x01 }, 9042);
    testing.expect(net.Address.eql(localhost, status_change.node_address));
}

test "event frame: schema change/keyspace" {
    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x2a\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45\x00\x07\x43\x52\x45\x41\x54\x45\x44\x00\x08\x4b\x45\x59\x53\x50\x41\x43\x45\x00\x06\x62\x61\x72\x62\x61\x7a";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Event, data.len, framer.header);

    const frame = try EventFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expect(frame.event == .SCHEMA_CHANGE);

    const schema_change = frame.event.SCHEMA_CHANGE;
    testing.expectEqual(SchemaChangeType.CREATED, schema_change.type);
    testing.expectEqual(SchemaChangeTarget.KEYSPACE, schema_change.target);

    const options = schema_change.options;
    testing.expectEqualString("barbaz", options.keyspace);
    testing.expectEqualString("", options.object_name);
    testing.expect(options.arguments == null);
}

test "event frame: schema change/table" {
    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x2e\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45\x00\x07\x43\x52\x45\x41\x54\x45\x44\x00\x05\x54\x41\x42\x4c\x45\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x05\x73\x61\x6c\x75\x74";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Event, data.len, framer.header);

    const frame = try EventFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expect(frame.event == .SCHEMA_CHANGE);

    const schema_change = frame.event.SCHEMA_CHANGE;
    testing.expectEqual(SchemaChangeType.CREATED, schema_change.type);
    testing.expectEqual(SchemaChangeTarget.TABLE, schema_change.target);

    const options = schema_change.options;
    testing.expectEqualString("foobar", options.keyspace);
    testing.expectEqualString("salut", options.object_name);
    testing.expect(options.arguments == null);
}

test "event frame: schema change/function" {
    const data = "\x84\x00\xff\xff\x0c\x00\x00\x00\x40\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45\x00\x07\x43\x52\x45\x41\x54\x45\x44\x00\x08\x46\x55\x4e\x43\x54\x49\x4f\x4e\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x0d\x73\x6f\x6d\x65\x5f\x66\x75\x6e\x63\x74\x69\x6f\x6e\x00\x01\x00\x03\x69\x6e\x74";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Event, data.len, framer.header);

    const frame = try EventFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expect(frame.event == .SCHEMA_CHANGE);

    const schema_change = frame.event.SCHEMA_CHANGE;
    testing.expectEqual(SchemaChangeType.CREATED, schema_change.type);
    testing.expectEqual(SchemaChangeTarget.FUNCTION, schema_change.target);

    const options = schema_change.options;
    testing.expectEqualString("foobar", options.keyspace);
    testing.expectEqualString("some_function", options.object_name);
    const arguments = options.arguments.?;
    testing.expectEqual(@as(usize, 1), arguments.len);
    testing.expectEqualString("int", arguments[0]);
}