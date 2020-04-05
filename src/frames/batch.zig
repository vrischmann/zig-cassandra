const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

const Framer = @import("../framer.zig").Framer;
const sm = @import("../string_map.zig");
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// Structure of a query in a BATCH frame
const BatchQuery = struct {
    const Self = @This();

    query_string: ?[]const u8,
    query_id: ?[]const u8,

    values: Values,

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !BatchQuery {
        var query = Self{
            .query_string = null,
            .query_id = null,
            .values = undefined,
        };

        const kind = try framer.readByte();
        switch (kind) {
            0 => query.query_string = try framer.readLongString(),
            1 => query.query_id = try framer.readShortBytes(),
            else => return error.InvalidQueryKind,
        }

        var list = std.ArrayList(Value).init(allocator);
        errdefer list.deinit();

        const n_values = try framer.readInt(u16);
        var j: usize = 0;
        while (j < @as(usize, n_values)) : (j += 1) {
            const value = try framer.readValue();
            _ = try list.append(value);
        }

        query.values = Values{ .Normal = list.toOwnedSlice() };

        return query;
    }
};

/// BATCH is sent to execute a list of queries (prepared or not) as a batch.
///
/// Described in the protocol spec at §4.1.7
const BatchFrame = struct {
    const Self = @This();

    batch_type: BatchType,
    queries: []BatchQuery,
    consistency_level: Consistency,
    serial_consistency_level: ?Consistency,
    timestamp: ?u64,
    keyspace: ?[]const u8,
    now_in_seconds: ?u32,

    const FlagWithSerialConsistency: u32 = 0x0010;
    const FlagWithDefaultTimestamp: u32 = 0x0020;
    const FlagWithNamedValues: u32 = 0x0040; // NOTE(vincent): the spec says this is broker so it's not implemented
    const FlagWithKeyspace: u32 = 0x0080;
    const FlagWithNowInSeconds: u32 = 0x100;

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .batch_type = undefined,
            .queries = undefined,
            .consistency_level = undefined,
            .serial_consistency_level = null,
            .timestamp = null,
            .keyspace = null,
            .now_in_seconds = null,
        };

        frame.batch_type = @intToEnum(BatchType, try framer.readByte());
        frame.queries = &[_]BatchQuery{};

        // Read all queries in the batch

        var queries = std.ArrayList(BatchQuery).init(allocator);
        errdefer queries.deinit();

        const n = try framer.readInt(u16);
        var i: usize = 0;
        while (i < @as(usize, n)) : (i += 1) {
            const query = try BatchQuery.read(allocator, FramerType, framer);
            _ = try queries.append(query);
        }

        frame.queries = queries.toOwnedSlice();

        // Read the rest of the frame

        frame.consistency_level = try framer.readConsistency();

        // The size of the flags bitmask depends on the protocol version.
        var flags: u32 = 0;
        if (framer.header.version == ProtocolVersion.V5) {
            flags = try framer.readInt(u32);
        } else {
            flags = try framer.readInt(u8);
        }

        if (flags & FlagWithSerialConsistency == FlagWithSerialConsistency) {
            const consistency_level = try framer.readConsistency();
            if (consistency_level != .Serial and consistency_level != .LocalSerial) {
                return error.InvalidSerialConsistency;
            }
            frame.serial_consistency_level = consistency_level;
        }
        if (flags & FlagWithDefaultTimestamp == FlagWithDefaultTimestamp) {
            const timestamp = try framer.readInt(u64);
            if (timestamp < 0) {
                return error.InvalidNegativeTimestamp;
            }
            frame.timestamp = timestamp;
        }

        if (framer.header.version != ProtocolVersion.V5) {
            return frame;
        }

        // The following flags are only valid with protocol v5
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            frame.keyspace = try framer.readString();
        }
        if (flags & FlagWithNowInSeconds == FlagWithNowInSeconds) {
            frame.now_in_seconds = try framer.readInt(u32);
        }

        return frame;
    }
};

test "batch frame: query type string" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x04\x00\x00\xc0\x0d\x00\x00\x00\xcc\x00\x00\x03\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(&arena.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Batch, data.len, framer.header);

    const frame = try BatchFrame.read(&arena.allocator, @TypeOf(framer), &framer);

    testing.expectEqual(BatchType.Logged, frame.batch_type);

    testing.expectEqual(@as(usize, 3), frame.queries.len);
    for (frame.queries) |query| {
        const exp = "INSERT INTO foobar.user(id, name) values(uuid(), 'vincent')";
        testing.expectEqualString(exp, query.query_string.?);
        testing.expect(query.query_id == null);
        testing.expect(query.values == .Normal);
        testing.expectEqual(@as(usize, 0), query.values.Normal.len);
    }

    testing.expectEqual(Consistency.Any, frame.consistency_level);
    testing.expect(frame.serial_consistency_level == null);
    testing.expect(frame.timestamp == null);
    testing.expect(frame.keyspace == null);
    testing.expect(frame.now_in_seconds == null);
}

test "batch frame: query type prepared" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x04\x00\x01\x00\x0d\x00\x00\x00\xa2\x00\x00\x03\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\x3a\x9a\xab\x41\x68\x24\x4a\xef\x9d\xf5\x72\xc7\x84\xab\xa2\x57\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\xed\x54\xb0\x6d\xcc\xb2\x43\x51\x96\x51\x74\x5e\xee\xae\xd2\xfe\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\x79\xdf\x8a\x28\x5a\x60\x47\x19\x9b\x42\x84\xea\x69\x10\x1a\xe6\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x00\x00\x00";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(&arena.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Batch, data.len, framer.header);

    const frame = try BatchFrame.read(&arena.allocator, @TypeOf(framer), &framer);

    testing.expectEqual(BatchType.Logged, frame.batch_type);

    const expUUIDs = &[_][]const u8{
        "\x3a\x9a\xab\x41\x68\x24\x4a\xef\x9d\xf5\x72\xc7\x84\xab\xa2\x57",
        "\xed\x54\xb0\x6d\xcc\xb2\x43\x51\x96\x51\x74\x5e\xee\xae\xd2\xfe",
        "\x79\xdf\x8a\x28\x5a\x60\x47\x19\x9b\x42\x84\xea\x69\x10\x1a\xe6",
    };

    testing.expectEqual(@as(usize, 3), frame.queries.len);
    var i: usize = 0;
    for (frame.queries) |query| {
        testing.expect(query.query_string == null);
        const exp_query_id = "\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65";
        testing.expectEqualSlices(u8, exp_query_id, query.query_id.?);

        testing.expect(query.values == .Normal);
        testing.expectEqual(@as(usize, 2), query.values.Normal.len);

        const value1 = query.values.Normal[0];
        testing.expectEqualSlices(u8, expUUIDs[i], value1.Set);

        const value2 = query.values.Normal[1];
        testing.expectEqualString("Vincent", value2.Set);

        i += 1;
    }

    testing.expectEqual(Consistency.Any, frame.consistency_level);
    testing.expect(frame.serial_consistency_level == null);
    testing.expect(frame.timestamp == null);
    testing.expect(frame.keyspace == null);
    testing.expect(frame.now_in_seconds == null);
}
