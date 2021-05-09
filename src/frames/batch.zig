const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

usingnamespace @import("../frame.zig");
usingnamespace @import("../primitive_types.zig");

const sm = @import("../string_map.zig");

const testing = @import("../testing.zig");

/// Structure of a query in a BATCH frame
const BatchQuery = struct {
    const Self = @This();

    query_string: ?[]const u8,
    query_id: ?[]const u8,

    values: Values,

    pub fn write(self: Self, pw: *PrimitiveWriter) !void {
        if (self.query_string) |query_string| {
            _ = try pw.writeByte(0);
            _ = try pw.writeLongString(query_string);
        } else if (self.query_id) |query_id| {
            _ = try pw.writeByte(1);
            _ = try pw.writeShortBytes(query_id);
        }

        switch (self.values) {
            .Normal => |normal_values| {
                _ = try pw.writeInt(u16, @intCast(u16, normal_values.len));
                for (normal_values) |value| {
                    _ = try pw.writeValue(value);
                }
            },
            .Named => |named_values| {
                _ = try pw.writeInt(u16, @intCast(u16, named_values.len));
                for (named_values) |v| {
                    _ = try pw.writeString(v.name);
                    _ = try pw.writeValue(v.value);
                }
            },
        }
    }

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !BatchQuery {
        var query = Self{
            .query_string = null,
            .query_id = null,
            .values = undefined,
        };

        const kind = try pr.readByte();
        switch (kind) {
            0 => query.query_string = try pr.readLongString(allocator),
            1 => query.query_id = try pr.readShortBytes(allocator),
            else => return error.InvalidQueryKind,
        }

        var list = std.ArrayList(Value).init(allocator);
        errdefer list.deinit();

        const n_values = try pr.readInt(u16);
        var j: usize = 0;
        while (j < @as(usize, n_values)) : (j += 1) {
            const value = try pr.readValue(allocator);
            _ = try list.append(value);
        }

        query.values = Values{ .Normal = list.toOwnedSlice() };

        return query;
    }
};

/// BATCH is sent to execute a list of queries (prepared or not) as a batch.
///
/// Described in the protocol spec at §4.1.7
pub const BatchFrame = struct {
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
    const FlagWithNamedValues: u32 = 0x0040; // NOTE(vincent): the spec says this is broken so it's not implemented
    const FlagWithKeyspace: u32 = 0x0080;
    const FlagWithNowInSeconds: u32 = 0x100;

    pub fn write(self: Self, protocol_version: ProtocolVersion, pw: *PrimitiveWriter) !void {
        _ = try pw.writeInt(u8, @intCast(u8, @enumToInt(self.batch_type)));

        // Write the queries

        _ = try pw.writeInt(u16, @intCast(u16, self.queries.len));
        for (self.queries) |query| {
            _ = try query.write(pw);
        }

        // Write the consistency

        _ = try pw.writeConsistency(self.consistency_level);

        // Build the flags value

        var flags: u32 = 0;
        if (self.serial_consistency_level != null) {
            flags |= FlagWithSerialConsistency;
        }
        if (self.timestamp != null) {
            flags |= FlagWithDefaultTimestamp;
        }
        if (protocol_version.is(5)) {
            if (self.keyspace != null) {
                flags |= FlagWithKeyspace;
            }
            if (self.now_in_seconds != null) {
                flags |= FlagWithNowInSeconds;
            }
        }

        if (protocol_version.is(5)) {
            _ = try pw.writeInt(u32, flags);
        } else {
            _ = try pw.writeInt(u8, @intCast(u8, flags));
        }

        // Write the remaining body
    }

    pub fn read(allocator: *mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        var frame = Self{
            .batch_type = undefined,
            .queries = undefined,
            .consistency_level = undefined,
            .serial_consistency_level = null,
            .timestamp = null,
            .keyspace = null,
            .now_in_seconds = null,
        };

        frame.batch_type = @intToEnum(BatchType, try pr.readByte());
        frame.queries = &[_]BatchQuery{};

        // Read all queries in the batch

        var queries = std.ArrayList(BatchQuery).init(allocator);
        errdefer queries.deinit();

        const n = try pr.readInt(u16);
        var i: usize = 0;
        while (i < @as(usize, n)) : (i += 1) {
            const query = try BatchQuery.read(allocator, pr);
            _ = try queries.append(query);
        }

        frame.queries = queries.toOwnedSlice();

        // Read the rest of the frame

        frame.consistency_level = try pr.readConsistency();

        // The size of the flags bitmask depends on the protocol version.
        var flags: u32 = 0;
        if (protocol_version.is(5)) {
            flags = try pr.readInt(u32);
        } else {
            flags = try pr.readInt(u8);
        }

        if (flags & FlagWithSerialConsistency == FlagWithSerialConsistency) {
            const consistency_level = try pr.readConsistency();
            if (consistency_level != .Serial and consistency_level != .LocalSerial) {
                return error.InvalidSerialConsistency;
            }
            frame.serial_consistency_level = consistency_level;
        }
        if (flags & FlagWithDefaultTimestamp == FlagWithDefaultTimestamp) {
            const timestamp = try pr.readInt(u64);
            if (timestamp < 0) {
                return error.InvalidNegativeTimestamp;
            }
            frame.timestamp = timestamp;
        }

        if (!protocol_version.is(5)) {
            return frame;
        }

        // The following flags are only valid with protocol v5
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            frame.keyspace = try pr.readString(allocator);
        }
        if (flags & FlagWithNowInSeconds == FlagWithNowInSeconds) {
            frame.now_in_seconds = try pr.readInt(u32);
        }

        return frame;
    }
};

test "batch frame: query type string" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\xc0\x0d\x00\x00\x00\xcc\x00\x00\x03\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00\x00\x3b\x49\x4e\x53\x45\x52\x54\x20\x49\x4e\x54\x4f\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x28\x69\x64\x2c\x20\x6e\x61\x6d\x65\x29\x20\x76\x61\x6c\x75\x65\x73\x28\x75\x75\x69\x64\x28\x29\x2c\x20\x27\x76\x69\x6e\x63\x65\x6e\x74\x27\x29\x00\x00\x00\x00\x00";
    const raw_frame = try testing.readRawFrame(&arena.allocator, exp);

    try checkHeader(Opcode.Batch, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try BatchFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    try testing.expectEqual(BatchType.Logged, frame.batch_type);

    try testing.expectEqual(@as(usize, 3), frame.queries.len);
    for (frame.queries) |query| {
        const exp_query = "INSERT INTO foobar.user(id, name) values(uuid(), 'vincent')";
        try testing.expectEqualStrings(exp_query, query.query_string.?);
        try testing.expect(query.query_id == null);
        try testing.expect(query.values == .Normal);
        try testing.expectEqual(@as(usize, 0), query.values.Normal.len);
    }

    try testing.expectEqual(Consistency.Any, frame.consistency_level);
    try testing.expect(frame.serial_consistency_level == null);
    try testing.expect(frame.timestamp == null);
    try testing.expect(frame.keyspace == null);
    try testing.expect(frame.now_in_seconds == null);

    // write

    try testing.expectSameRawFrame(frame, raw_frame.header, exp);
}

test "batch frame: query type prepared" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x01\x00\x0d\x00\x00\x00\xa2\x00\x00\x03\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\x3a\x9a\xab\x41\x68\x24\x4a\xef\x9d\xf5\x72\xc7\x84\xab\xa2\x57\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\xed\x54\xb0\x6d\xcc\xb2\x43\x51\x96\x51\x74\x5e\xee\xae\xd2\xfe\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x01\x00\x10\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65\x00\x02\x00\x00\x00\x10\x79\xdf\x8a\x28\x5a\x60\x47\x19\x9b\x42\x84\xea\x69\x10\x1a\xe6\x00\x00\x00\x07\x56\x69\x6e\x63\x65\x6e\x74\x00\x00\x00";
    const raw_frame = try testing.readRawFrame(&arena.allocator, exp);

    try checkHeader(Opcode.Batch, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try BatchFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    try testing.expectEqual(BatchType.Logged, frame.batch_type);

    const expUUIDs = &[_][]const u8{
        "\x3a\x9a\xab\x41\x68\x24\x4a\xef\x9d\xf5\x72\xc7\x84\xab\xa2\x57",
        "\xed\x54\xb0\x6d\xcc\xb2\x43\x51\x96\x51\x74\x5e\xee\xae\xd2\xfe",
        "\x79\xdf\x8a\x28\x5a\x60\x47\x19\x9b\x42\x84\xea\x69\x10\x1a\xe6",
    };

    try testing.expectEqual(@as(usize, 3), frame.queries.len);
    var i: usize = 0;
    for (frame.queries) |query| {
        try testing.expect(query.query_string == null);
        const exp_query_id = "\x88\xb7\xd6\x81\x8b\x2d\x8d\x97\xfc\x41\xc1\x34\x7b\x27\xde\x65";
        try testing.expectEqualSlices(u8, exp_query_id, query.query_id.?);

        try testing.expect(query.values == .Normal);
        try testing.expectEqual(@as(usize, 2), query.values.Normal.len);

        const value1 = query.values.Normal[0];
        try testing.expectEqualSlices(u8, expUUIDs[i], value1.Set);

        const value2 = query.values.Normal[1];
        try testing.expectEqualStrings("Vincent", value2.Set);

        i += 1;
    }

    try testing.expectEqual(Consistency.Any, frame.consistency_level);
    try testing.expect(frame.serial_consistency_level == null);
    try testing.expect(frame.timestamp == null);
    try testing.expect(frame.keyspace == null);
    try testing.expect(frame.now_in_seconds == null);

    // write

    try testing.expectSameRawFrame(frame, raw_frame.header, exp);
}
