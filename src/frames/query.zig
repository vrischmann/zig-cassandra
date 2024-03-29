const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

usingnamespace @import("../frame.zig");
usingnamespace @import("../primitive_types.zig");
usingnamespace @import("../query_parameters.zig");
const testing = @import("../testing.zig");

/// QUERY is sent to perform a CQL query.
///
/// Described in the protocol spec at §4.1.4
pub const QueryFrame = struct {
    const Self = @This();

    query: []const u8,
    query_parameters: QueryParameters,

    pub fn write(self: Self, protocol_version: ProtocolVersion, pw: *PrimitiveWriter) !void {
        _ = try pw.writeLongString(self.query);
        _ = try self.query_parameters.write(protocol_version, pw);
    }

    pub fn read(allocator: *mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        return Self{
            .query = try pr.readLongString(allocator),
            .query_parameters = try QueryParameters.read(allocator, protocol_version, pr),
        };
    }
};

test "query frame: no values, no paging state" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\x08\x07\x00\x00\x00\x30\x00\x00\x00\x1b\x53\x45\x4c\x45\x43\x54\x20\x2a\x20\x46\x52\x4f\x4d\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x20\x3b\x00\x01\x34\x00\x00\x00\x64\x00\x08\x00\x05\xa2\x2c\xf0\x57\x3e\x3f";
    const raw_frame = try testing.readRawFrame(&arena.allocator, exp);

    try checkHeader(Opcode.Query, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try QueryFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    try testing.expectEqualStrings("SELECT * FROM foobar.user ;", frame.query);
    try testing.expectEqual(Consistency.One, frame.query_parameters.consistency_level);
    try testing.expect(frame.query_parameters.values == null);
    try testing.expectEqual(@as(u32, 100), frame.query_parameters.page_size.?);
    try testing.expect(frame.query_parameters.paging_state == null);
    try testing.expectEqual(Consistency.Serial, frame.query_parameters.serial_consistency_level.?);
    try testing.expectEqual(@as(u64, 1585688778063423), frame.query_parameters.timestamp.?);
    try testing.expect(frame.query_parameters.keyspace == null);
    try testing.expect(frame.query_parameters.now_in_seconds == null);

    // write

    try testing.expectSameRawFrame(frame, raw_frame.header, exp);
}
