const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
usingnamespace @import("../query_parameters.zig");
const testing = @import("../testing.zig");

/// QUERY is sent to perform a CQL query.
///
/// Described in the protocol spec at §4.1.4
const QueryFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    query: []const u8,
    query_parameters: QueryParameters,

    pub fn deinit(self: Self) void {
        self.allocator.free(self.query);
        self.query_parameters.deinit();
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .query = undefined,
            .query_parameters = undefined,
        };

        frame.query = try framer.readLongString();
        frame.query_parameters = try QueryParameters.read(allocator, FramerType, framer);

        return frame;
    }
};

test "query frame: no values, no paging state" {
    const data = "\x04\x00\x00\x08\x07\x00\x00\x00\x30\x00\x00\x00\x1b\x53\x45\x4c\x45\x43\x54\x20\x2a\x20\x46\x52\x4f\x4d\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x20\x3b\x00\x01\x34\x00\x00\x00\x64\x00\x08\x00\x05\xa2\x2c\xf0\x57\x3e\x3f";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Query, data.len, framer.header);

    const frame = try QueryFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqualString("SELECT * FROM foobar.user ;", frame.query);
    testing.expectEqual(Consistency.One, frame.query_parameters.consistency_level);
    testing.expect(frame.query_parameters.values == null);
    testing.expectEqual(@as(u32, 100), frame.query_parameters.page_size.?);
    testing.expect(frame.query_parameters.paging_state == null);
    testing.expectEqual(Consistency.Serial, frame.query_parameters.serial_consistency_level.?);
    testing.expectEqual(@as(u64, 1585688778063423), frame.query_parameters.timestamp.?);
    testing.expect(frame.query_parameters.keyspace == null);
    testing.expect(frame.query_parameters.now_in_seconds == null);
}
