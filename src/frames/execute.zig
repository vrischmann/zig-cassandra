const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
usingnamespace @import("../query_parameters.zig");
const testing = @import("../testing.zig");

/// EXECUTE is sent to execute a prepared query.
///
/// Described in the protocol spec at §4.1.6
const ExecuteFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    query_id: []const u8,
    result_metadata_id: ?[]const u8,
    query_parameters: QueryParameters,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.query_id);
        if (self.result_metadata_id) |id| {
            self.allocator.free(id);
        }
        self.query_parameters.deinit();
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .query_id = undefined,
            .result_metadata_id = null,
            .query_parameters = undefined,
        };

        frame.query_id = (try framer.readShortBytes()) orelse &[_]u8{};
        if (framer.header.version == ProtocolVersion.V5) {
            frame.result_metadata_id = try framer.readShortBytes();
        }
        frame.query_parameters = try QueryParameters.read(allocator, FramerType, framer);

        return frame;
    }
};

test "execute frame" {
    const data = "\x04\x00\x01\x00\x0a\x00\x00\x00\x37\x00\x10\x97\x97\x95\x6d\xfe\xb2\x4c\x99\x86\x8e\xd3\x84\xff\x6f\xd9\x4c\x00\x04\x27\x00\x01\x00\x00\x00\x10\xeb\x11\xc9\x1e\xd8\xcc\x48\x4d\xaf\x55\xe9\x9f\x5c\xd9\xec\x4a\x00\x00\x13\x88\x00\x05\xa2\x41\x4c\x1b\x06\x4c";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Execute, data.len, framer.header);

    const frame = try ExecuteFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    const exp_query_id = "\x97\x97\x95\x6d\xfe\xb2\x4c\x99\x86\x8e\xd3\x84\xff\x6f\xd9\x4c";
    testing.expectEqualSlices(u8, exp_query_id, frame.query_id);

    testing.expectEqual(Consistency.Quorum, frame.query_parameters.consistency_level);

    const values = frame.query_parameters.values.?.Normal;
    testing.expectEqual(@as(usize, 1), values.len);
    testing.expectEqualSlices(u8, "\xeb\x11\xc9\x1e\xd8\xcc\x48\x4d\xaf\x55\xe9\x9f\x5c\xd9\xec\x4a", values[0].Set);
    testing.expectEqual(@as(u32, 5000), frame.query_parameters.page_size.?);
    testing.expect(frame.query_parameters.paging_state == null);
    testing.expect(frame.query_parameters.serial_consistency_level == null);
    testing.expectEqual(@as(u64, 1585776216966732), frame.query_parameters.timestamp.?);
    testing.expect(frame.query_parameters.keyspace == null);
    testing.expect(frame.query_parameters.now_in_seconds == null);
}
