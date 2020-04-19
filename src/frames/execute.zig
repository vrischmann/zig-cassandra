const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;

usingnamespace @import("../frame.zig");
usingnamespace @import("../primitive_types.zig");
usingnamespace @import("../query_parameters.zig");
const testing = @import("../testing.zig");

/// EXECUTE is sent to execute a prepared query.
///
/// Described in the protocol spec at §4.1.6
const ExecuteFrame = struct {
    const Self = @This();

    query_id: []const u8,
    result_metadata_id: ?[]const u8,
    query_parameters: QueryParameters,

    pub fn write(self: Self, protocol_version: ProtocolVersion, pw: *PrimitiveWriter) !void {
        _ = try pw.writeShortBytes(self.query_id);
        if (protocol_version.is(5)) {
            if (self.result_metadata_id) |id| {
                _ = try pw.writeShortBytes(id);
            }
        }
        _ = try self.query_parameters.write(protocol_version, pw);
    }

    pub fn read(allocator: *mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        var frame = Self{
            .query_id = undefined,
            .result_metadata_id = null,
            .query_parameters = undefined,
        };

        frame.query_id = (try pr.readShortBytes()) orelse &[_]u8{};
        if (protocol_version.is(5)) {
            frame.result_metadata_id = try pr.readShortBytes();
        }
        frame.query_parameters = try QueryParameters.read(allocator, protocol_version, pr);

        return frame;
    }
};

test "execute frame" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x01\x00\x0a\x00\x00\x00\x37\x00\x10\x97\x97\x95\x6d\xfe\xb2\x4c\x99\x86\x8e\xd3\x84\xff\x6f\xd9\x4c\x00\x04\x27\x00\x01\x00\x00\x00\x10\xeb\x11\xc9\x1e\xd8\xcc\x48\x4d\xaf\x55\xe9\x9f\x5c\xd9\xec\x4a\x00\x00\x13\x88\x00\x05\xa2\x41\x4c\x1b\x06\x4c";
    const raw_frame = try testing.readRawFrame(&arena.allocator, exp);

    checkHeader(Opcode.Execute, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init(&arena.allocator);
    pr.reset(raw_frame.body);

    const frame = try ExecuteFrame.read(&arena.allocator, raw_frame.header.version, &pr);

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

    // write

    testing.expectSameRawFrame(frame, raw_frame.header, exp);
}
