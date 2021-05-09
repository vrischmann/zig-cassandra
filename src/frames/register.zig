const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;

usingnamespace @import("../frame.zig");
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// REGISTER is sent to register this connection to receive some types of events.
///
/// Described in the protocol spec at §4.1.8
const RegisterFrame = struct {
    event_types: []const []const u8,

    pub fn write(self: @This(), pw: *PrimitiveWriter) !void {
        return pw.writeStringList(self.event_types);
    }

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !RegisterFrame {
        return RegisterFrame{
            .event_types = try pr.readStringList(allocator),
        };
    }
};

test "register frame" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\xc0\x0b\x00\x00\x00\x31\x00\x03\x00\x0f\x54\x4f\x50\x4f\x4c\x4f\x47\x59\x5f\x43\x48\x41\x4e\x47\x45\x00\x0d\x53\x54\x41\x54\x55\x53\x5f\x43\x48\x41\x4e\x47\x45\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45";
    const raw_frame = try testing.readRawFrame(&arena.allocator, exp);

    try checkHeader(Opcode.Register, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try RegisterFrame.read(&arena.allocator, &pr);

    try testing.expectEqual(@as(usize, 3), frame.event_types.len);
    try testing.expectEqualStrings("TOPOLOGY_CHANGE", frame.event_types[0]);
    try testing.expectEqualStrings("STATUS_CHANGE", frame.event_types[1]);
    try testing.expectEqualStrings("SCHEMA_CHANGE", frame.event_types[2]);

    // write

    try testing.expectSameRawFrame(frame, raw_frame.header, exp);
}
