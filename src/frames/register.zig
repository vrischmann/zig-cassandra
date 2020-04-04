const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// REGISTER is sent to register this connection to receive some types of events.
///
/// Described in the protocol spec at §4.1.8
const RegisterFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    event_types: [][]const u8,

    pub fn deinit(self: *const Self) void {
        for (self.event_types) |s| {
            self.allocator.free(s);
        }
        self.allocator.free(self.event_types);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .event_types = undefined,
        };

        frame.event_types = (try framer.readStringList()).toOwnedSlice();

        return frame;
    }
};

test "register frame" {
    const data = "\x04\x00\x00\xc0\x0b\x00\x00\x00\x31\x00\x03\x00\x0f\x54\x4f\x50\x4f\x4c\x4f\x47\x59\x5f\x43\x48\x41\x4e\x47\x45\x00\x0d\x53\x54\x41\x54\x55\x53\x5f\x43\x48\x41\x4e\x47\x45\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Register, data.len, framer.header);

    const frame = try RegisterFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqual(@as(usize, 3), frame.event_types.len);
    testing.expectEqualString("TOPOLOGY_CHANGE", frame.event_types[0]);
    testing.expectEqualString("STATUS_CHANGE", frame.event_types[1]);
    testing.expectEqualString("SCHEMA_CHANGE", frame.event_types[2]);
}
