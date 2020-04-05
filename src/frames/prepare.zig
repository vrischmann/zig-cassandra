const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// PREPARE is sent to prepare a CQL query for later execution (through EXECUTE).
///
/// Described in the protocol spec at §4.1.5
const PrepareFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    query: []const u8,
    keyspace: ?[]const u8,

    pub fn deinit(self: Self) void {
        self.allocator.free(self.query);
    }

    const FlagWithKeyspace = 0x01;

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .query = undefined,
            .keyspace = null,
        };

        frame.query = try framer.readLongString();

        if (framer.header.version != ProtocolVersion.V5) {
            return frame;
        }

        const flags = try framer.readInt(u32);
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            frame.keyspace = try framer.readString();
        }

        return frame;
    }
};

test "prepare frame" {
    const data = "\x04\x00\x00\xc0\x09\x00\x00\x00\x32\x00\x00\x00\x2e\x53\x45\x4c\x45\x43\x54\x20\x61\x67\x65\x2c\x20\x6e\x61\x6d\x65\x20\x66\x72\x6f\x6d\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x20\x77\x68\x65\x72\x65\x20\x69\x64\x20\x3d\x20\x3f";

    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Prepare, data.len, framer.header);

    const frame = try PrepareFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqualString("SELECT age, name from foobar.user where id = ?", frame.query);
    testing.expect(frame.keyspace == null);
}
