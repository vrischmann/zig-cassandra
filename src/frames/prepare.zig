const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;

usingnamespace @import("../frame.zig");
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// PREPARE is sent to prepare a CQL query for later execution (through EXECUTE).
///
/// Described in the protocol spec at §4.1.5
pub const PrepareFrame = struct {
    const Self = @This();

    query: []const u8,
    keyspace: ?[]const u8,

    const FlagWithKeyspace = 0x01;

    pub fn write(self: Self, protocol_version: ProtocolVersion, pw: *PrimitiveWriter) !void {
        _ = try pw.writeLongString(self.query);
        if (!protocol_version.is(5)) {
            return;
        }

        if (self.keyspace) |ks| {
            _ = try pw.writeInt(u32, FlagWithKeyspace);
            _ = try pw.writeString(ks);
        } else {
            _ = try pw.writeInt(u32, 0);
        }
    }

    pub fn read(allocator: *mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        var frame = Self{
            .query = undefined,
            .keyspace = null,
        };

        frame.query = try pr.readLongString(allocator);

        if (!protocol_version.is(5)) {
            return frame;
        }

        const flags = try pr.readInt(u32);
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            frame.keyspace = try pr.readString(allocator);
        }

        return frame;
    }
};

test "prepare frame" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\xc0\x09\x00\x00\x00\x32\x00\x00\x00\x2e\x53\x45\x4c\x45\x43\x54\x20\x61\x67\x65\x2c\x20\x6e\x61\x6d\x65\x20\x66\x72\x6f\x6d\x20\x66\x6f\x6f\x62\x61\x72\x2e\x75\x73\x65\x72\x20\x77\x68\x65\x72\x65\x20\x69\x64\x20\x3d\x20\x3f";
    const raw_frame = try testing.readRawFrame(&arena.allocator, exp);

    checkHeader(Opcode.Prepare, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try PrepareFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    testing.expectEqualStrings("SELECT age, name from foobar.user where id = ?", frame.query);
    testing.expect(frame.keyspace == null);

    // write

    testing.expectSameRawFrame(frame, raw_frame.header, exp);
}
