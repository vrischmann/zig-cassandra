const std = @import("std");
const io = std.io;

const testing = @import("../testing.zig");

pub const PrimitiveWriter = struct {
    const Self = @This();

    wbuf: []u8,
    source: io.StreamSource,
    out_stream: io.StreamSource.OutStream,

    pub fn init() Self {
        return Self{
            .wbuf = undefined,
            .source = undefined,
            .out_stream = undefined,
        };
    }

    pub fn reset(self: *Self, wbuf: []u8) void {
        self.wbuf = wbuf;
        self.source = io.StreamSource{ .buffer = io.fixedBufferStream(wbuf) };
        self.out_stream = self.source.outStream();
    }

    /// Write either a short, a int or a long to the stream.
    pub fn writeInt(self: *Self, comptime T: type, value: T) !void {
        return self.out_stream.writeIntBig(T, value);
    }
};

test "primitive writer: write int" {
    var buf: [1024]u8 = undefined;
    var pw = PrimitiveWriter.init();
    pw.reset(&buf);

    // read all int types

    try pw.writeInt(i32, 2101504);
    testing.expectEqualSlices(u8, "\x00\x20\x11\x00", buf[0..4]);

    try pw.writeInt(i64, 70368746279168);
    testing.expectEqualSlices(u8, "\x00\x00\x40\x00\x00\x20\x11\x00", buf[4..12]);

    try pw.writeInt(i16, 4352);
    testing.expectEqualSlices(u8, "\x11\x00", buf[12..14]);

    try pw.writeInt(u8, 0xFF);
    testing.expectEqualSlices(u8, "\xff", buf[14..15]);
}
