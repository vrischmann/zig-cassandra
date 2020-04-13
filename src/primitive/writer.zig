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

    /// Write either a short, a int or a long to the buffer.
    pub fn writeInt(self: *Self, comptime T: type, value: T) !void {
        return self.out_stream.writeIntBig(T, value);
    }

    /// Write a byte to the buffer.
    pub fn writeByte(self: *Self, value: u8) !void {
        return self.out_stream.writeByte(value);
    }

    /// Write a length-prefixed byte slice to the buffer. The length is 2 bytes.
    /// The slice can be null.
    pub fn writeShortBytes(self: *Self, value: ?[]const u8) !void {
        return self.writeBytesGeneric(i16, value);
    }

    /// Write a length-prefixed byte slice to the buffer. The length is 4 bytes.
    /// The slice can be null.
    pub fn writeBytes(self: *Self, value: ?[]const u8) !void {
        return self.writeBytesGeneric(i32, value);
    }

    /// Write bytes from the stream in a generic way.
    fn writeBytesGeneric(self: *Self, comptime LenType: type, value: ?[]const u8) !void {
        if (value) |v| {
            _ = try self.out_stream.writeIntBig(LenType, @intCast(LenType, v.len));
            return self.out_stream.writeAll(v);
        } else {
            return self.out_stream.writeIntBig(LenType, -1);
        }
    }

    /// Write a length-prefixed string to the buffer. The length is 2 bytes.
    /// The string can't be null.
    pub fn writeString(self: *Self, value: []const u8) !void {
        return self.writeBytesGeneric(i16, value);
    }

    /// Write a length-prefixed string to the buffer. The length is 4 bytes.
    /// The string can't be null.
    pub fn writeLongString(self: *Self, value: []const u8) !void {
        return self.writeBytesGeneric(i32, value);
    }
};

test "primitive writer: write int" {
    var buf: [1024]u8 = undefined;
    var pw = PrimitiveWriter.init();
    pw.reset(&buf);

    try pw.writeInt(i32, 2101504);
    testing.expectEqualSlices(u8, "\x00\x20\x11\x00", buf[0..4]);

    try pw.writeInt(i64, 70368746279168);
    testing.expectEqualSlices(u8, "\x00\x00\x40\x00\x00\x20\x11\x00", buf[4..12]);

    try pw.writeInt(i16, 4352);
    testing.expectEqualSlices(u8, "\x11\x00", buf[12..14]);

    try pw.writeByte(0xFF);
    testing.expectEqualSlices(u8, "\xff", buf[14..15]);
}

test "primitive writer: write strings and bytes" {
    var buf: [1024]u8 = undefined;
    var pw = PrimitiveWriter.init();
    pw.reset(&buf);

    {
        // short string
        _ = try pw.writeString("foobar");
        testing.expectEqualString("\x00\x06foobar", buf[0..8]);

        // long string

        _ = try pw.writeLongString("foobar");
        testing.expectEqualString("\x00\x00\x00\x06foobar", buf[8..18]);
    }

    // // int32 + bytes
    // {
    //     pr.reset("\x00\x00\x00\x0A123456789A");
    //     testing.expectEqualString("123456789A", (try pr.readBytes()).?);

    //     pr.reset("\x00\x00\x00\x00");
    //     testing.expectEqualString("", (try pr.readBytes()).?);

    //     pr.reset("\xff\xff\xff\xff");
    //     testing.expect((try pr.readBytes()) == null);
    // }

    // // int16 + bytes
    // {
    //     pr.reset("\x00\x0A123456789A");
    //     testing.expectEqualString("123456789A", (try pr.readShortBytes()).?);

    //     pr.reset("\x00\x00");
    //     testing.expectEqualString("", (try pr.readShortBytes()).?);

    //     pr.reset("\xff\xff");
    //     testing.expect((try pr.readShortBytes()) == null);
    // }
}
