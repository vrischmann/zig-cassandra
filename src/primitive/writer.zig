const std = @import("std");
const io = std.io;

usingnamespace @import("../primitive_types.zig");
const sm = @import("../string_map.zig");

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

    /// Write a UUID to the buffer.
    pub fn writeUUID(self: *Self, uuid: [16]u8) !void {
        return self.out_stream.writeAll(&uuid);
    }

    /// Write a list of string to the buffer.
    pub fn writeStringList(self: *Self, list: []const []const u8) !void {
        _ = try self.out_stream.writeIntBig(u16, @intCast(u16, list.len));
        for (list) |value| {
            _ = try self.writeString(value);
        }
    }

    /// Write a value to the buffer.
    pub fn writeValue(self: *Self, value: Value) !void {
        return switch (value) {
            .Null => self.out_stream.writeIntBig(i32, @as(i32, -1)),
            .NotSet => self.out_stream.writeIntBig(i32, @as(i32, -2)),
            .Set => |data| {
                _ = try self.out_stream.writeIntBig(i32, @intCast(i32, data.len));
                return self.out_stream.writeAll(data);
            },
        };
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
        testing.expectEqualSlices(u8, "\x00\x06foobar", buf[0..8]);

        // long string

        _ = try pw.writeLongString("foobar");
        testing.expectEqualSlices(u8, "\x00\x00\x00\x06foobar", buf[8..18]);
    }

    {
        // int32 + bytes
        _ = try pw.writeBytes("123456789A");
        testing.expectEqualSlices(u8, "\x00\x00\x00\x0A123456789A", buf[18..32]);

        _ = try pw.writeBytes("");
        testing.expectEqualSlices(u8, "\x00\x00\x00\x00", buf[32..36]);

        _ = try pw.writeBytes(null);
        testing.expectEqualSlices(u8, "\xff\xff\xff\xff", buf[36..40]);
    }

    {
        // int16 + bytes
        _ = try pw.writeShortBytes("123456789A");
        testing.expectEqualSlices(u8, "\x00\x0A123456789A", buf[40..52]);

        _ = try pw.writeShortBytes("");
        testing.expectEqualSlices(u8, "\x00\x00", buf[52..54]);

        _ = try pw.writeShortBytes(null);
        testing.expectEqualSlices(u8, "\xff\xff", buf[54..56]);
    }
}

test "primitive writer: write uuid" {
    var buf: [1024]u8 = undefined;
    var pw = PrimitiveWriter.init();
    pw.reset(&buf);

    var uuid: [16]u8 = undefined;
    try std.os.getrandom(&uuid);

    _ = try pw.writeUUID(uuid);
    testing.expectEqualSlices(u8, &uuid, buf[0..16]);
}

test "primitive writer: write string list" {
    var buf: [1024]u8 = undefined;
    var pw = PrimitiveWriter.init();
    pw.reset(&buf);

    const list = &[_][]const u8{ "foo", "bar" };

    _ = try pw.writeStringList(list);
    testing.expectEqualSlices(u8, "\x00\x02\x00\x03foo\x00\x03bar", buf[0..12]);
}

test "primitive writer: write value" {
    var buf: [1024]u8 = undefined;
    var pw = PrimitiveWriter.init();
    pw.reset(&buf);

    // Normal value
    _ = try pw.writeValue(Value{ .Set = "ab" });
    testing.expectEqualSlices(u8, "\x00\x00\x00\x02\x61\x62", buf[0..6]);

    // Null value
    _ = try pw.writeValue(Value{ .Null = {} });
    testing.expectEqualSlices(u8, "\xff\xff\xff\xff", buf[6..10]);

    // "Not set" value
    _ = try pw.writeValue(Value{ .NotSet = {} });
    testing.expectEqualSlices(u8, "\xff\xff\xff\xfe", buf[10..14]);
}
