const std = @import("std");
const io = std.io;
const mem = std.mem;
const net = std.net;
const os = std.os;

usingnamespace @import("../primitive_types.zig");
const sm = @import("../string_map.zig");

const testing = @import("../testing.zig");

pub const PrimitiveWriter = struct {
    const Self = @This();

    // TODO(vincent): I don't like recreating the outStream() everytime but for now it's fine.

    wbuf: std.ArrayList(u8),

    pub fn deinit(self: *Self, allocator: *mem.Allocator) void {
        self.wbuf.deinit();
    }

    pub fn reset(self: *Self, allocator: *mem.Allocator) !void {
        self.wbuf = try std.ArrayList(u8).initCapacity(allocator, 1024);
    }

    pub fn toOwnedSlice(self: *Self) []u8 {
        return self.wbuf.toOwnedSlice();
    }

    pub fn getWritten(self: *Self) []u8 {
        return self.wbuf.span();
    }

    /// Write either a short, a int or a long to the buffer.
    pub fn writeInt(self: *Self, comptime T: type, value: T) !void {
        return self.wbuf.outStream().writeIntBig(T, value);
    }

    /// Write a byte to the buffer.
    pub fn writeByte(self: *Self, value: u8) !void {
        return self.wbuf.append(value);
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
            _ = try self.wbuf.outStream().writeIntBig(LenType, @intCast(LenType, v.len));
            return self.wbuf.appendSlice(v);
        } else {
            return self.wbuf.outStream().writeIntBig(LenType, -1);
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
        return self.wbuf.appendSlice(&uuid);
    }

    /// Write a list of string to the buffer.
    pub fn writeStringList(self: *Self, list: []const []const u8) !void {
        _ = try self.wbuf.outStream().writeIntBig(u16, @intCast(u16, list.len));
        for (list) |value| {
            _ = try self.writeString(value);
        }
    }

    /// Write a value to the buffer.
    pub fn writeValue(self: *Self, value: Value) !void {
        return switch (value) {
            .Null => self.wbuf.outStream().writeIntBig(i32, @as(i32, -1)),
            .NotSet => self.wbuf.outStream().writeIntBig(i32, @as(i32, -2)),
            .Set => |data| {
                _ = try self.wbuf.outStream().writeIntBig(i32, @intCast(i32, data.len));
                return self.wbuf.outStream().writeAll(data);
            },
        };
    }

    pub inline fn writeInetaddr(self: *Self, inet: net.Address) !void {
        return self.writeInetGeneric(inet, false);
    }
    pub inline fn writeInet(self: *Self, inet: net.Address) !void {
        return self.writeInetGeneric(inet, true);
    }

    /// Write a net address to the buffer.
    fn writeInetGeneric(self: *Self, inet: net.Address, comptime with_port: bool) !void {
        switch (inet.any.family) {
            os.AF_INET => {
                if (with_port) {
                    var buf: [9]u8 = undefined;
                    buf[0] = 4;
                    mem.writeIntNative(u32, buf[1..5], inet.in.addr);
                    mem.writeIntBig(u32, buf[5..9], inet.getPort());
                    return self.wbuf.outStream().writeAll(&buf);
                } else {
                    var buf: [5]u8 = undefined;
                    buf[0] = 4;
                    mem.writeIntNative(u32, buf[1..5], inet.in.addr);
                    return self.wbuf.outStream().writeAll(&buf);
                }
            },
            os.AF_INET6 => {
                if (with_port) {
                    var buf: [21]u8 = undefined;
                    buf[0] = 16;
                    mem.copy(u8, buf[1..17], &inet.in6.addr);
                    mem.writeIntBig(u32, buf[17..21], inet.getPort());
                    return self.wbuf.outStream().writeAll(&buf);
                } else {
                    var buf: [17]u8 = undefined;
                    buf[0] = 16;
                    mem.copy(u8, buf[1..17], &inet.in6.addr);
                    return self.wbuf.outStream().writeAll(&buf);
                }
            },
            else => |af| std.debug.panic("invalid address family {}\n", .{af}),
        }
    }

    pub fn writeConsistency(self: *Self, consistency: Consistency) !void {
        const n = @intCast(u16, @enumToInt(consistency));
        return self.wbuf.outStream().writeIntBig(u16, n);
    }

    pub fn startStringMap(self: *Self, size: usize) !void {
        _ = try self.wbuf.outStream().writeIntBig(u16, @intCast(u16, size));
    }
};

test "primitive writer: write int" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pw: PrimitiveWriter = undefined;
    try pw.reset(&arena.allocator);

    try pw.writeInt(i32, 2101504);
    testing.expectEqualSlices(u8, "\x00\x20\x11\x00", pw.getWritten()[0..4]);

    try pw.writeInt(i64, 70368746279168);
    testing.expectEqualSlices(u8, "\x00\x00\x40\x00\x00\x20\x11\x00", pw.getWritten()[4..12]);

    try pw.writeInt(i16, 4352);
    testing.expectEqualSlices(u8, "\x11\x00", pw.getWritten()[12..14]);

    try pw.writeByte(0xFF);
    testing.expectEqualSlices(u8, "\xff", pw.getWritten()[14..15]);
}

test "primitive writer: write strings and bytes" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pw: PrimitiveWriter = undefined;
    try pw.reset(&arena.allocator);

    {
        // short string
        _ = try pw.writeString("foobar");
        testing.expectEqualSlices(u8, "\x00\x06foobar", pw.getWritten()[0..8]);

        // long string

        _ = try pw.writeLongString("foobar");
        testing.expectEqualSlices(u8, "\x00\x00\x00\x06foobar", pw.getWritten()[8..18]);
    }

    {
        // int32 + bytes
        _ = try pw.writeBytes("123456789A");
        testing.expectEqualSlices(u8, "\x00\x00\x00\x0A123456789A", pw.getWritten()[18..32]);

        _ = try pw.writeBytes("");
        testing.expectEqualSlices(u8, "\x00\x00\x00\x00", pw.getWritten()[32..36]);

        _ = try pw.writeBytes(null);
        testing.expectEqualSlices(u8, "\xff\xff\xff\xff", pw.getWritten()[36..40]);
    }

    {
        // int16 + bytes
        _ = try pw.writeShortBytes("123456789A");
        testing.expectEqualSlices(u8, "\x00\x0A123456789A", pw.getWritten()[40..52]);

        _ = try pw.writeShortBytes("");
        testing.expectEqualSlices(u8, "\x00\x00", pw.getWritten()[52..54]);

        _ = try pw.writeShortBytes(null);
        testing.expectEqualSlices(u8, "\xff\xff", pw.getWritten()[54..56]);
    }
}

test "primitive writer: write uuid" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pw: PrimitiveWriter = undefined;
    try pw.reset(&arena.allocator);

    var uuid: [16]u8 = undefined;
    try std.os.getrandom(&uuid);

    _ = try pw.writeUUID(uuid);
    testing.expectEqualSlices(u8, &uuid, pw.getWritten()[0..16]);
}

test "primitive writer: write string list" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pw: PrimitiveWriter = undefined;
    try pw.reset(&arena.allocator);

    const list = &[_][]const u8{ "foo", "bar" };

    _ = try pw.writeStringList(list);
    testing.expectEqualSlices(u8, "\x00\x02\x00\x03foo\x00\x03bar", pw.getWritten()[0..12]);
}

test "primitive writer: write value" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pw: PrimitiveWriter = undefined;
    try pw.reset(&arena.allocator);

    // Normal value
    _ = try pw.writeValue(Value{ .Set = "ab" });
    testing.expectEqualSlices(u8, "\x00\x00\x00\x02\x61\x62", pw.getWritten()[0..6]);

    // Null value
    _ = try pw.writeValue(Value{ .Null = {} });
    testing.expectEqualSlices(u8, "\xff\xff\xff\xff", pw.getWritten()[6..10]);

    // "Not set" value
    _ = try pw.writeValue(Value{ .NotSet = {} });
    testing.expectEqualSlices(u8, "\xff\xff\xff\xfe", pw.getWritten()[10..14]);
}

test "primitive writer: write inet and inetaddr" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pw: PrimitiveWriter = undefined;
    try pw.reset(&arena.allocator);

    // IPv4
    _ = try pw.writeInet(net.Address.initIp4([_]u8{ 0x78, 0x56, 0x34, 0x12 }, 34));
    testing.expectEqualSlices(u8, "\x04\x78\x56\x34\x12\x00\x00\x00\x22", pw.getWritten()[0..9]);

    // IPv6
    _ = try pw.writeInet(net.Address.initIp6([_]u8{0xff} ** 16, 34, 0, 0));
    testing.expectEqualSlices(u8, "\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x22", pw.getWritten()[9..30]);

    // IPv4 without port
    _ = try pw.writeInetaddr(net.Address.initIp4([_]u8{ 0x78, 0x56, 0x34, 0x12 }, 34));
    testing.expectEqualSlices(u8, "\x04\x78\x56\x34\x12", pw.getWritten()[30..35]);

    // IPv6 without port
    _ = try pw.writeInetaddr(net.Address.initIp6([_]u8{0xff} ** 16, 34, 0, 0));
    testing.expectEqualSlices(u8, "\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", pw.getWritten()[35..52]);
}

test "primitive writer: write consistency" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pw: PrimitiveWriter = undefined;
    try pw.reset(&arena.allocator);

    const testCase = struct {
        consistency: Consistency,
        exp: []const u8,
    };

    const testCases = [_]testCase{
        testCase{ .consistency = Consistency.Any, .exp = "\x00\x00" },
        testCase{ .consistency = Consistency.One, .exp = "\x00\x01" },
        testCase{ .consistency = Consistency.Two, .exp = "\x00\x02" },
        testCase{ .consistency = Consistency.Three, .exp = "\x00\x03" },
        testCase{ .consistency = Consistency.Quorum, .exp = "\x00\x04" },
        testCase{ .consistency = Consistency.All, .exp = "\x00\x05" },
        testCase{ .consistency = Consistency.LocalQuorum, .exp = "\x00\x06" },
        testCase{ .consistency = Consistency.EachQuorum, .exp = "\x00\x07" },
        testCase{ .consistency = Consistency.Serial, .exp = "\x00\x08" },
        testCase{ .consistency = Consistency.LocalSerial, .exp = "\x00\x09" },
        testCase{ .consistency = Consistency.LocalOne, .exp = "\x00\x0A" },
    };

    for (testCases) |tc| {
        try pw.reset(&arena.allocator);
        _ = try pw.writeConsistency(tc.consistency);
        testing.expectEqualSlices(u8, tc.exp, pw.getWritten()[0..2]);
    }
}
