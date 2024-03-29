const std = @import("std");
const io = std.io;
const mem = std.mem;
const net = std.net;
const os = std.os;

usingnamespace @import("../primitive_types.zig");
const sm = @import("../string_map.zig");

const testing = @import("../testing.zig");

pub const PrimitiveReader = struct {
    const Self = @This();

    buffer: io.FixedBufferStream([]const u8),
    reader: io.FixedBufferStream([]const u8).Reader,

    pub fn init() Self {
        return Self{
            .buffer = undefined,
            .reader = undefined,
        };
    }

    pub fn reset(self: *Self, rbuf: []const u8) void {
        self.buffer = io.fixedBufferStream(rbuf);
        self.reader = self.buffer.reader();
    }

    /// Read either a short, a int or a long from the buffer.
    pub fn readInt(self: *Self, comptime T: type) !T {
        return self.reader.readIntBig(T);
    }

    /// Read a single byte from the buffer.
    pub fn readByte(self: *Self) !u8 {
        return self.reader.readByte();
    }

    /// Read a length-prefixed byte slice from the stream. The length is 2 bytes.
    /// The slice can be null.
    pub fn readShortBytes(self: *Self, allocator: *mem.Allocator) !?[]const u8 {
        return self.readBytesGeneric(allocator, i16);
    }

    /// Read a length-prefixed byte slice from the stream. The length is 4 bytes.
    /// The slice can be null.
    pub fn readBytes(self: *Self, allocator: *mem.Allocator) !?[]const u8 {
        return self.readBytesGeneric(allocator, i32);
    }

    /// Read bytes from the stream in a generic way.
    fn readBytesGeneric(self: *Self, allocator: *mem.Allocator, comptime LenType: type) !?[]const u8 {
        const len = try self.readInt(LenType);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return &[_]u8{};
        }

        const buf = try allocator.alloc(u8, @intCast(usize, len));

        const n_read = try self.reader.readAll(buf);
        if (n_read != len) {
            return error.UnexpectedEOF;
        }

        return buf;
    }

    /// Read a length-prefixed string from the stream. The length is 2 bytes.
    /// The string can't be null.
    pub fn readString(self: *Self, allocator: *mem.Allocator) ![]const u8 {
        if (try self.readBytesGeneric(allocator, i16)) |v| {
            return v;
        } else {
            return error.UnexpectedEOF;
        }
    }

    /// Read a length-prefixed string from the stream. The length is 4 bytes.
    /// The string can't be null.
    pub fn readLongString(self: *Self, allocator: *mem.Allocator) ![]const u8 {
        if (try self.readBytesGeneric(allocator, i32)) |v| {
            return v;
        } else {
            return error.UnexpectedEOF;
        }
    }

    /// Read a UUID from the stream.
    pub fn readUUID(self: *Self) ![16]u8 {
        var buf: [16]u8 = undefined;
        _ = try self.reader.readAll(&buf);
        return buf;
    }

    /// Read a list of string from the stream.
    pub fn readStringList(self: *Self, allocator: *mem.Allocator) ![]const []const u8 {
        const len = @as(usize, try self.readInt(u16));

        var list = try std.ArrayList([]const u8).initCapacity(allocator, len);

        var i: usize = 0;
        while (i < len) {
            const tmp = try self.readString(allocator);
            try list.append(tmp);

            i += 1;
        }

        return list.toOwnedSlice();
    }

    /// Read a value from the stream.
    pub fn readValue(self: *Self, allocator: *mem.Allocator) !Value {
        const len = try self.readInt(i32);

        if (len >= 0) {
            const result = try allocator.alloc(u8, @intCast(usize, len));
            _ = try self.reader.readAll(result);

            return Value{ .Set = result };
        } else if (len == -1) {
            return Value.Null;
        } else if (len == -2) {
            return Value.NotSet;
        } else {
            return error.InvalidValueLength;
        }
    }

    pub fn readVarint(self: *Self, comptime IntType: type) !IntType {
        // TODO(vincent): implement this for uvint and vint
        unreachable;
    }
    pub fn readInetaddr(self: *Self) callconv(.Inline) !net.Address {
        return self.readInetGeneric(false);
    }
    pub fn readInet(self: *Self) callconv(.Inline) !net.Address {
        return self.readInetGeneric(true);
    }

    fn readInetGeneric(self: *Self, comptime with_port: bool) !net.Address {
        const n = try self.readByte();

        return switch (n) {
            4 => {
                var buf: [4]u8 = undefined;
                _ = try self.reader.readAll(&buf);

                const port = if (with_port) try self.readInt(i32) else 0;

                return net.Address.initIp4(buf, @intCast(u16, port));
            },
            16 => {
                var buf: [16]u8 = undefined;
                _ = try self.reader.readAll(&buf);

                const port = if (with_port) try self.readInt(i32) else 0;

                return net.Address.initIp6(buf, @intCast(u16, port), 0, 0);
            },
            else => return error.InvalidInetSize,
        };
    }

    pub fn readConsistency(self: *Self) !Consistency {
        const n = try self.readInt(u16);

        return @intToEnum(Consistency, n);
    }

    pub fn readStringMap(self: *Self, allocator: *mem.Allocator) !sm.Map {
        const n = try self.readInt(u16);

        var map = sm.Map.init(allocator);

        var i: usize = 0;
        while (i < n) : (i += 1) {
            const k = try self.readString(allocator);
            const v = try self.readString(allocator);

            _ = try map.put(k, v);
        }

        return map;
    }

    pub fn readStringMultimap(self: *Self, allocator: *mem.Allocator) !sm.Multimap {
        const n = try self.readInt(u16);

        var map = sm.Multimap.init(allocator);

        var i: usize = 0;
        while (i < n) : (i += 1) {
            const k = try self.readString(allocator);
            const list = try self.readStringList(allocator);

            _ = try map.put(k, list);
        }

        return map;
    }
};

test "primitive reader: read int" {
    var pr = PrimitiveReader.init();

    pr.reset("\x00\x20\x11\x00");
    try testing.expectEqual(@as(i32, 2101504), try pr.readInt(i32));

    pr.reset("\x00\x00\x40\x00\x00\x20\x11\x00");
    try testing.expectEqual(@as(i64, 70368746279168), try pr.readInt(i64));

    pr.reset("\x11\x00");
    try testing.expectEqual(@as(u16, 4352), try pr.readInt(u16));

    pr.reset("\xff");
    try testing.expectEqual(@as(u8, 0xFF), try pr.readByte());
}

test "primitive reader: read strings and bytes" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init();

    {
        // short string
        pr.reset("\x00\x06foobar");
        try testing.expectEqualStrings("foobar", try pr.readString(&arena.allocator));

        // long string
        pr.reset("\x00\x00\x00\x06foobar");
        try testing.expectEqualStrings("foobar", try pr.readLongString(&arena.allocator));
    }

    {
        // int32 + bytes
        pr.reset("\x00\x00\x00\x0A123456789A");
        try testing.expectEqualStrings("123456789A", (try pr.readBytes(&arena.allocator)).?);

        pr.reset("\x00\x00\x00\x00");
        try testing.expectEqualStrings("", (try pr.readBytes(&arena.allocator)).?);

        pr.reset("\xff\xff\xff\xff");
        try testing.expect((try pr.readBytes(&arena.allocator)) == null);
    }

    {
        // int16 + bytes
        pr.reset("\x00\x0A123456789A");
        try testing.expectEqualStrings("123456789A", (try pr.readShortBytes(&arena.allocator)).?);

        pr.reset("\x00\x00");
        try testing.expectEqualStrings("", (try pr.readShortBytes(&arena.allocator)).?);

        pr.reset("\xff\xff");
        try testing.expect((try pr.readShortBytes(&arena.allocator)) == null);
    }
}

test "primitive reader: read uuid" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init();

    var uuid: [16]u8 = undefined;
    try std.os.getrandom(&uuid);
    pr.reset(&uuid);

    try testing.expectEqualSlices(u8, &uuid, &(try pr.readUUID()));
}

test "primitive reader: read string list" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init();

    pr.reset("\x00\x02\x00\x03foo\x00\x03bar");

    var result = try pr.readStringList(&arena.allocator);
    try testing.expectEqual(@as(usize, 2), result.len);

    var tmp = result[0];
    try testing.expectEqualStrings("foo", tmp);

    tmp = result[1];
    try testing.expectEqualStrings("bar", tmp);
}

test "primitive reader: read value" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init();

    // Normal value
    pr.reset("\x00\x00\x00\x02\x61\x62");

    var value = try pr.readValue(&arena.allocator);
    try testing.expect(value == .Set);
    try testing.expectEqualStrings("ab", value.Set);

    // Null value

    pr.reset("\xff\xff\xff\xff");
    var value2 = try pr.readValue(&arena.allocator);
    try testing.expect(value2 == .Null);

    // "Not set" value

    pr.reset("\xff\xff\xff\xfe");
    var value3 = try pr.readValue(&arena.allocator);
    try testing.expect(value3 == .NotSet);
}

test "primitive reader: read inet and inetaddr" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init();

    // IPv4
    pr.reset("\x04\x12\x34\x56\x78\x00\x00\x00\x22");

    var result = try pr.readInet();
    try testing.expectEqual(@as(u16, os.AF_INET), result.any.family);
    try testing.expectEqual(@as(u32, 0x78563412), result.in.sa.addr);
    try testing.expectEqual(@as(u16, 34), result.getPort());

    // IPv6
    pr.reset("\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x22");

    result = try pr.readInet();
    try testing.expectEqual(@as(u16, os.AF_INET6), result.any.family);
    try testing.expectEqualSlices(u8, &[_]u8{0xff} ** 16, &result.in6.sa.addr);
    try testing.expectEqual(@as(u16, 34), result.getPort());

    // IPv4 without port
    pr.reset("\x04\x12\x34\x56\x78");

    result = try pr.readInetaddr();
    try testing.expectEqual(@as(u16, os.AF_INET), result.any.family);
    try testing.expectEqual(@as(u32, 0x78563412), result.in.sa.addr);
    try testing.expectEqual(@as(u16, 0), result.getPort());

    // IPv6 without port
    pr.reset("\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff");

    result = try pr.readInetaddr();
    try testing.expectEqual(@as(u16, os.AF_INET6), result.any.family);
    try testing.expectEqualSlices(u8, &[_]u8{0xff} ** 16, &result.in6.sa.addr);
    try testing.expectEqual(@as(u16, 0), result.getPort());
}

test "primitive reader: read consistency" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init();

    const testCase = struct {
        exp: Consistency,
        b: []const u8,
    };

    const testCases = [_]testCase{
        testCase{ .exp = Consistency.Any, .b = "\x00\x00" },
        testCase{ .exp = Consistency.One, .b = "\x00\x01" },
        testCase{ .exp = Consistency.Two, .b = "\x00\x02" },
        testCase{ .exp = Consistency.Three, .b = "\x00\x03" },
        testCase{ .exp = Consistency.Quorum, .b = "\x00\x04" },
        testCase{ .exp = Consistency.All, .b = "\x00\x05" },
        testCase{ .exp = Consistency.LocalQuorum, .b = "\x00\x06" },
        testCase{ .exp = Consistency.EachQuorum, .b = "\x00\x07" },
        testCase{ .exp = Consistency.Serial, .b = "\x00\x08" },
        testCase{ .exp = Consistency.LocalSerial, .b = "\x00\x09" },
        testCase{ .exp = Consistency.LocalOne, .b = "\x00\x0A" },
    };

    for (testCases) |tc| {
        pr.reset(tc.b);
        var result = try pr.readConsistency();
        try testing.expectEqual(tc.exp, result);
    }
}

test "primitive reader: read stringmap" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init();

    // 2 elements string map

    pr.reset("\x00\x02\x00\x03foo\x00\x03baz\x00\x03bar\x00\x03baz");

    var result = try pr.readStringMap(&arena.allocator);
    try testing.expectEqual(@as(usize, 2), result.count());

    var it = result.iterator();
    while (it.next()) |entry| {
        try testing.expect(std.mem.eql(u8, "foo", entry.key) or std.mem.eql(u8, "bar", entry.key));
        try testing.expectEqualStrings("baz", entry.value);
    }
}

test "primitive reader: read string multimap" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var pr = PrimitiveReader.init();

    // 1 key, 2 values multimap

    pr.reset("\x00\x01\x00\x03foo\x00\x02\x00\x03bar\x00\x03baz");

    var result = try pr.readStringMultimap(&arena.allocator);
    try testing.expectEqual(@as(usize, 1), result.count());

    const slice = result.get("foo").?;
    try testing.expectEqualStrings("bar", slice[0]);
    try testing.expectEqualStrings("baz", slice[1]);
}
