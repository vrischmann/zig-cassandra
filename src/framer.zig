const std = @import("std");
const os = std.os;
const net = std.net;
const ArrayList = std.ArrayList;

const sm = @import("string_map.zig");
usingnamespace @import("primitive_types.zig");
const testing = @import("testing.zig");

pub fn Framer(comptime InStreamType: type) type {
    const BytesType = enum {
        Short,
        Long,
    };

    return struct {
        const Self = @This();

        allocator: *std.mem.Allocator,
        in_stream: InStreamType,

        header: FrameHeader,

        pub fn init(allocator: *std.mem.Allocator, in: InStreamType) Self {
            return Self{
                .allocator = allocator,
                .in_stream = in,
                .header = undefined,
            };
        }

        pub fn deinit(self: *Self) void {}

        pub fn readHeader(self: *Self) !void {
            self.header = try FrameHeader.read(InStreamType, self.in_stream);
        }

        /// Read either a short, a int or a long from the stream.
        pub fn readInt(self: *Self, comptime T: type) !T {
            return self.in_stream.readIntBig(T);
        }

        /// Read a single byte from the stream
        pub fn readByte(self: *Self) !u8 {
            return self.in_stream.readByte();
        }

        /// Read a length-prefixed byte slice from the stream. The length is 2 bytes.
        /// The slice can be null.
        pub fn readShortBytes(self: *Self) !?[]const u8 {
            return self.readBytesGeneric(.Short);
        }

        /// Read a length-prefixed byte slice from the stream. The length is 4 bytes.
        /// The slice can be null.
        pub fn readBytes(self: *Self) !?[]const u8 {
            return self.readBytesGeneric(.Long);
        }

        /// Read bytes from the stream in a generic way.
        fn readBytesGeneric(self: *Self, comptime T: BytesType) !?[]const u8 {
            const len = switch (T) {
                .Short => @as(i32, try self.readInt(i16)),
                .Long => @as(i32, try self.readInt(i32)),
                else => @compileError("invalid bytes length type " ++ @typeName(IntType)),
            };

            if (len < 0) {
                return null;
            }

            const buf = try self.allocator.alloc(u8, @intCast(usize, len));

            const n_read = try self.in_stream.readAll(buf);
            if (n_read != len) {
                return error.UnexpectedEOF;
            }

            return buf;
        }

        /// Read a length-prefixed string from the stream. The length is 2 bytes.
        /// The string can't be null.
        pub fn readString(self: *Self) ![]const u8 {
            if (try self.readBytesGeneric(.Short)) |v| {
                return v;
            } else {
                return error.UnexpectedEOF;
            }
        }

        /// Read a length-prefixed string from the stream. The length is 4 bytes.
        /// The string can't be null.
        pub fn readLongString(self: *Self) ![]const u8 {
            if (try self.readBytesGeneric(.Long)) |v| {
                return v;
            } else {
                return error.UnexpectedEOF;
            }
        }

        /// Read a UUID from the stream.
        pub fn readUUID(self: *Self) ![16]u8 {
            var buf: [16]u8 = undefined;
            _ = try self.in_stream.readAll(&buf);
            return buf;
        }

        /// Read a list of string from the stream.
        pub fn readStringList(self: *Self) !ArrayList([]const u8) {
            const len = @as(usize, try self.readInt(u16));

            var list = try ArrayList([]const u8).initCapacity(self.allocator, len);

            var i: usize = 0;
            while (i < len) {
                const tmp = try self.readString();
                try list.append(tmp);

                i += 1;
            }

            return list;
        }

        /// Read a value from the stream.
        /// A value can be null.
        pub fn readValue(self: *Self) !Value {
            const len = try self.readInt(i32);

            if (len >= 0) {
                const result = try self.allocator.alloc(u8, @intCast(usize, len));
                _ = try self.in_stream.readAll(result);

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

        // TODO(vincent): add read option

        // pub fn readOptionID(self: *Self) !u16 {
        //     return self.readInt(u16);
        // }

        pub fn readInetaddr(self: *Self) !net.Address {
            return self.readInetGeneric(false);
        }

        pub fn readInet(self: *Self) !net.Address {
            return self.readInetGeneric(true);
        }

        fn readInetGeneric(self: *Self, with_port: bool) !net.Address {
            const n = try self.readByte();

            return switch (n) {
                4 => {
                    var buf: [4]u8 = undefined;
                    _ = try self.in_stream.readAll(&buf);

                    const port = if (with_port) try self.readInt(i32) else 0;

                    return net.Address.initIp4(buf, @intCast(u16, port));
                },
                16 => {
                    var buf: [16]u8 = undefined;
                    _ = try self.in_stream.readAll(&buf);

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

        pub fn readStringMap(self: *Self) !sm.Map {
            const n = try self.readInt(u16);

            var map = sm.Map.init(self.allocator);

            var i: usize = 0;
            while (i < n) : (i += 1) {
                const k = try self.readString();
                const v = try self.readString();

                _ = try map.put(k, v);
            }

            return map;
        }

        pub fn readStringMultimap(self: *Self) !sm.Multimap {
            const n = try self.readInt(u16);

            var map = sm.Multimap.init(self.allocator);

            var i: usize = 0;
            while (i < n) : (i += 1) {
                const k = try self.readString();
                const list = try self.readStringList();

                _ = try map.put(k, list);
            }

            return map;
        }
    };
}

test "framer: read int" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // read all int types

    resetAndWrite(fbs_type, &fbs, "\x00\x20\x11\x00");
    testing.expectEqual(@as(i32, 2101504), try framer.readInt(i32));

    resetAndWrite(fbs_type, &fbs, "\x00\x00\x40\x00\x00\x20\x11\x00");
    testing.expectEqual(@as(i64, 70368746279168), try framer.readInt(i64));

    resetAndWrite(fbs_type, &fbs, "\x11\x00");
    testing.expectEqual(@as(u16, 4352), try framer.readInt(u16));

    resetAndWrite(fbs_type, &fbs, "\xff");
    testing.expectEqual(@as(u8, 0xFF), try framer.readByte());
}

test "framer: read strings and bytes" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // short string
    {
        resetAndWrite(fbs_type, &fbs, "\x00\x06foobar");
        var result = try framer.readString();
        defer std.testing.allocator.free(result);

        testing.expectEqualSlices(u8, "foobar", result);

        // long string

        resetAndWrite(fbs_type, &fbs, "\x00\x00\x00\x06foobar");
        result = try framer.readLongString();
        defer std.testing.allocator.free(result);

        testing.expectEqualSlices(u8, "foobar", result);
    }

    // int32 + bytes
    {
        resetAndWrite(fbs_type, &fbs, "\x00\x00\x00\x0A123456789A");
        var result = (try framer.readBytes()).?;
        defer std.testing.allocator.free(result);
        testing.expectEqualSlices(u8, "123456789A", result);

        resetAndWrite(fbs_type, &fbs, "\x00\x00\x00\x00");
        var result2 = (try framer.readBytes()).?;
        defer std.testing.allocator.free(result2);
        testing.expectEqualSlices(u8, "", result2);

        resetAndWrite(fbs_type, &fbs, "\xff\xff\xff\xff");
        testing.expect((try framer.readBytes()) == null);
    }

    // int16 + bytes
    {
        resetAndWrite(fbs_type, &fbs, "\x00\x0A123456789A");
        var result = (try framer.readShortBytes()).?;
        defer std.testing.allocator.free(result);
        testing.expectEqualSlices(u8, "123456789A", result);

        resetAndWrite(fbs_type, &fbs, "\x00\x00");
        var result2 = (try framer.readShortBytes()).?;
        defer std.testing.allocator.free(result2);
        testing.expectEqualSlices(u8, "", result2);

        resetAndWrite(fbs_type, &fbs, "\xff\xff");
        testing.expect((try framer.readShortBytes()) == null);
    }
}

test "framer: read uuid" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // read UUID

    var uuid: [16]u8 = undefined;
    try std.os.getrandom(&uuid);
    resetAndWrite(fbs_type, &fbs, &uuid);

    testing.expectEqualSlices(u8, &uuid, &(try framer.readUUID()));
}

test "framer: read string list" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // read string lists

    resetAndWrite(fbs_type, &fbs, "\x00\x02\x00\x03foo\x00\x03bar");

    var list = try framer.readStringList();
    defer list.deinit();

    var result = list.toOwnedSlice();
    defer std.testing.allocator.free(result);

    testing.expectEqual(@as(usize, 2), result.len);

    var tmp = result[0];
    defer std.testing.allocator.free(tmp);
    testing.expectEqualSlices(u8, "foo", tmp);

    tmp = result[1];
    defer std.testing.allocator.free(tmp);
    testing.expectEqualSlices(u8, "bar", tmp);
}

test "framer: read value" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // Normal value
    resetAndWrite(fbs_type, &fbs, "\x00\x00\x00\x02\x61\x62");

    var value = try framer.readValue();
    defer std.testing.allocator.free(value.Set);
    testing.expect(value == .Set);
    testing.expectString("\x61\x62", value.Set);

    // Null value

    resetAndWrite(fbs_type, &fbs, "\xff\xff\xff\xff");
    var value2 = try framer.readValue();
    testing.expect(value2 == .Null);

    // "Not set" value

    resetAndWrite(fbs_type, &fbs, "\xff\xff\xff\xfe");
    var value3 = try framer.readValue();
    testing.expect(value3 == .NotSet);
}

test "framer: read inet and inetaddr" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // IPv4
    resetAndWrite(fbs_type, &fbs, "\x04\x12\x34\x56\x78\x00\x00\x00\x22");

    var result = try framer.readInet();
    testing.expectEqual(@as(u16, os.AF_INET), result.any.family);
    testing.expectEqual(@as(u32, 0x78563412), result.in.addr);
    testing.expectEqual(@as(u16, 34), result.getPort());

    // IPv6
    resetAndWrite(fbs_type, &fbs, "\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x22");

    result = try framer.readInet();
    testing.expectEqual(@as(u16, os.AF_INET6), result.any.family);
    testing.expectEqualSlices(u8, "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", &result.in6.addr);
    testing.expectEqual(@as(u16, 34), result.getPort());

    // IPv4 without port
    resetAndWrite(fbs_type, &fbs, "\x04\x12\x34\x56\x78");

    result = try framer.readInetaddr();
    testing.expectEqual(@as(u16, os.AF_INET), result.any.family);
    testing.expectEqual(@as(u32, 0x78563412), result.in.addr);
    testing.expectEqual(@as(u16, 0), result.getPort());

    // IPv6 without port
    resetAndWrite(fbs_type, &fbs, "\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff");

    result = try framer.readInetaddr();
    testing.expectEqual(@as(u16, os.AF_INET6), result.any.family);
    testing.expectEqualSlices(u8, "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", &result.in6.addr);
    testing.expectEqual(@as(u16, 0), result.getPort());
}

test "framer: read consistency" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

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
        resetAndWrite(fbs_type, &fbs, tc.b);
        var result = try framer.readConsistency();
        testing.expectEqual(tc.exp, result);
    }
}

test "framer: read stringmap" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // 2 elements string map

    resetAndWrite(fbs_type, &fbs, "\x00\x02\x00\x03foo\x00\x03baz\x00\x03bar\x00\x03baz");

    var result = try framer.readStringMap();
    defer result.deinit();
    testing.expectEqual(@as(usize, 2), result.count());

    var it = result.iterator();
    while (it.next()) |entry| {
        testing.expect(std.mem.eql(u8, "foo", entry.key) or std.mem.eql(u8, "bar", entry.key));
        testing.expectEqualSlices(u8, "baz", entry.value);
    }
}

test "framer: read string multimap" {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fbs_type = @TypeOf(fbs);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);

    // 1 key, 2 values multimap

    resetAndWrite(fbs_type, &fbs, "\x00\x01\x00\x03foo\x00\x02\x00\x03bar\x00\x03baz");

    var result = try framer.readStringMultimap();
    defer result.deinit();
    testing.expectEqual(@as(usize, 1), result.count());

    var it = result.iterator();
    if (it.next()) |entry| {
        testing.expect(std.mem.eql(u8, "foo", entry.key));

        const slice = entry.value.span();

        testing.expectEqual(@as(usize, 2), slice.len);
        testing.expectEqualSlices(u8, "bar", slice[0]);
        testing.expectEqualSlices(u8, "baz", slice[1]);
    } else {
        std.debug.panic("expected bytes to not be null", .{});
    }
}

fn resetAndWrite(comptime T: type, fbs: *T, data: []const u8) void {
    fbs.reset();
    _ = fbs.write(data) catch |err| {
        unreachable;
    };
    fbs.reset();
}
