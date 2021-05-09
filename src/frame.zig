const std = @import("std");
const io = std.io;
const heap = std.heap;
const mem = std.mem;
const os = std.os;
const net = std.net;

usingnamespace @import("primitive_types.zig");
pub const PrimitiveReader = @import("primitive/reader.zig").PrimitiveReader;
pub const PrimitiveWriter = @import("primitive/writer.zig").PrimitiveWriter;

const testing = @import("testing.zig");

pub const FrameFlags = struct {
    pub const Compression: u8 = 0x01;
    pub const Tracing: u8 = 0x02;
    pub const CustomPayload: u8 = 0x04;
    pub const Warning: u8 = 0x08;
    pub const UseBeta: u8 = 0x10;
};

pub const FrameHeader = packed struct {
    version: ProtocolVersion,
    flags: u8,
    stream: i16,
    opcode: Opcode,
    body_len: u32,

    pub fn init(comptime ReaderType: type, in: ReaderType) !FrameHeader {
        var buf: [@sizeOf(FrameHeader)]u8 = undefined;

        const read = try in.readAll(&buf);
        if (read != @sizeOf(FrameHeader)) {
            return error.UnexpectedEOF;
        }

        return FrameHeader{
            .version = try ProtocolVersion.init(buf[0]),
            .flags = buf[1],
            .stream = mem.readIntBig(i16, @ptrCast(*[2]u8, buf[2..4])),
            .opcode = @intToEnum(Opcode, buf[4]),
            .body_len = mem.readIntBig(u32, @ptrCast(*[4]u8, buf[5..9])),
        };
    }
};

pub const RawFrame = struct {
    header: FrameHeader,
    body: []const u8,

    pub fn deinit(self: @This(), allocator: *mem.Allocator) void {
        allocator.free(self.body);
    }
};

pub fn RawFrameReader(comptime ReaderType: type) type {
    return struct {
        const Self = @This();

        reader: ReaderType,

        pub fn init(in: ReaderType) Self {
            return Self{
                .reader = in,
            };
        }

        pub fn read(self: *Self, allocator: *mem.Allocator) !RawFrame {
            var buf: [@sizeOf(FrameHeader)]u8 = undefined;

            const n_header_read = try self.reader.readAll(&buf);
            if (n_header_read != @sizeOf(FrameHeader)) {
                return error.UnexpectedEOF;
            }

            var header = FrameHeader{
                .version = ProtocolVersion{ .version = buf[0] },
                .flags = buf[1],
                .stream = mem.readIntBig(i16, @ptrCast(*[2]u8, buf[2..4])),
                .opcode = @intToEnum(Opcode, buf[4]),
                .body_len = mem.readIntBig(u32, @ptrCast(*[4]u8, buf[5..9])),
            };

            const len = @as(usize, header.body_len);

            const body = try allocator.alloc(u8, len);
            const n_read = try self.reader.readAll(body);
            if (n_read != len) {
                return error.UnexpectedEOF;
            }

            return RawFrame{
                .header = header,
                .body = body,
            };
        }
    };
}

pub fn RawFrameWriter(comptime WriterType: type) type {
    return struct {
        const Self = @This();

        writer: WriterType,

        pub fn init(out: WriterType) Self {
            return Self{
                .writer = out,
            };
        }

        pub fn write(self: *Self, raw_frame: RawFrame) !void {
            var buf: [@sizeOf(FrameHeader)]u8 = undefined;

            buf[0] = raw_frame.header.version.version;
            buf[1] = raw_frame.header.flags;
            mem.writeIntBig(i16, @ptrCast(*[2]u8, buf[2..4]), raw_frame.header.stream);
            buf[4] = @enumToInt(raw_frame.header.opcode);
            mem.writeIntBig(u32, @ptrCast(*[4]u8, buf[5..9]), raw_frame.header.body_len);

            try self.writer.writeAll(&buf);
            try self.writer.writeAll(raw_frame.body);
        }
    };
}

// TODO(vincent): do we want to keep these wrapper types ?

/// ColumnData is a wrapper around a slice of bytes.
pub const ColumnData = struct {
    slice: []const u8,
};

/// RowData is a wrapper around a slice of ColumnData.
pub const RowData = struct {
    slice: []const ColumnData,
};

pub fn checkHeader(opcode: Opcode, data_len: usize, header: FrameHeader) !void {
    // We can only use v4 for now
    try testing.expect(header.version.is(4));
    // Don't care about the flags here
    // Don't care about the stream
    try testing.expectEqual(opcode, header.opcode);
    try testing.expectEqual(@as(usize, header.body_len), data_len - @sizeOf(FrameHeader));
}

test "frame header: read and write" {
    const exp = "\x04\x00\x00\xd7\x05\x00\x00\x00\x00";
    var fbs = io.fixedBufferStream(exp);

    // deserialize the header

    var reader = fbs.reader();

    const header = try FrameHeader.init(@TypeOf(reader), fbs.reader());
    try testing.expect(header.version.is(4));
    try testing.expect(header.version.isRequest());
    try testing.expectEqual(@as(u8, 0), header.flags);
    try testing.expectEqual(@as(i16, 215), header.stream);
    try testing.expectEqual(Opcode.Options, header.opcode);
    try testing.expectEqual(@as(u32, 0), header.body_len);
    try testing.expectEqual(@as(usize, 0), exp.len - @sizeOf(FrameHeader));
}

test "" {
    _ = @import("frames/error.zig");
    _ = @import("frames/startup.zig");
    _ = @import("frames/ready.zig");
    _ = @import("frames/auth.zig");
    _ = @import("frames/options.zig");
    _ = @import("frames/supported.zig");
    _ = @import("frames/query.zig");
    _ = @import("frames/result.zig");
    _ = @import("frames/prepare.zig");
    _ = @import("frames/execute.zig");
    _ = @import("frames/register.zig");
    _ = @import("frames/event.zig");
    _ = @import("frames/batch.zig");

    _ = @import("primitive/reader.zig");
    _ = @import("primitive/writer.zig");
}
