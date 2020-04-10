const std = @import("std");
const io = std.io;
const heap = std.heap;
const mem = std.mem;
const os = std.os;
const net = std.net;

usingnamespace @import("primitive_types.zig");

// Ordered by Opcode

usingnamespace @import("frames/error.zig");
usingnamespace @import("frames/startup.zig");
usingnamespace @import("frames/ready.zig");
usingnamespace @import("frames/auth.zig");
usingnamespace @import("frames/options.zig");
usingnamespace @import("frames/supported.zig");
usingnamespace @import("frames/query.zig");
usingnamespace @import("frames/result.zig");
usingnamespace @import("frames/prepare.zig");
usingnamespace @import("frames/execute.zig");
usingnamespace @import("frames/register.zig");
usingnamespace @import("frames/event.zig");
usingnamespace @import("frames/batch.zig");

const testing = @import("testing.zig");

pub const FrameHeader = packed struct {
    version: ProtocolVersion,
    flags: u8,
    stream: i16,
    opcode: Opcode,
    body_len: u32,

    pub fn write(self: @This(), comptime OutStreamType: type, out: OutStreamType) !void {
        var serializer = std.io.serializer(std.builtin.Endian.Big, std.io.Packing.Bit, out);
        _ = try serializer.serialize(self);
    }

    pub fn read(comptime InStreamType: type, in: InStreamType) !FrameHeader {
        var deserializer = std.io.deserializer(std.builtin.Endian.Big, std.io.Packing.Bit, in);
        return deserializer.deserialize(FrameHeader);
    }
};

pub const RawFrame = struct {
    header: FrameHeader,
    body: []const u8,
};

pub fn RawFrameReader(comptime InStreamType: type) type {
    return struct {
        const Self = @This();

        allocator: *mem.Allocator,

        in_stream: InStreamType,

        pub fn init(allocator: *mem.Allocator, in: InStreamType) Self {
            return Self{
                .allocator = allocator,
                .in_stream = in,
            };
        }

        pub fn read(self: *Self) !RawFrame {
            const header = try FrameHeader.read(InStreamType, self.in_stream);

            const len = @as(usize, header.body_len);

            const body = try self.allocator.alloc(u8, len);
            const n_read = try self.in_stream.readAll(body);
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

pub const OptionID = packed enum(u16) {
    Custom = 0x0000,
    Ascii = 0x0001,
    Bigint = 0x0002,
    Blob = 0x0003,
    Boolean = 0x0004,
    Counter = 0x0005,
    Decimal = 0x0006,
    Double = 0x0007,
    Float = 0x0008,
    Int = 0x0009,
    Timestamp = 0x000B,
    UUID = 0x000C,
    Varchar = 0x000D,
    Varint = 0x000E,
    Timeuuid = 0x000F,
    Inet = 0x0010,
    Date = 0x0011,
    Time = 0x0012,
    Smallint = 0x0013,
    Tinyint = 0x0014,
    Duration = 0x0015,
    List = 0x0020,
    Map = 0x0021,
    Set = 0x0022,
    UDT = 0x0030,
    Tuple = 0x0031,
};

pub const Option = struct {
    id: OptionID,
    value: ?Value,

    pub fn read(pr: *PrimitiveReader) !Option {
        var option = Option{
            .id = @intToEnum(OptionID, try pr.readInt(u16)),
            .value = null,
        };

        switch (option.id) {
            .Custom, .List, .Map, .Set, .UDT, .Tuple => {
                option.value = try pr.readValue();
            },
            else => {},
        }

        return option;
    }
};

pub const GlobalTableSpec = struct {
    keyspace: []const u8,
    table: []const u8,
};

pub const ColumnSpec = struct {
    const Self = @This();

    keyspace: ?[]const u8,
    table: ?[]const u8,
    name: []const u8,

    option: Option,

    // TODO(vincent): Custom types

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader, has_global_table_spec: bool) !Self {
        var spec = Self{
            .keyspace = null,
            .table = null,
            .name = undefined,
            .option = undefined,
        };

        if (!has_global_table_spec) {
            spec.keyspace = try pr.readString();
            spec.table = try pr.readString();
        }
        spec.name = try pr.readString();
        spec.option = try Option.read(pr);

        switch (spec.option.id) {
            .Custom => {
                // TODO(vincent): test this
                unreachable;
            },
            .List, .Set => {
                // TODO(vincent): test this
                unreachable;
            },
            .Map => {
                // TODO(vincent): test this
                unreachable;
            },
            .UDT => {
                // TODO(vincent): test this
                unreachable;
            },
            .Tuple => {
                // TODO(vincent): test this
                unreachable;
            },
            else => {},
        }

        return spec;
    }
};

/// Described in the protocol spec at §4.2.5.2.
pub const RowsMetadata = struct {
    const Self = @This();

    paging_state: ?[]const u8,
    new_metadata_id: ?[]const u8,
    global_table_spec: ?GlobalTableSpec,
    column_specs: []const ColumnSpec,

    const FlagGlobalTablesSpec = 0x0001;
    const FlagHasMorePages = 0x0002;
    const FlagNoMetadata = 0x0004;
    const FlagMetadataChanged = 0x0008;

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !Self {
        var metadata = Self{
            .paging_state = null,
            .new_metadata_id = null,
            .global_table_spec = null,
            .column_specs = undefined,
        };

        const flags = try pr.readInt(u32);
        const columns_count = @as(usize, try pr.readInt(u32));

        if (flags & FlagHasMorePages == FlagHasMorePages) {
            metadata.paging_state = try pr.readBytes();
        }
        // Only valid with Protocol v5
        if (flags & FlagMetadataChanged == FlagMetadataChanged) {
            metadata.new_metadata_id = try pr.readShortBytes();
        }

        if (flags & FlagNoMetadata == 0) {
            if (flags & FlagGlobalTablesSpec == FlagGlobalTablesSpec) {
                const spec = GlobalTableSpec{
                    .keyspace = try pr.readString(),
                    .table = try pr.readString(),
                };
                metadata.global_table_spec = spec;
            }

            var column_specs = std.ArrayList(ColumnSpec).init(allocator);
            var i: usize = 0;
            while (i < columns_count) : (i += 1) {
                const column_spec = try ColumnSpec.read(allocator, pr, metadata.global_table_spec != null);
                _ = try column_specs.append(column_spec);
            }

            metadata.column_specs = column_specs.toOwnedSlice();
        }

        return metadata;
    }
};

// TODO(vincent): do we want to keep these wrapper types ?

/// ColumnData is a wrapper around a slice of bytes.
pub const ColumnData = struct {
    slice: []const u8,
};

/// RowData is a wrapper around a slice of ColumnData.
pub const RowData = struct {
    slice: []const ColumnData,
};

pub fn checkHeader(opcode: Opcode, data_len: usize, header: FrameHeader) void {
    // We can only use v4 for now
    testing.expectEqual(ProtocolVersion.V4, header.version);
    // Don't care about the flags here
    // Don't care about the stream
    testing.expectEqual(opcode, header.opcode);
    testing.expectEqual(@as(usize, header.body_len), data_len - @sizeOf(FrameHeader));
}

test "frame header: read and write" {
    const exp = "\x04\x00\x00\xd7\x05\x00\x00\x00\x00";
    var fbs = std.io.fixedBufferStream(exp);

    // deserialize the header

    var header = try FrameHeader.read(@TypeOf(fbs.inStream()), fbs.inStream());
    testing.expectEqual(ProtocolVersion.V4, header.version);
    testing.expectEqual(@as(u8, 0), header.flags);
    testing.expectEqual(@as(i16, 215), header.stream);
    testing.expectEqual(Opcode.Options, header.opcode);
    testing.expectEqual(@as(u32, 0), header.body_len);
    testing.expectEqual(@as(usize, 0), exp.len - @sizeOf(FrameHeader));

    // reserialize it

    var new_buf: [32]u8 = undefined;
    var new_fbs = std.io.fixedBufferStream(&new_buf);

    _ = try header.write(@TypeOf(new_fbs.outStream()), new_fbs.outStream());
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
}
