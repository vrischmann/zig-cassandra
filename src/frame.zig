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

pub const FrameHeader = packed struct {
    version: ProtocolVersion,
    flags: u8,
    stream: i16,
    opcode: Opcode,
    body_len: u32,
};

pub const RawFrame = struct {
    header: FrameHeader,
    body: []const u8,

    pub fn deinit(self: @This(), allocator: *mem.Allocator) void {
        allocator.free(self.body);
    }
};

pub fn RawFrameReader(comptime InStreamType: type) type {
    return struct {
        const Self = @This();

        allocator: *mem.Allocator,

        in_stream: InStreamType,
        deserializer: io.Deserializer(.Big, io.Packing.Bit, InStreamType),

        pub fn init(allocator: *mem.Allocator, in: InStreamType) Self {
            return Self{
                .allocator = allocator,
                .in_stream = in,
                .deserializer = io.deserializer(std.builtin.Endian.Big, io.Packing.Bit, in),
            };
        }

        pub fn read(self: *Self) !RawFrame {
            const header = try self.deserializer.deserialize(FrameHeader);

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

pub fn RawFrameWriter(comptime OutStreamType: type) type {
    return struct {
        const Self = @This();

        out_stream: OutStreamType,
        serializer: io.Serializer(std.builtin.Endian.Big, io.Packing.Bit, OutStreamType),

        pub fn init(out: OutStreamType) Self {
            return Self{
                .out_stream = out,
                .serializer = io.serializer(std.builtin.Endian.Big, io.Packing.Bit, out),
            };
        }

        pub fn write(self: *Self, raw_frame: RawFrame) !void {
            _ = try self.serializer.serialize(raw_frame.header);
            _ = try self.out_stream.writeAll(raw_frame.body);
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

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !Option {
        var option = Option{
            .id = @intToEnum(OptionID, try pr.readInt(u16)),
            .value = null,
        };

        switch (option.id) {
            .Custom => {
                option.value = Value{ .Set = try pr.readString(allocator) };
            },
            .List, .Map, .Set => {
                option.value = try pr.readValue(allocator);
            },
            .UDT, .Tuple => {
                std.debug.panic("option {} not implemented yet", .{option.id});
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

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader, has_global_table_spec: bool) !Self {
        var spec = Self{
            .keyspace = null,
            .table = null,
            .name = undefined,
            .option = undefined,
        };

        if (!has_global_table_spec) {
            spec.keyspace = try pr.readString(allocator);
            spec.table = try pr.readString(allocator);
        }
        spec.name = try pr.readString(allocator);
        spec.option = try Option.read(allocator, pr);

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
            metadata.paging_state = try pr.readBytes(allocator);
        }
        // Only valid with Protocol v5
        if (flags & FlagMetadataChanged == FlagMetadataChanged) {
            metadata.new_metadata_id = try pr.readShortBytes(allocator);
        }

        if (flags & FlagNoMetadata == FlagNoMetadata) {
            return metadata;
        }

        if (flags & FlagGlobalTablesSpec == FlagGlobalTablesSpec) {
            const spec = GlobalTableSpec{
                .keyspace = try pr.readString(allocator),
                .table = try pr.readString(allocator),
            };
            metadata.global_table_spec = spec;
        }

        var column_specs = try allocator.alloc(ColumnSpec, columns_count);
        var i: usize = 0;
        while (i < columns_count) : (i += 1) {
            column_specs[i] = try ColumnSpec.read(allocator, pr, metadata.global_table_spec != null);
        }
        metadata.column_specs = column_specs;

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

/// PreparedMetadata in the protocol spec at §4.2.5.4.
pub const PreparedMetadata = struct {
    const Self = @This();

    global_table_spec: ?GlobalTableSpec,
    pk_indexes: []const u16,
    column_specs: []const ColumnSpec,

    const FlagGlobalTablesSpec = 0x0001;

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !Self {
        var metadata = Self{
            .global_table_spec = null,
            .pk_indexes = undefined,
            .column_specs = undefined,
        };

        const flags = try pr.readInt(u32);
        const columns_count = @as(usize, try pr.readInt(u32));
        const pk_count = @as(usize, try pr.readInt(u32));

        // Read the partition key indexes

        var pk_indexes = try allocator.alloc(u16, pk_count);
        errdefer allocator.free(pk_indexes);

        var i: usize = 0;
        while (i < pk_count) : (i += 1) {
            pk_indexes[i] = try pr.readInt(u16);
        }
        metadata.pk_indexes = pk_indexes;

        // Next are the table spec and column spec

        if (flags & FlagGlobalTablesSpec == FlagGlobalTablesSpec) {
            const spec = GlobalTableSpec{
                .keyspace = try pr.readString(allocator),
                .table = try pr.readString(allocator),
            };
            metadata.global_table_spec = spec;
        }

        // Read the column specs

        var column_specs = try allocator.alloc(ColumnSpec, columns_count);
        errdefer allocator.free(column_specs);

        i = 0;
        while (i < columns_count) : (i += 1) {
            column_specs[i] = try ColumnSpec.read(allocator, pr, metadata.global_table_spec != null);
        }
        metadata.column_specs = column_specs;

        return metadata;
    }
};

pub const TopologyChangeType = enum {
    NEW_NODE,
    REMOVED_NODE,
};

pub const StatusChangeType = enum {
    UP,
    DOWN,
};

pub const SchemaChangeType = enum {
    CREATED,
    UPDATED,
    DROPPED,
};

pub const SchemaChangeTarget = enum {
    KEYSPACE,
    TABLE,
    TYPE,
    FUNCTION,
    AGGREGATE,
};

pub const SchemaChangeOptions = struct {
    keyspace: []const u8,
    object_name: []const u8,
    arguments: ?[]const []const u8,

    pub fn init() SchemaChangeOptions {
        return SchemaChangeOptions{
            .keyspace = &[_]u8{},
            .object_name = &[_]u8{},
            .arguments = null,
        };
    }
};

pub const TopologyChange = struct {
    type: TopologyChangeType,
    node_address: net.Address,
};

pub const StatusChange = struct {
    type: StatusChangeType,
    node_address: net.Address,
};

pub const SchemaChange = struct {
    const Self = @This();

    type: SchemaChangeType,
    target: SchemaChangeTarget,
    options: SchemaChangeOptions,

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !Self {
        var change = Self{
            .type = undefined,
            .target = undefined,
            .options = undefined,
        };

        change.type = std.meta.stringToEnum(SchemaChangeType, (try pr.readString(allocator))) orelse return error.InvalidSchemaChangeType;
        change.target = std.meta.stringToEnum(SchemaChangeTarget, (try pr.readString(allocator))) orelse return error.InvalidSchemaChangeTarget;

        change.options = SchemaChangeOptions.init();

        switch (change.target) {
            .KEYSPACE => {
                change.options.keyspace = try pr.readString(allocator);
            },
            .TABLE, .TYPE => {
                change.options.keyspace = try pr.readString(allocator);
                change.options.object_name = try pr.readString(allocator);
            },
            .FUNCTION, .AGGREGATE => {
                change.options.keyspace = try pr.readString(allocator);
                change.options.object_name = try pr.readString(allocator);
                change.options.arguments = try pr.readStringList(allocator);
            },
        }

        return change;
    }
};

pub const EventType = enum {
    TOPOLOGY_CHANGE,
    STATUS_CHANGE,
    SCHEMA_CHANGE,
};

pub const Event = union(EventType) {
    TOPOLOGY_CHANGE: TopologyChange,
    STATUS_CHANGE: StatusChange,
    SCHEMA_CHANGE: SchemaChange,
};

pub fn checkHeader(opcode: Opcode, data_len: usize, header: FrameHeader) void {
    // We can only use v4 for now
    testing.expect(header.version.is(4));
    // Don't care about the flags here
    // Don't care about the stream
    testing.expectEqual(opcode, header.opcode);
    testing.expectEqual(@as(usize, header.body_len), data_len - @sizeOf(FrameHeader));
}

test "frame header: read and write" {
    const exp = "\x04\x00\x00\xd7\x05\x00\x00\x00\x00";
    var fbs = io.fixedBufferStream(exp);

    // deserialize the header

    var deserializer = io.deserializer(.Big, io.Packing.Bit, fbs.inStream());
    const header = try deserializer.deserialize(FrameHeader);
    testing.expect(header.version.is(4));
    testing.expect(header.version.is_request());
    testing.expectEqual(@as(u8, 0), header.flags);
    testing.expectEqual(@as(i16, 215), header.stream);
    testing.expectEqual(Opcode.Options, header.opcode);
    testing.expectEqual(@as(u32, 0), header.body_len);
    testing.expectEqual(@as(usize, 0), exp.len - @sizeOf(FrameHeader));

    // reserialize it

    var new_buf: [32]u8 = undefined;
    var new_fbs = io.fixedBufferStream(&new_buf);

    var serializer = io.serializer(.Big, io.Packing.Bit, new_fbs.outStream());
    _ = try serializer.serialize(header);

    testing.expectEqualSlices(u8, exp, new_fbs.getWritten());
}

test "schema change options" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var options = SchemaChangeOptions.init();

    options.keyspace = try mem.dupe(&arena.allocator, u8, "foobar");
    options.object_name = try mem.dupe(&arena.allocator, u8, "barbaz");
    var arguments = try arena.allocator.alloc([]const u8, 4);
    var i: usize = 0;
    while (i < arguments.len) : (i += 1) {
        arguments[i] = try mem.dupe(&arena.allocator, u8, "hello");
    }
    options.arguments = arguments;
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
