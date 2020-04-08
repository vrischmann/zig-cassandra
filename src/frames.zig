const std = @import("std");
const mem = std.mem;

usingnamespace @import("frames/auth.zig");
usingnamespace @import("frames/batch.zig");
usingnamespace @import("frames/error.zig");
usingnamespace @import("frames/event.zig");
usingnamespace @import("frames/execute.zig");
usingnamespace @import("frames/options.zig");
usingnamespace @import("frames/prepare.zig");
usingnamespace @import("frames/query.zig");
usingnamespace @import("frames/ready.zig");
usingnamespace @import("frames/register.zig");
usingnamespace @import("frames/startup.zig");
usingnamespace @import("frames/supported.zig");

const Value = @import("value.zig").Value;

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
};

pub const GlobalTableSpec = struct {
    keyspace: []const u8,
    table: []const u8,
};

pub fn readOption(comptime FramerType: type, framer: *FramerType) !Option {
    var option = Option{
        .id = @intToEnum(OptionID, try framer.readInt(u16)),
        .value = null,
    };

    switch (option.id) {
        .Custom, .List, .Map, .Set, .UDT, .Tuple => {
            option.value = try framer.readValue();
        },
        else => {},
    }

    return option;
}

pub const ColumnSpec = struct {
    const Self = @This();

    keyspace: ?[]const u8,
    table: ?[]const u8,
    name: []const u8,

    option: Option,

    // TODO(vincent): Custom types

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType, has_global_table_spec: bool) !Self {
        var spec = Self{
            .keyspace = null,
            .table = null,
            .name = undefined,
            .option = undefined,
        };

        if (!has_global_table_spec) {
            spec.keyspace = try framer.readString();
            spec.table = try framer.readString();
        }
        spec.name = try framer.readString();
        spec.option = try readOption(FramerType, framer);

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

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var metadata = Self{
            .paging_state = null,
            .new_metadata_id = null,
            .global_table_spec = null,
            .column_specs = undefined,
        };

        const flags = try framer.readInt(u32);
        const columns_count = @as(usize, try framer.readInt(u32));

        if (flags & FlagHasMorePages == FlagHasMorePages) {
            metadata.paging_state = try framer.readBytes();
        }
        // Only valid with Protocol v5
        if (flags & FlagMetadataChanged == FlagMetadataChanged) {
            metadata.new_metadata_id = try framer.readShortBytes();
        }

        if (flags & FlagNoMetadata == 0) {
            if (flags & FlagGlobalTablesSpec == FlagGlobalTablesSpec) {
                const spec = GlobalTableSpec{
                    .keyspace = try framer.readString(),
                    .table = try framer.readString(),
                };
                metadata.global_table_spec = spec;
            }

            var column_specs = std.ArrayList(ColumnSpec).init(allocator);
            var i: usize = 0;
            while (i < columns_count) : (i += 1) {
                const column_spec = try ColumnSpec.read(allocator, FramerType, framer, metadata.global_table_spec != null);
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

test "" {
    _ = @import("frames/startup.zig");
    _ = @import("frames/options.zig");
    _ = @import("frames/query.zig");
    _ = @import("frames/prepare.zig");
    _ = @import("frames/execute.zig");
    _ = @import("frames/batch.zig");
    _ = @import("frames/register.zig");
    _ = @import("frames/error.zig");
    _ = @import("frames/ready.zig");
    _ = @import("frames/supported.zig");
    _ = @import("frames/result.zig");
    _ = @import("frames/event.zig");
    _ = @import("frames/auth.zig");
}
