const std = @import("std");
const mem = std.mem;

usingnamespace @import("primitive_types.zig");

const PrimitiveReader = @import("primitive/reader.zig").PrimitiveReader;

const testing = @import("testing.zig");

pub const GlobalTableSpec = struct {
    keyspace: []const u8,
    table: []const u8,
};

fn readOptionID(pr: *PrimitiveReader) !OptionID {
    return @intToEnum(OptionID, try pr.readInt(u16));
}

pub const ColumnSpec = struct {
    const Self = @This();

    option: OptionID,

    keyspace: ?[]const u8 = null,
    table: ?[]const u8 = null,
    name: []const u8 = "",

    // TODO(vincent): not a fan of this but for now it's fine.
    listset_element_type_option: ?OptionID = null,
    map_key_type_option: ?OptionID = null,
    map_value_type_option: ?OptionID = null,
    custom_class_name: ?[]const u8 = null,

    pub fn deinit(self: Self, allocator: *mem.Allocator) void {
        if (self.keyspace) |str| allocator.free(str);
        if (self.table) |str| allocator.free(str);
        allocator.free(self.name);

        if (self.custom_class_name) |str| allocator.free(str);
    }

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader, has_global_table_spec: bool) !Self {
        var spec = Self{
            .keyspace = null,
            .table = null,
            .name = undefined,
            .option = undefined,
            .listset_element_type_option = null,
            .map_key_type_option = null,
            .map_value_type_option = null,
            .custom_class_name = null,
        };

        if (!has_global_table_spec) {
            spec.keyspace = try pr.readString(allocator);
            spec.table = try pr.readString(allocator);
        }
        spec.name = try pr.readString(allocator);
        spec.option = try readOptionID(pr);

        switch (spec.option) {
            .Tuple => unreachable,
            .UDT => unreachable,
            .Custom => {
                spec.custom_class_name = try pr.readString(allocator);
            },
            .List, .Set => {
                const option = try readOptionID(pr);
                spec.listset_element_type_option = option;
                if (option == .Custom) {
                    spec.custom_class_name = try pr.readString(allocator);
                }
            },
            .Map => {
                spec.map_key_type_option = try readOptionID(pr);
                spec.map_value_type_option = try readOptionID(pr);
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

    /// Store the column count as well as the column specs because
    /// with FlagNoMetadata the specs are empty
    columns_count: usize,
    column_specs: []const ColumnSpec,

    const FlagGlobalTablesSpec = 0x0001;
    const FlagHasMorePages = 0x0002;
    const FlagNoMetadata = 0x0004;
    const FlagMetadataChanged = 0x0008;

    pub fn deinit(self: *Self, allocator: *mem.Allocator) void {
        if (self.paging_state) |ps| allocator.free(ps);
        if (self.new_metadata_id) |id| allocator.free(id);
        if (self.global_table_spec) |spec| {
            allocator.free(spec.keyspace);
            allocator.free(spec.table);
        }
        for (self.column_specs) |spec| {
            spec.deinit(allocator);
        }
        allocator.free(self.column_specs);
    }

    pub fn read(allocator: *mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        var metadata = Self{
            .paging_state = null,
            .new_metadata_id = null,
            .global_table_spec = null,
            .columns_count = 0,
            .column_specs = undefined,
        };

        const flags = try pr.readInt(u32);
        metadata.columns_count = @as(usize, try pr.readInt(u32));

        if (flags & FlagHasMorePages == FlagHasMorePages) {
            metadata.paging_state = try pr.readBytes(allocator);
        }
        if (protocol_version.is(5)) {
            if (flags & FlagMetadataChanged == FlagMetadataChanged) {
                metadata.new_metadata_id = try pr.readShortBytes(allocator);
            }
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

        var column_specs = try allocator.alloc(ColumnSpec, metadata.columns_count);
        var i: usize = 0;
        while (i < metadata.columns_count) : (i += 1) {
            column_specs[i] = try ColumnSpec.read(allocator, pr, metadata.global_table_spec != null);
        }
        metadata.column_specs = column_specs;

        return metadata;
    }
};

/// PreparedMetadata in the protocol spec at §4.2.5.4.
pub const PreparedMetadata = struct {
    const Self = @This();

    global_table_spec: ?GlobalTableSpec,
    pk_indexes: []const u16,
    column_specs: []const ColumnSpec,

    const FlagGlobalTablesSpec = 0x0001;
    const FlagNoMetadata = 0x0004;

    pub fn deinit(self: *const Self, allocator: *mem.Allocator) void {
        if (self.global_table_spec) |spec| {
            allocator.free(spec.keyspace);
            allocator.free(spec.table);
        }
        allocator.free(self.pk_indexes);
        for (self.column_specs) |spec| {
            spec.deinit(allocator);
        }
        allocator.free(self.column_specs);
    }

    pub fn read(allocator: *mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        var metadata = Self{
            .global_table_spec = null,
            .pk_indexes = undefined,
            .column_specs = undefined,
        };

        const flags = try pr.readInt(u32);
        const columns_count = @as(usize, try pr.readInt(u32));

        if (protocol_version.isAtLeast(4)) {
            const pk_count = @as(usize, try pr.readInt(u32));

            // Read the partition key indexes

            var pk_indexes = try allocator.alloc(u16, pk_count);
            errdefer allocator.free(pk_indexes);

            var i: usize = 0;
            while (i < pk_count) : (i += 1) {
                pk_indexes[i] = try pr.readInt(u16);
            }
            metadata.pk_indexes = pk_indexes;
        }

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

        var i: usize = 0;
        while (i < columns_count) : (i += 1) {
            column_specs[i] = try ColumnSpec.read(allocator, pr, metadata.global_table_spec != null);
        }
        metadata.column_specs = column_specs;

        return metadata;
    }
};

test "column spec: deinit" {
    const allocator = testing.allocator;

    const column_spec = ColumnSpec{
        .keyspace = try mem.dupe(allocator, u8, "keyspace"),
        .table = try mem.dupe(allocator, u8, "table"),
        .name = try mem.dupe(allocator, u8, "name"),
        .option = .Set,
        .listset_element_type_option = .Inet,
        .map_key_type_option = .Varchar,
        .map_value_type_option = .Varint,
        .custom_class_name = try mem.dupe(allocator, u8, "custom_class_name"),
    };

    column_spec.deinit(allocator);
}

test "prepared metadata: deinit" {
    const allocator = testing.allocator;

    const column_spec = ColumnSpec{
        .keyspace = try mem.dupe(allocator, u8, "keyspace"),
        .table = try mem.dupe(allocator, u8, "table"),
        .name = try mem.dupe(allocator, u8, "name"),
        .option = .Set,
        .listset_element_type_option = .Inet,
        .map_key_type_option = .Varchar,
        .map_value_type_option = .Varint,
        .custom_class_name = try mem.dupe(allocator, u8, "custom_class_name"),
    };

    var metadata: PreparedMetadata = undefined;
    metadata.global_table_spec = GlobalTableSpec{
        .keyspace = try mem.dupe(allocator, u8, "global_keyspace"),
        .table = try mem.dupe(allocator, u8, "global_table"),
    };
    metadata.pk_indexes = try mem.dupe(allocator, u16, &[_]u16{ 0xde, 0xad, 0xbe, 0xef });
    metadata.column_specs = try mem.dupe(allocator, ColumnSpec, &[_]ColumnSpec{column_spec});

    metadata.deinit(allocator);
}

test "rows metadata: deinit" {
    const allocator = testing.allocator;

    const column_spec = ColumnSpec{
        .keyspace = try mem.dupe(allocator, u8, "keyspace"),
        .table = try mem.dupe(allocator, u8, "table"),
        .name = try mem.dupe(allocator, u8, "name"),
        .option = .Set,
        .listset_element_type_option = .Inet,
        .map_key_type_option = .Varchar,
        .map_value_type_option = .Varint,
        .custom_class_name = try mem.dupe(allocator, u8, "custom_class_name"),
    };

    var metadata: RowsMetadata = undefined;
    metadata.paging_state = try mem.dupe(allocator, u8, "\xbb\xbc\xde\xfe");
    metadata.new_metadata_id = try mem.dupe(allocator, u8, "\xac\xbd\xde\xad");
    metadata.global_table_spec = GlobalTableSpec{
        .keyspace = try mem.dupe(allocator, u8, "global_keyspace"),
        .table = try mem.dupe(allocator, u8, "global_table"),
    };
    metadata.column_specs = try mem.dupe(allocator, ColumnSpec, &[_]ColumnSpec{column_spec});

    metadata.deinit(allocator);
}
