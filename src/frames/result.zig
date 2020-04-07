const std = @import("std");
const mem = std.mem;
const meta = std.meta;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

const ResultKind = packed enum(u32) {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    Prepared = 0x0004,
    SchemaChange = 0x0005,
};

const GlobalTableSpec = struct {
    keyspace: []const u8,
    table: []const u8,
};

const OptionID = packed enum(u16) {
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

const Option = struct {
    id: OptionID,
    value: ?Value,
};

fn readOption(comptime FramerType: type, framer: *FramerType) !Option {
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

const ColumnSpec = struct {
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
const RowsMetadata = struct {
    const Self = @This();

    paging_state: ?[]const u8,
    new_metadata_id: ?[]const u8,
    global_table_spec: ?GlobalTableSpec,
    column_specs: []ColumnSpec,

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

const Rows = struct {
    const Self = @This();

    metadata: RowsMetadata,
    data: [][]const u8,

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Rows {
        var rows = Self{
            .metadata = undefined,
            .data = undefined,
        };

        rows.metadata = try RowsMetadata.read(allocator, FramerType, framer);

        var data = std.ArrayList([]const u8).init(allocator);
        errdefer data.deinit();

        // Iterate over rows
        const rows_count = @as(usize, try framer.readInt(u32));

        var i: usize = 0;
        while (i < rows_count) : (i += 1) {
            // Read a single row
            var j: usize = 0;
            while (j < rows.metadata.column_specs.len) : (j += 1) {
                const column_spec = rows.metadata.column_specs[j];
                // TODO(vincent): can this ever be null ?
                const column_data = (try framer.readBytes()) orelse unreachable;

                _ = try data.append(column_data);
            }
        }

        rows.data = data.toOwnedSlice();

        return rows;
    }
};

const Prepared = struct {};

const Result = union(ResultKind) {
    Void: void,
    Rows: Rows,
    SetKeyspace: []const u8,
    Prepared: Prepared,
    SchemaChange: SchemaChange,
};

/// RESULT is the result to a query (QUERY, PREPARE, EXECUTE or BATCH messages).
///
/// Described in the protocol spec at §4.2.5.
const ResultFrame = struct {
    const Self = @This();

    result: Result,

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !ResultFrame {
        var frame = Self{
            .result = undefined,
        };

        const kind = @intToEnum(ResultKind, try framer.readInt(u32));

        switch (kind) {
            .Void => frame.result = Result{ .Void = .{} },
            .Rows => {
                const rows = try Rows.read(allocator, FramerType, framer);
                frame.result = Result{ .Rows = rows };
            },
            .SetKeyspace => {
                const keyspace = try framer.readString();
                frame.result = Result{ .SetKeyspace = keyspace };
            },
            .Prepared => unreachable,
            .SchemaChange => {
                const schema_change = try SchemaChange.read(allocator, FramerType, framer);
                frame.result = Result{ .SchemaChange = schema_change };
            },
        }

        return frame;
    }
};

test "result frame: void" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x9d\x08\x00\x00\x00\x04\x00\x00\x00\x01";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(&arena.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Result, data.len, framer.header);

    const frame = try ResultFrame.read(&arena.allocator, @TypeOf(framer), &framer);

    testing.expect(frame.result == .Void);
}

test "result frame: rows" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x20\x08\x00\x00\x00\xa2\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x03\x00\x00\x00\x10\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\xd7\x77\xd5\xd7\x58\xc0\x4d\x2b\x8c\xf9\xa3\x53\xfa\x8e\x6c\x96\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\x94\xa4\x7b\xb2\x8c\xf7\x43\x3d\x97\x6e\x72\x74\xb3\xfd\xd3\x31\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(&arena.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Result, data.len, framer.header);

    const frame = try ResultFrame.read(&arena.allocator, @TypeOf(framer), &framer);

    testing.expect(frame.result == .Rows);

    // check metadata

    const metadata = frame.result.Rows.metadata;
    testing.expect(metadata.paging_state == null);
    testing.expect(metadata.new_metadata_id == null);
    testing.expectEqualString("foobar", metadata.global_table_spec.?.keyspace);
    testing.expectEqualString("user", metadata.global_table_spec.?.table);
    testing.expectEqual(@as(usize, 3), metadata.column_specs.len);

    const col1 = metadata.column_specs[0];
    testing.expectEqualString("id", col1.name);
    testing.expectEqual(OptionID.UUID, col1.option.id);
    const col2 = metadata.column_specs[1];
    testing.expectEqualString("age", col2.name);
    testing.expectEqual(OptionID.Tinyint, col2.option.id);
    const col3 = metadata.column_specs[2];
    testing.expectEqualString("name", col3.name);
    testing.expectEqual(OptionID.Varchar, col3.option.id);

    // check data
}

test "result frame: set keyspace" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x77\x08\x00\x00\x00\x0c\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(&arena.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Result, data.len, framer.header);

    const frame = try ResultFrame.read(&arena.allocator, @TypeOf(framer), &framer);

    testing.expect(frame.result == .SetKeyspace);
    testing.expectEqualString("foobar", frame.result.SetKeyspace);
}
