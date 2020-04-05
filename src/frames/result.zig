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

const NativeType = packed enum(u16) {
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

const NativeValue = union(NativeType) {
    Custom: []const u8,
    Ascii: void,
    Bigint: void,
    Blob: void,
    Boolean: void,
    Counter: void,
    Decimal: void,
    Double: void,
    Float: void,
    Int: void,
    Timestamp: void,
    UUID: void,
    Varchar: void,
    Varint: void,
    Timeuuid: void,
    Inet: void,
    Date: void,
    Time: void,
    Smallint: void,
    Tinyint: void,
    Duration: void,
    List: Option,
    Map: struct {
        key_type: Option,
        value_type: Option,
    },
    Set: Option,
    UDT: void,
    Tuple: void,
};

const ColumnSpec = struct {
    const Self = @This();

    keyspace: ?[]const u8,
    table: ?[]const u8,
    name: []const u8,
    native_type: NativeType,

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType, has_global_table_spec: bool) !Self {
        var spec = Self{
            .keyspace = null,
            .table = null,
            .name = undefined,
            .native_type = undefined,
        };

        if (!has_global_table_spec) {
            spec.keyspace = try framer.readString();
            spec.table = try framer.readString();
        }
        spec.name = try framer.readString();
        spec.native_type = @intToEnum(NativeType, try framer.readInt(u16));

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

    rows_metadata: RowsMetadata,

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Rows {
        var rows = Self{
            .rows_metadata = undefined,
        };

        rows.rows_metadata = try RowsMetadata.read(allocator, FramerType, framer);

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

    const data = "\x84\x00\x00\xa7\x08\x00\x00\x00\x89\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x02\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x03\x00\x00\x00\x10\x70\xb7\xe5\x04\x81\x65\x4e\xdc\x81\x76\xdb\x24\x61\xc7\x74\x3e\x00\x00\x00\x07\x76\x69\x6e\x63\x65\x6e\x74\x00\x00\x00\x10\x4f\xd0\x7c\x5b\x14\x0c\x4d\x0c\xa3\xcf\xfd\xe4\xfe\x46\xa8\xf9\x00\x00\x00\x07\x76\x69\x6e\x63\x65\x6e\x74\x00\x00\x00\x10\x20\x33\x45\x82\xde\xcc\x4b\xee\xb3\x1e\x2f\x6d\xb9\xf2\x7e\x84\x00\x00\x00\x07\x76\x69\x6e\x63\x65\x6e\x74";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(&arena.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Result, data.len, framer.header);

    const frame = try ResultFrame.read(&arena.allocator, @TypeOf(framer), &framer);

    testing.expect(frame.result == .Rows);

    const metadata = frame.result.Rows.rows_metadata;
    testing.expect(metadata.paging_state == null);
    testing.expect(metadata.new_metadata_id == null);

    testing.expectEqualString("foobar", metadata.global_table_spec.?.keyspace);
    testing.expectEqualString("user", metadata.global_table_spec.?.table);
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
