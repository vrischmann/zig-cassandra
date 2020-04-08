const std = @import("std");
const mem = std.mem;

usingnamespace @import("primitive_types.zig");
usingnamespace @import("frames.zig");

const Iterator = struct {
    const Self = @This();

    metadata: RowsMetadata,
    rows: []const RowData,

    pub fn deinit(self: Self) void {}

    pub fn init(metadata: RowsMetadata, rows: []const RowData) Self {
        return Self{
            .metadata = metadata,
            .rows = rows,
        };
    }

    pub fn scan(args: var) !void {}
};

fn getTestRowsMetadata() RowsMetadata {
    const column_specs = &[_]ColumnSpec{
        ColumnSpec{
            .keyspace = null,
            .table = null,
            .name = "id",
            .option = Option{
                .id = .UUID,
                .value = null,
            },
        },
        ColumnSpec{
            .keyspace = null,
            .table = null,
            .name = "age",
            .option = Option{
                .id = .Tinyint,
                .value = null,
            },
        },
        ColumnSpec{
            .keyspace = null,
            .table = null,
            .name = "name",
            .option = Option{
                .id = .Varchar,
                .value = null,
            },
        },
    };

    return RowsMetadata{
        .paging_state = null,
        .new_metadata_id = null,
        .global_table_spec = GlobalTableSpec{
            .keyspace = "foobar",
            .table = "user",
        },
        .column_specs = column_specs,
    };
}

test "iterator scan" {
    const metadata = getTestRowsMetadata();

    const row_data = &[_]RowData{
        RowData{
            .data = &[_]ColumnData{
                ColumnData{ .data = "\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8" },
                ColumnData{ .data = "\x00" },
                ColumnData{ .data = "\x56\x69\x6e\x63\x65\x6e\x74\x30" },
            },
        },
    };

    var iterator = Iterator.init(metadata, row_data);
}
