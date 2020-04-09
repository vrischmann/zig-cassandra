const std = @import("std");
const mem = std.mem;

usingnamespace @import("primitive_types.zig");
usingnamespace @import("frames.zig");

const Iterator = struct {
    const Self = @This();

    metadata: RowsMetadata,
    rows: []const RowData,

    pos: usize,

    pub fn deinit(self: Self) void {}

    pub fn init(metadata: RowsMetadata, rows: []const RowData) Self {
        return Self{
            .metadata = metadata,
            .rows = rows,
            .pos = 0,
        };
    }

    pub fn scan(self: *Self, args: var) !bool {
        const child = switch (@typeInfo(@TypeOf(args))) {
            .Pointer => |info| info.child,
            else => @compileError("Expected a pointer to a tuple or struct, found " ++ @typeName(@TypeOf(args))),
        };
        if (@typeInfo(child) != .Struct) {
            @compileError("Expected tuple or struct argument, found " ++ @typeName(child));
        }

        if (self.pos >= self.rows.len) {
            return false;
        }

        inline for (@typeInfo(child).Struct.fields) |struct_field, i| {
            const field_type_info = @typeInfo(struct_field.field_type);
            const field_name = struct_field.name;

            switch (field_type_info) {
                .Array => |array| {
                    @field(args, field_name) = try self.readArray(i, struct_field.field_type);
                },
                .Int => |int| {},
                .Pointer => |pointer| {},
                else => @compileError("field type " ++ @typeName(struct_field.field_type) ++ " not handled yet"),
            }
        }

        self.pos += 1;

        return true;
    }

    fn readArray(self: *Self, comptime column: comptime_int, comptime Type: type) !Type {
        const column_data = self.rows[self.pos].slice[@as(usize, column)];
        const column_spec = self.metadata.column_specs[@as(usize, column)];

        var array: Type = undefined;

        switch (column_spec.option.id) {
            // .Ascii, .Varchar => {
            //     if (std.meta.Elem(Type) != u8) {
            //         @compileError("unable to read an Ascii string in type " ++ @typeName(Type));
            //     }
            //     if (array.len < column_data.slice.len) {
            //         return error.ArrayTooSmall;
            //     }
            //     mem.copy(u8, &array, column_data.slice);
            // },
            .UUID, .Timeuuid => {
                if (array.len < column_data.slice.len) {
                    return error.ArrayTooSmall;
                }
                mem.copy(u8, &array, column_data.slice);
                std.debug.warn("slice: {x}\n", .{array});
            },
            .List, .Set => {
                unreachable;
            },
            else => {
                unreachable;
                // std.debug.panic("CQL type " ++ @tagName(column_spec.option.id) ++ " is not implemented");
            },
        }

        return array;
    }
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
            .slice = &[_]ColumnData{
                ColumnData{ .slice = "\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8" },
                ColumnData{ .slice = "\x00" },
                ColumnData{ .slice = "\x56\x69\x6e\x63\x65\x6e\x74\x30" },
            },
        },
    };

    var iterator = Iterator.init(metadata, row_data);

    const Row = struct {
        id: [16]u8,
        age: u8,
        name: [64]u8,
    };
    var row: Row = undefined;

    row.age = 1;
    row.name = undefined;

    while (try iterator.scan(&row)) {
        std.debug.warn("{x}\n", .{row});
    }
}
