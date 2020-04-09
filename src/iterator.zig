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

            const column_spec = self.metadata.column_specs[@as(usize, i)];
            const column_data = self.rows[self.pos].slice[@as(usize, i)];

            @field(args, field_name) = try self.readType(column_spec, column_data, struct_field.field_type);
        }

        self.pos += 1;

        return true;
    }

    fn readType(self: *Self, column_spec: ColumnSpec, column_data: ColumnData, comptime Type: type) !Type {
        const type_info = @typeInfo(Type);

        switch (type_info) {
            .Array => {
                return try self.readArray(column_spec, column_data, Type);
            },
            .Int => {
                return try self.readInt(column_spec, column_data, Type);
            },
            .Pointer => |pointer| switch (pointer.size) {
                .One => {
                    return try self.readType(column_spec, column_data, @TypeOf(pointer.child));
                },
                .Slice => {
                    return try self.readSlice(column_spec, column_data, Type, @TypeOf(pointer.child));
                },
                else => @compileError("invalid pointer size " ++ @tagName(pointer.size)),
            },
            else => @compileError("field type " ++ @typeName(struct_field.field_type) ++ " not handled yet"),
        }
    }

    fn readInt(self: *Self, column_spec: ColumnSpec, column_data: ColumnData, comptime Type: type) !Type {
        var r: Type = 0;

        return r;
    }

    fn readSlice(self: *Self, column_spec: ColumnSpec, column_data: ColumnData, comptime Type: type, comptime ChildType: type) !Type {
        var slice: Type = undefined;

        switch (column_spec.option.id) {
            .Blob, .UUID, .Timeuuid => {
                slice = column_data.slice;
            },
            .Ascii, .Varchar => {
                slice = column_data.slice;
            },
            else => unreachable,
        }

        return slice;
    }

    fn readArray(self: *Self, column_spec: ColumnSpec, column_data: ColumnData, comptime Type: type) !Type {
        var array: Type = undefined;

        switch (column_spec.option.id) {
            .UUID, .Timeuuid => {
                mem.copy(u8, &array, column_data.slice[0..array.len]);
            },
            .List, .Set => {
                unreachable;
            },
            else => {
                std.debug.panic("CQL type {} can't be read into the type {}", .{ std.meta.tagName(column_spec.option.id), @typeName(Type) });
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
        name: []const u8,
    };
    var row: Row = undefined;

    row.age = 1;
    row.name = undefined;

    while (try iterator.scan(&row)) {
        std.debug.warn("{x}\n", .{row});
    }
}
