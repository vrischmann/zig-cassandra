const std = @import("std");
const heap = std.heap;
const mem = std.mem;

const Framer = @import("framer.zig").Framer;
usingnamespace @import("primitive_types.zig");
usingnamespace @import("frames.zig");

const testing = @import("testing.zig");

const Iterator = struct {
    const Self = @This();

    arena: heap.ArenaAllocator,
    metadata: RowsMetadata,
    rows: []const RowData,

    pos: usize,

    pub fn deinit(self: Self) void {
        self.arena.deinit();
    }

    pub fn init(allocator: *mem.Allocator, metadata: RowsMetadata, rows: []const RowData) Self {
        return Self{
            .arena = heap.ArenaAllocator.init(allocator),
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
            @compileError("Expected tuple or struct argument, found " ++ @typeName(child) ++ " of type " ++ @tagName(@typeInfo(child)));
        }

        if (self.pos >= self.rows.len) {
            return false;
        }

        if (self.metadata.column_specs.len > @typeInfo(child).Struct.fields.len) {
            return error.NotEnoughFieldsInStruct;
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

        // TODO(vincent): if the struct is packed we could maybe read stuff directly
        // TODO(vincent): handle packed enum
        // TODO(vincent): handle union

        switch (type_info) {
            .Int => return try self.readInt(column_spec, column_data, Type),
            .Float => return try self.readFloat(column_spec, column_data, Type),
            .Pointer => |pointer| switch (pointer.size) {
                .One => {
                    return try self.readType(column_spec, column_data, @TypeOf(pointer.child));
                },
                .Slice => {
                    return try self.readSlice(column_spec, column_data, Type, @TypeOf(pointer.child));
                },
                else => @compileError("invalid pointer size " ++ @tagName(pointer.size)),
            },
            .Array => return try self.readArray(column_spec, column_data, Type),
            else => @compileError("field type " ++ @typeName(struct_field.field_type) ++ " not handled yet"),
        }
    }

    fn readFloatFromSlice(comptime Type: type, slice: []const u8) Type {
        var r: Type = 0.0;

        // Compute the number of bytes needed for the float type we're trying to read.
        comptime const len = @divExact(Type.bit_count, 8);

        std.debug.assert(slice.len <= len);

        // See readIntFromSlice below for an explanation as to why we do this.

        var buf = [_]u8{0} ** len;

        const padding = len - slice.len;
        mem.copy(u8, buf[padding..buf.len], slice);

        var bytes = @ptrCast(*const [len]u8, &buf);

        return @ptrCast(*align(1) const Type, bytes).*;
    }

    fn readIntFromSlice(comptime Type: type, slice: []const u8) Type {
        var r: Type = 0;

        // TODO(vincent): should we optimize for u8/i8 ?

        // Compute the number of bytes needed for the integer type we're trying to read.
        comptime const len = @divExact(Type.bit_count, 8);

        std.debug.assert(slice.len <= len);

        // This may be confusing.
        //
        // This function needs to be able to read a integers from
        // the slice even if the integer type has more bytes than the slice has.
        //
        // For example, if we read a tinyint from Cassandra which is a u8,
        // we need to be able to read that into a u16/u32/u64/u128.
        //
        // To do that we allocate a buffer of how many bytes the integer type has,
        // then we copy the slice data in it starting from the correct position.
        //
        // So for example if we have a u32 and we read a u16, we allocate a 4 byte buffer:
        //   [0x00, 0x00, 0x00, 0x00]
        // Then to write at the correct position we compute the padding, in this case 2 bytes.
        // With that we write the data:
        //   [0x00, 0x00, 0x21, 0x20]
        // Now the u32 will have to correct data.

        var buf = [_]u8{0} ** len;

        const padding = len - slice.len;
        mem.copy(u8, buf[padding..buf.len], slice);

        var bytes = @ptrCast(*const [len]u8, &buf);

        return mem.readIntBig(Type, bytes);
    }

    fn readInt(self: *Self, column_spec: ColumnSpec, column_data: ColumnData, comptime Type: type) !Type {
        var r: Type = 0;

        const id = column_spec.option.id;

        switch (Type) {
            u8, i8 => {
                switch (id) {
                    .Tinyint => return readIntFromSlice(Type, column_data.slice),
                    else => std.debug.panic("CQL type {} can't be read into the type {}", .{ @tagName(id), @typeName(Type) }),
                }
            },
            u16, i16 => {
                switch (id) {
                    .Tinyint,
                    .Smallint,
                    => return readIntFromSlice(Type, column_data.slice),
                    else => std.debug.panic("CQL type {} can't be read into the type {}", .{ @tagName(id), @typeName(Type) }),
                }
            },
            u32, i32 => {
                switch (id) {
                    .Tinyint,
                    .Smallint,
                    .Int,
                    .Date,
                    => return readIntFromSlice(Type, column_data.slice),
                    else => std.debug.panic("CQL type {} can't be read into the type {}", .{ @tagName(id), @typeName(Type) }),
                }
            },
            u64, i64, u128, i128 => {
                switch (id) {
                    .Tinyint,
                    .Smallint,
                    .Int,
                    .Bigint,
                    .Counter,
                    .Date,
                    .Time,
                    .Timestamp,
                    => return readIntFromSlice(Type, column_data.slice),
                    else => std.debug.panic("CQL type {} can't be read into the type {}", .{ @tagName(id), @typeName(Type) }),
                }
            },
            else => @compileError("int type " ++ @typeName(Type) ++ " is invalid"),
        }

        return r;
    }

    fn readFloat(self: *Self, column_spec: ColumnSpec, column_data: ColumnData, comptime Type: type) !Type {
        var r: Type = 0.0;

        const id = column_spec.option.id;

        switch (Type) {
            f32 => {
                switch (id) {
                    .Float => return readFloatFromSlice(f32, column_data.slice),
                    else => std.debug.panic("CQL type {} can't be read into the type {}", .{ @tagName(id), @typeName(Type) }),
                }
            },
            f64 => {
                switch (id) {
                    .Float => {
                        const f_32 = readFloatFromSlice(f32, column_data.slice);
                        return @floatCast(Type, f_32);
                    },
                    .Double => return readFloatFromSlice(f64, column_data.slice),
                    else => std.debug.panic("CQL type {} can't be read into the type {}", .{ @tagName(id), @typeName(Type) }),
                }
            },
            f128 => {
                switch (id) {
                    .Float => {
                        const f_32 = readFloatFromSlice(f32, column_data.slice);
                        return @floatCast(Type, f_32);
                    },
                    .Double => {
                        const f_64 = readFloatFromSlice(f64, column_data.slice);
                        return @floatCast(Type, f_64);
                    },
                    else => std.debug.panic("CQL type {} can't be read into the type {}", .{ @tagName(id), @typeName(Type) }),
                }
            },
            else => @compileError("float type " ++ @typeName(Type) ++ " is invalid"),
        }

        return r;
    }

    fn readSlice(self: *Self, column_spec: ColumnSpec, column_data: ColumnData, comptime Type: type, comptime ChildType: type) !Type {
        var slice: Type = undefined;

        switch (column_spec.option.id) {
            .Blob, .UUID, .Timeuuid => slice = column_data.slice,
            .Ascii, .Varchar => slice = column_data.slice,
            .List, .Set => {
                std.debug.warn("{}\n", .{column_spec.option.value});

                // TODO(vincent): maybe too heavy weight to have the full streamer here
                var fbs = std.io.fixedBufferStream(column_data.slice);
                var framer = Framer(@TypeOf(fbs.inStream())).init(&self.arena.allocator, fbs.inStream());

                const n = try framer.readInt(u32);

                // var res = try self.arena.allocator.alloc(ChildType, @as(usize, n));

                var i: usize = 0;
                while (i < @as(usize, n)) : (i += 1) {
                    // _ = try res.append();
                }
            },
            else => std.debug.panic("CQL type {} can't be read into the type {}", .{ std.meta.tagName(column_spec.option.id), @typeName(Type) }),
        }

        return slice;
    }

    fn readArray(self: *Self, column_spec: ColumnSpec, column_data: ColumnData, comptime Type: type) !Type {
        var array: Type = undefined;

        // NOTE(vincent): Arrays are fixed size and the only thing we know has a fixed size with CQL is a UUID.
        // Maybe in the future we could allow more advanced stuff like reading blobs for arrays of packed struct/union/enum
        // Maybe we can also allow reading strings and adding zeroes to tne end ?

        switch (column_spec.option.id) {
            .UUID, .Timeuuid => mem.copy(u8, &array, column_data.slice[0..array.len]),
            else => std.debug.panic("CQL type {} can't be read into the type {}", .{ std.meta.tagName(column_spec.option.id), @typeName(Type) }),
        }

        return array;
    }
};

fn columnSpec(id: OptionID) ColumnSpec {
    return ColumnSpec{
        .keyspace = null,
        .table = null,
        .name = "name",
        .option = Option{ .id = id, .value = null },
    };
}

fn testIteratorScan(column_specs: []ColumnSpec, data: []const []const u8, row: var) !void {
    const metadata = RowsMetadata{
        .paging_state = null,
        .new_metadata_id = null,
        .global_table_spec = GlobalTableSpec{
            .keyspace = "foobar",
            .table = "user",
        },
        .column_specs = column_specs,
    };

    var column_data = std.ArrayList(ColumnData).init(testing.allocator);
    defer column_data.deinit();
    for (data) |d| {
        _ = try column_data.append(ColumnData{ .slice = d });
    }

    const row_data = &[_]RowData{
        RowData{ .slice = column_data.span() },
    };

    var iterator = Iterator.init(testing.allocator, metadata, row_data);
    defer iterator.deinit();

    var res = try iterator.scan(row);
    testing.expect(res);
}

test "iterator scan: u8/i8" {
    const Row = struct {
        u_8: u8,
        i_8: i8,
    };
    var row: Row = undefined;

    const column_specs = &[_]ColumnSpec{
        columnSpec(.Tinyint),
        columnSpec(.Tinyint),
    };
    const test_data = &[_][]const u8{ "\x20", "\x20" };

    try testIteratorScan(column_specs, test_data, &row);

    testing.expectEqual(@as(u8, 0x20), row.u_8);
    testing.expectEqual(@as(i8, 0x20), row.i_8);
}

test "iterator scan: u16/i16" {
    const Row = struct {
        u_16: u16,
        i_16: i16,
    };
    var row: Row = undefined;

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Tinyint),
            columnSpec(.Tinyint),
        };
        const test_data = &[_][]const u8{ "\x20", "\x20" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u16, 0x20), row.u_16);
        testing.expectEqual(@as(i16, 0x20), row.i_16);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Smallint),
            columnSpec(.Smallint),
        };
        const test_data = &[_][]const u8{ "\x21\x20", "\x22\x20" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u16, 0x2120), row.u_16);
        testing.expectEqual(@as(i16, 0x2220), row.i_16);
    }
}

test "iterator scan: u32/i32" {
    const Row = struct {
        u_32: u32,
        i_32: i32,
    };
    var row: Row = undefined;

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Tinyint),
            columnSpec(.Tinyint),
        };
        const test_data = &[_][]const u8{ "\x20", "\x20" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u32, 0x20), row.u_32);
        testing.expectEqual(@as(i32, 0x20), row.i_32);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Smallint),
            columnSpec(.Smallint),
        };
        const test_data = &[_][]const u8{ "\x21\x20", "\x22\x20" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u32, 0x2120), row.u_32);
        testing.expectEqual(@as(i32, 0x2220), row.i_32);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Int),
            columnSpec(.Int),
        };
        const test_data = &[_][]const u8{ "\x21\x22\x23\x24", "\x25\x26\x27\x28" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u32, 0x21222324), row.u_32);
        testing.expectEqual(@as(i32, 0x25262728), row.i_32);
    }
}

test "iterator scan: u64/i64" {
    const Row = struct {
        u_64: u64,
        i_64: i64,
    };
    var row: Row = undefined;

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Tinyint),
            columnSpec(.Tinyint),
        };
        const test_data = &[_][]const u8{ "\x20", "\x20" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u64, 0x20), row.u_64);
        testing.expectEqual(@as(i64, 0x20), row.i_64);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Smallint),
            columnSpec(.Smallint),
        };
        const test_data = &[_][]const u8{ "\x22\x20", "\x23\x20" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u64, 0x2220), row.u_64);
        testing.expectEqual(@as(i64, 0x2320), row.i_64);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Int),
            columnSpec(.Int),
        };
        const test_data = &[_][]const u8{ "\x24\x22\x28\x21", "\x22\x29\x23\x22" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u64, 0x24222821), row.u_64);
        testing.expectEqual(@as(i64, 0x22292322), row.i_64);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Bigint),
            columnSpec(.Bigint),
        };
        const test_data = &[_][]const u8{ "\x21\x22\x23\x24\x25\x26\x27\x28", "\x31\x32\x33\x34\x35\x36\x37\x38" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u64, 0x2122232425262728), row.u_64);
        testing.expectEqual(@as(i64, 0x3132333435363738), row.i_64);
    }
}

test "iterator scan: u64/i64" {
    const Row = struct {
        u_128: u128,
        i_128: i128,
    };
    var row: Row = undefined;

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Tinyint),
            columnSpec(.Tinyint),
        };
        const test_data = &[_][]const u8{ "\x20", "\x20" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u128, 0x20), row.u_128);
        testing.expectEqual(@as(i128, 0x20), row.i_128);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Smallint),
            columnSpec(.Smallint),
        };
        const test_data = &[_][]const u8{ "\x22\x20", "\x23\x20" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u128, 0x2220), row.u_128);
        testing.expectEqual(@as(i128, 0x2320), row.i_128);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Bigint),
            columnSpec(.Bigint),
        };
        const test_data = &[_][]const u8{ "\x24\x22\x28\x21", "\x22\x29\x23\x22" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u128, 0x24222821), row.u_128);
        testing.expectEqual(@as(i128, 0x22292322), row.i_128);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Bigint),
            columnSpec(.Bigint),
        };
        const test_data = &[_][]const u8{ "\x21\x22\x23\x24\x25\x26\x27\x28", "\x31\x32\x33\x34\x35\x36\x37\x38" };

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectEqual(@as(u128, 0x2122232425262728), row.u_128);
        testing.expectEqual(@as(i128, 0x3132333435363738), row.i_128);
    }
}

test "iterator scan: f32" {
    const Row = struct {
        f_32: f32,
    };
    var row: Row = undefined;

    const column_specs = &[_]ColumnSpec{columnSpec(.Float)};
    const test_data = &[_][]const u8{"\x85\xeb\x01\x40"};

    try testIteratorScan(column_specs, test_data, &row);
    testing.expectEqual(@as(f32, 2.03), row.f_32);
}

test "iterator scan: f64" {
    const Row = struct {
        f_64: f64,
    };
    var row: Row = undefined;

    {
        const column_specs = &[_]ColumnSpec{columnSpec(.Float)};
        const test_data = &[_][]const u8{"\x85\xeb\x01\x40"};

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectInDelta(@as(f64, 2.03), row.f_64, 0.000001);
    }

    {
        const column_specs = &[_]ColumnSpec{columnSpec(.Double)};
        const test_data = &[_][]const u8{"\x05\x51\xf7\x01\x40\x12\xe6\x40"};
        try testIteratorScan(column_specs, test_data, &row);
        testing.expectInDelta(@as(f64, 45202.00024), row.f_64, 0.000001);
    }
}

test "iterator scan: f128" {
    const Row = struct {
        f_128: f128,
    };
    var row: Row = undefined;

    {
        const column_specs = &[_]ColumnSpec{columnSpec(.Float)};
        const test_data = &[_][]const u8{"\x85\xeb\x01\x40"};

        try testIteratorScan(column_specs, test_data, &row);
        testing.expectInDelta(@as(f128, 2.03), row.f_128, 0.000001);
    }

    {
        const column_specs = &[_]ColumnSpec{columnSpec(.Double)};
        const test_data = &[_][]const u8{"\x05\x51\xf7\x01\x40\x12\xe6\x40"};
        try testIteratorScan(column_specs, test_data, &row);
        testing.expectInDelta(@as(f128, 45202.00024), row.f_128, 0.000001);
    }
}

test "iterator scan: blobs, uuids, timeuuids" {
    const Row = struct {
        blob: []const u8,
        uuid: [16]u8,
        tuuid: [16]u8,
    };
    var row: Row = undefined;

    const column_specs = &[_]ColumnSpec{
        columnSpec(.Blob),
        columnSpec(.UUID),
        columnSpec(.Timeuuid),
    };
    const test_data = &[_][]const u8{
        "Vincent",
        "\x02\x9a\x93\xa9\xc3\x27\x4c\x79\xbe\x32\x71\x8e\x22\xb5\x02\x4c",
        "\xe9\x13\x93\x2e\x7a\xb7\x11\xea\xbf\x1b\x10\xc3\x7b\x6e\x96\xcc",
    };

    try testIteratorScan(column_specs, test_data, &row);

    testing.expectEqualString("Vincent", row.blob);
    testing.expectEqualSlices(u8, "\x02\x9a\x93\xa9\xc3\x27\x4c\x79\xbe\x32\x71\x8e\x22\xb5\x02\x4c", &row.uuid);
    testing.expectEqualSlices(u8, "\xe9\x13\x93\x2e\x7a\xb7\x11\xea\xbf\x1b\x10\xc3\x7b\x6e\x96\xcc", &row.tuuid);
}

test "iterator scan: ascii/varchar" {
    const Row = struct {
        ascii: []const u8,
        varchar: []const u8,
    };
    var row: Row = undefined;

    const column_specs = &[_]ColumnSpec{
        columnSpec(.Ascii),
        columnSpec(.Varchar),
    };
    const test_data = &[_][]const u8{
        "Ascii vincent",
        "Varchar vincent",
    };

    try testIteratorScan(column_specs, test_data, &row);

    testing.expectEqualString("Ascii vincent", row.ascii);
    testing.expectEqualString("Varchar vincent", row.varchar);
}

test "iterator scan: set/list" {
    const Row = struct {
        set: []u32,
        list: []u32,
    };
    var row: Row = undefined;

    const column_specs = &[_]ColumnSpec{
        columnSpec(.Set),
        columnSpec(.List),
    };
    const test_data = &[_][]const u8{
        "\x00\x00\x00\x02\x00\x00\x00\x04\x21\x22\x23\x24\x00\x00\x00\x04\x31\x32\x33\x34",
        "\x00\x00\x00\x02\x00\x00\x00\x04\x41\x42\x43\x44\x00\x00\x00\x04\x51\x52\x53\x54",
    };

    try testIteratorScan(column_specs, test_data, &row);
}

// test "iterator scan" {
//     const metadata = getTestRowsMetadata();

//     var iterator = Iterator.init(metadata, row_data);

//     const Row = struct {
//         id: [16]u8,
//         age: u8,
//         name: []const u8,
//     };
//     var row: Row = undefined;

//     var res = try iterator.scan(&row);
//     testing.expect(res);

//     testing.expectEqualSlices(u8, "\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8", &row.id);
//     testing.expectEqual(@as(u8, 16), row.age);
//     testing.expectEqualString("Vincent0", row.name);

//     res = try iterator.scan(&row);
//     testing.expect(!res);
// }
