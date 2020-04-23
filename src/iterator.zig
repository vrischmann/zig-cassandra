const std = @import("std");
const heap = std.heap;
const io = std.io;
const mem = std.mem;

usingnamespace @import("primitive_types.zig");
usingnamespace @import("frame.zig");

const testing = @import("testing.zig");

pub const RawBytes = struct {
    data: []const u8,
};

pub const Iterator = struct {
    const Self = @This();

    metadata: RowsMetadata,
    rows: []const RowData,

    pos: usize,

    pub fn init(metadata: RowsMetadata, rows: []const RowData) Self {
        return Self{
            .metadata = metadata,
            .rows = rows,
            .pos = 0,
        };
    }

    pub const ScanOptions = struct {
        /// If this is provided, scan will populate some information about failures.
        /// This will provide more detail than an error can.
        diags: ?*Diagnostics = null,

        pub const Diagnostics = struct {
            /// If there was an error returned by Cassandra in a ERROR frame its message will be provided here.
            cassandra_error: ?[]const u8 = null,

            /// If the struct type in which the CQL data should be scanned is incompatible
            incompatible_metadata: IncompatibleMetadata = IncompatibleMetadata{},

            pub const IncompatibleMetadata = struct {
                /// The number of columns in the CQL result.
                metadata_columns: usize = 0,
                /// The number of fields in the struct to scan into.
                struct_fields: usize = 0,

                /// If there was an attempt to scan a CQL type into an incompatible native type, both type names
                /// will be provided here.
                incompatible_types: IncompatibleTypes = IncompatibleTypes{},

                pub const IncompatibleTypes = struct {
                    cql_type_name: ?[]const u8 = null,
                    native_type_name: ?[]const u8 = null,
                };
            };
        };
    };

    const Diags = ScanOptions.Diagnostics;

    pub fn scan(self: *Self, allocator: *mem.Allocator, options: ScanOptions, args: var) !bool {
        var dummy_diags = Diags{};
        var diags = options.diags orelse &dummy_diags;

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

        if (self.metadata.column_specs.len != @typeInfo(child).Struct.fields.len) {
            diags.incompatible_metadata.metadata_columns = self.metadata.column_specs.len;
            diags.incompatible_metadata.struct_fields = @typeInfo(child).Struct.fields.len;
            return error.IncompatibleMetadata;
        }

        inline for (@typeInfo(child).Struct.fields) |struct_field, _i| {
            const i = @as(usize, _i);

            const field_type_info = @typeInfo(struct_field.field_type);
            const field_name = struct_field.name;

            const column_spec = self.metadata.column_specs[i];
            const column_data = self.rows[self.pos].slice[i];

            @field(args, field_name) = try self.readType(allocator, diags, column_spec, column_data.slice, struct_field.field_type);
        }

        self.pos += 1;

        return true;
    }

    fn readType(self: *Self, allocator: *mem.Allocator, diags: *Diags, column_spec: ColumnSpec, column_data: []const u8, comptime Type: type) !Type {
        const type_info = @typeInfo(Type);

        // TODO(vincent): if the struct is packed we could maybe read stuff directly
        // TODO(vincent): handle packed enum
        // TODO(vincent): handle union

        const option = column_spec.option;

        switch (type_info) {
            .Int => return try self.readInt(diags, option, column_data, Type),
            .Float => return try self.readFloat(diags, option, column_data, Type),
            .Pointer => |pointer| switch (pointer.size) {
                .One => {
                    return try self.readType(allocator, diags, column_spec, column_data, @TypeOf(pointer.child));
                },
                .Slice => {
                    return try self.readSlice(allocator, diags, column_spec, column_data, Type);
                },
                else => @compileError("invalid pointer size " ++ @tagName(pointer.size)),
            },
            .Struct => return try self.readStruct(allocator, diags, column_spec, column_data, Type),
            .Array => return try self.readArray(diags, option, column_data, Type),
            else => @compileError("field type " ++ @typeName(Type) ++ " not handled yet"),
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

    fn readInt(self: *Self, diags: *Diags, option: OptionID, column_data: []const u8, comptime Type: type) !Type {
        var r: Type = 0;

        switch (Type) {
            u8, i8 => {
                switch (option) {
                    .Tinyint => return readIntFromSlice(Type, column_data),
                    else => {
                        diags.incompatible_metadata.incompatible_types.cql_type_name = @tagName(option);
                        diags.incompatible_metadata.incompatible_types.native_type_name = @typeName(Type);
                        return error.IncompatibleMetadata;
                    },
                }
            },
            u16, i16 => {
                switch (option) {
                    .Tinyint,
                    .Smallint,
                    => return readIntFromSlice(Type, column_data),
                    else => {
                        diags.incompatible_metadata.incompatible_types.cql_type_name = @tagName(option);
                        diags.incompatible_metadata.incompatible_types.native_type_name = @typeName(Type);
                        return error.IncompatibleMetadata;
                    },
                }
            },
            u32, i32 => {
                switch (option) {
                    .Tinyint,
                    .Smallint,
                    .Int,
                    .Date,
                    => return readIntFromSlice(Type, column_data),
                    else => {
                        diags.incompatible_metadata.incompatible_types.cql_type_name = @tagName(option);
                        diags.incompatible_metadata.incompatible_types.native_type_name = @typeName(Type);
                        return error.IncompatibleMetadata;
                    },
                }
            },
            u64, i64, u128, i128 => {
                switch (option) {
                    .Tinyint,
                    .Smallint,
                    .Int,
                    .Bigint,
                    .Counter,
                    .Date,
                    .Time,
                    .Timestamp,
                    => return readIntFromSlice(Type, column_data),
                    else => {
                        diags.incompatible_metadata.incompatible_types.cql_type_name = @tagName(option);
                        diags.incompatible_metadata.incompatible_types.native_type_name = @typeName(Type);
                        return error.IncompatibleMetadata;
                    },
                }
            },
            else => @compileError("int type " ++ @typeName(Type) ++ " is invalid"),
        }

        return r;
    }

    fn readFloat(self: *Self, diags: *Diags, option: OptionID, column_data: []const u8, comptime Type: type) !Type {
        var r: Type = 0.0;

        switch (Type) {
            f32 => {
                switch (option) {
                    .Float => return readFloatFromSlice(f32, column_data),
                    else => {
                        diags.incompatible_metadata.incompatible_types.cql_type_name = @tagName(option);
                        diags.incompatible_metadata.incompatible_types.native_type_name = @typeName(Type);
                        return error.IncompatibleMetadata;
                    },
                }
            },
            f64 => {
                switch (option) {
                    .Float => {
                        const f_32 = readFloatFromSlice(f32, column_data);
                        return @floatCast(Type, f_32);
                    },
                    .Double => return readFloatFromSlice(f64, column_data),
                    else => {
                        diags.incompatible_metadata.incompatible_types.cql_type_name = @tagName(option);
                        diags.incompatible_metadata.incompatible_types.native_type_name = @typeName(Type);
                        return error.IncompatibleMetadata;
                    },
                }
            },
            f128 => {
                switch (option) {
                    .Float => {
                        const f_32 = readFloatFromSlice(f32, column_data);
                        return @floatCast(Type, f_32);
                    },
                    .Double => {
                        const f_64 = readFloatFromSlice(f64, column_data);
                        return @floatCast(Type, f_64);
                    },
                    else => {
                        diags.incompatible_metadata.incompatible_types.cql_type_name = @tagName(option);
                        diags.incompatible_metadata.incompatible_types.native_type_name = @typeName(Type);
                        return error.IncompatibleMetadata;
                    },
                }
            },
            else => @compileError("float type " ++ @typeName(Type) ++ " is invalid"),
        }

        return r;
    }

    fn readSlice(self: *Self, allocator: *mem.Allocator, diags: *Diags, column_spec: ColumnSpec, column_data: []const u8, comptime Type: type) !Type {
        const type_info = @typeInfo(Type);
        const ChildType = std.meta.Elem(Type);
        if (@typeInfo(ChildType) == .Array) {
            @compileError("cannot read a slice of arrays, use a slice instead as the element type");
        }

        const id = column_spec.option;

        // Special case the u8 type because it's used for strings.
        // We can simply reuse the column data slice for this so make sure the
        // user uses a []const u8 in its struct.
        if (type_info.Pointer.is_const and ChildType == u8) {
            var slice: Type = undefined;

            switch (id) {
                .Blob, .UUID, .Timeuuid, .Ascii, .Varchar => {
                    slice = column_data;
                },
                else => {
                    diags.incompatible_metadata.incompatible_types.cql_type_name = @tagName(id);
                    diags.incompatible_metadata.incompatible_types.native_type_name = @typeName(Type);
                    return error.IncompatibleMetadata;
                },
            }

            return slice;
        }

        switch (ChildType) {
            u8, i8, u16, i16, u32, i32, i64, u64 => {
                const NonConstType = @Type(std.builtin.TypeInfo{
                    .Pointer = .{
                        .size = .Slice,
                        .is_const = false,
                        .is_volatile = type_info.Pointer.is_volatile,
                        .alignment = type_info.Pointer.alignment,
                        .child = ChildType,
                        .is_allowzero = type_info.Pointer.is_allowzero,
                        .sentinel = type_info.Pointer.sentinel,
                    },
                });

                var slice: NonConstType = undefined;

                switch (id) {
                    .Set, .List => {
                        if (column_spec.listset_element_type_option == null) {
                            return error.InvalidColumnSpec;
                        }

                        var child_column_spec: ColumnSpec = undefined;
                        child_column_spec.option = column_spec.listset_element_type_option.?;

                        var pr = PrimitiveReader.init();
                        pr.reset(column_data);
                        const n = try pr.readInt(u32);

                        slice = try allocator.alloc(ChildType, @as(usize, n));
                        errdefer allocator.free(slice);

                        var i: usize = 0;
                        while (i < n) : (i += 1) {
                            const bytes = (try pr.readBytes(allocator)) orelse unreachable;
                            defer allocator.free(bytes);

                            slice[i] = try self.readType(allocator, diags, child_column_spec, bytes, ChildType);
                        }
                    },
                    else => {
                        diags.incompatible_metadata.incompatible_types.cql_type_name = @tagName(id);
                        diags.incompatible_metadata.incompatible_types.native_type_name = @typeName(Type);
                        return error.IncompatibleMetadata;
                    },
                }

                return slice;
            },
            else => @compileError("type " ++ @typeName(Type) ++ " is invalid"),
        }
    }

    fn readStruct(self: *Self, allocator: *mem.Allocator, diags: *Diags, column_spec: ColumnSpec, column_data: []const u8, comptime Type: type) !Type {
        if (Type == RawBytes) {
            return RawBytes{
                .data = column_data,
            };
        }

        if (comptime std.meta.trait.hasFn("scan")(Type)) {
            var res: Type = undefined;

            try res.scan(allocator, column_spec, column_data);

            return res;
        }

        @compileError("type " ++ @typeName(Type) ++ " is invalid");
    }

    fn readArray(self: *Self, diags: *Diags, option: OptionID, column_data: []const u8, comptime Type: type) !Type {
        const ChildType = std.meta.Elem(Type);

        var array: Type = undefined;

        // NOTE(vincent): Arrays are fixed size and the only thing we know has a fixed size with CQL is a UUID.
        // Maybe in the future we could allow more advanced stuff like reading blobs for arrays of packed struct/union/enum
        // Maybe we can also allow reading strings and adding zeroes to tne end ?

        switch (option) {
            .UUID, .Timeuuid => mem.copy(u8, &array, column_data[0..array.len]),
            else => {
                diags.incompatible_metadata.incompatible_types.cql_type_name = @tagName(option);
                diags.incompatible_metadata.incompatible_types.native_type_name = @typeName(Type);
                return error.IncompatibleMetadata;
            },
        }

        return array;
    }
};

fn columnSpec(id: OptionID) ColumnSpec {
    return ColumnSpec{
        .keyspace = null,
        .table = null,
        .name = "name",
        .option = id,
        .listset_element_type_option = null,
        .map_key_type_option = null,
        .map_value_type_option = null,
        .custom_class_name = null,
    };
}

fn testIteratorScan(allocator: *mem.Allocator, column_specs: []ColumnSpec, data: []const []const u8, diags: ?*Iterator.ScanOptions.Diagnostics, row: var) !void {
    const metadata = RowsMetadata{
        .paging_state = null,
        .new_metadata_id = null,
        .global_table_spec = GlobalTableSpec{
            .keyspace = "foobar",
            .table = "user",
        },
        .column_specs = column_specs,
    };

    var column_data = std.ArrayList(ColumnData).init(allocator);
    for (data) |d| {
        _ = try column_data.append(ColumnData{ .slice = d });
    }

    const row_data = &[_]RowData{
        RowData{ .slice = column_data.toOwnedSlice() },
    };

    var iterator = Iterator.init(metadata, row_data);
    var options = Iterator.ScanOptions{
        .diags = diags,
    };
    testing.expect(try iterator.scan(allocator, options, row));
}

test "iterator scan: incompatible metadata" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    var diags = Iterator.ScanOptions.Diagnostics{};

    {
        const Row = struct {
            u_8: u8,
            i_8: i8,
        };
        var row: Row = undefined;

        const column_specs = &[_]ColumnSpec{columnSpec(.Tinyint)};
        const test_data = &[_][]const u8{ "\x20", "\x20" };

        var err = testIteratorScan(&arena.allocator, column_specs, test_data, &diags, &row);
        testing.expectError(error.IncompatibleMetadata, err);
        testing.expectEqual(@as(usize, 1), diags.incompatible_metadata.metadata_columns);
        testing.expectEqual(@as(usize, 2), diags.incompatible_metadata.struct_fields);
    }

    diags.incompatible_metadata.metadata_columns = 0;
    diags.incompatible_metadata.struct_fields = 0;

    {
        const Row = struct {
            b: u8,
        };
        var row: Row = undefined;

        const column_specs = &[_]ColumnSpec{columnSpec(.Int)};
        const test_data = &[_][]const u8{"\x01"};

        var err = testIteratorScan(&arena.allocator, column_specs, test_data, &diags, &row);
        testing.expectError(error.IncompatibleMetadata, err);
        testing.expectEqual(@as(usize, 0), diags.incompatible_metadata.metadata_columns);
        testing.expectEqual(@as(usize, 0), diags.incompatible_metadata.struct_fields);
        testing.expectEqualString("Int", diags.incompatible_metadata.incompatible_types.cql_type_name.?);
        testing.expectEqualString("u8", diags.incompatible_metadata.incompatible_types.native_type_name.?);
    }
}

test "iterator scan: u8/i8" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

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

    try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

    testing.expectEqual(@as(u8, 0x20), row.u_8);
    testing.expectEqual(@as(i8, 0x20), row.i_8);
}

test "iterator scan: u16/i16" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

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

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u16, 0x20), row.u_16);
        testing.expectEqual(@as(i16, 0x20), row.i_16);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Smallint),
            columnSpec(.Smallint),
        };
        const test_data = &[_][]const u8{ "\x21\x20", "\x22\x20" };

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u16, 0x2120), row.u_16);
        testing.expectEqual(@as(i16, 0x2220), row.i_16);
    }
}

test "iterator scan: u32/i32" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

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

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u32, 0x20), row.u_32);
        testing.expectEqual(@as(i32, 0x20), row.i_32);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Smallint),
            columnSpec(.Smallint),
        };
        const test_data = &[_][]const u8{ "\x21\x20", "\x22\x20" };

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u32, 0x2120), row.u_32);
        testing.expectEqual(@as(i32, 0x2220), row.i_32);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Int),
            columnSpec(.Int),
        };
        const test_data = &[_][]const u8{ "\x21\x22\x23\x24", "\x25\x26\x27\x28" };

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u32, 0x21222324), row.u_32);
        testing.expectEqual(@as(i32, 0x25262728), row.i_32);
    }
}

test "iterator scan: u64/i64" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

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

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u64, 0x20), row.u_64);
        testing.expectEqual(@as(i64, 0x20), row.i_64);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Smallint),
            columnSpec(.Smallint),
        };
        const test_data = &[_][]const u8{ "\x22\x20", "\x23\x20" };

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u64, 0x2220), row.u_64);
        testing.expectEqual(@as(i64, 0x2320), row.i_64);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Int),
            columnSpec(.Int),
        };
        const test_data = &[_][]const u8{ "\x24\x22\x28\x21", "\x22\x29\x23\x22" };

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u64, 0x24222821), row.u_64);
        testing.expectEqual(@as(i64, 0x22292322), row.i_64);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Bigint),
            columnSpec(.Bigint),
        };
        const test_data = &[_][]const u8{ "\x21\x22\x23\x24\x25\x26\x27\x28", "\x31\x32\x33\x34\x35\x36\x37\x38" };

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u64, 0x2122232425262728), row.u_64);
        testing.expectEqual(@as(i64, 0x3132333435363738), row.i_64);
    }
}

test "iterator scan: u128/i128" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

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

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u128, 0x20), row.u_128);
        testing.expectEqual(@as(i128, 0x20), row.i_128);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Smallint),
            columnSpec(.Smallint),
        };
        const test_data = &[_][]const u8{ "\x22\x20", "\x23\x20" };

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u128, 0x2220), row.u_128);
        testing.expectEqual(@as(i128, 0x2320), row.i_128);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Bigint),
            columnSpec(.Bigint),
        };
        const test_data = &[_][]const u8{ "\x24\x22\x28\x21", "\x22\x29\x23\x22" };

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u128, 0x24222821), row.u_128);
        testing.expectEqual(@as(i128, 0x22292322), row.i_128);
    }

    {
        const column_specs = &[_]ColumnSpec{
            columnSpec(.Bigint),
            columnSpec(.Bigint),
        };
        const test_data = &[_][]const u8{ "\x21\x22\x23\x24\x25\x26\x27\x28", "\x31\x32\x33\x34\x35\x36\x37\x38" };

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectEqual(@as(u128, 0x2122232425262728), row.u_128);
        testing.expectEqual(@as(i128, 0x3132333435363738), row.i_128);
    }
}

test "iterator scan: f32" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const Row = struct {
        f_32: f32,
    };
    var row: Row = undefined;

    const column_specs = &[_]ColumnSpec{columnSpec(.Float)};
    const test_data = &[_][]const u8{"\x85\xeb\x01\x40"};

    try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

    testing.expectEqual(@as(f32, 2.03), row.f_32);
}

test "iterator scan: f64" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const Row = struct {
        f_64: f64,
    };
    var row: Row = undefined;

    {
        const column_specs = &[_]ColumnSpec{columnSpec(.Float)};
        const test_data = &[_][]const u8{"\x85\xeb\x01\x40"};

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectInDelta(@as(f64, 2.03), row.f_64, 0.000001);
    }

    {
        const column_specs = &[_]ColumnSpec{columnSpec(.Double)};
        const test_data = &[_][]const u8{"\x05\x51\xf7\x01\x40\x12\xe6\x40"};

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectInDelta(@as(f64, 45202.00024), row.f_64, 0.000001);
    }
}

test "iterator scan: f128" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const Row = struct {
        f_128: f128,
    };
    var row: Row = undefined;

    {
        const column_specs = &[_]ColumnSpec{columnSpec(.Float)};
        const test_data = &[_][]const u8{"\x85\xeb\x01\x40"};

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectInDelta(@as(f128, 2.03), row.f_128, 0.000001);
    }

    {
        const column_specs = &[_]ColumnSpec{columnSpec(.Double)};
        const test_data = &[_][]const u8{"\x05\x51\xf7\x01\x40\x12\xe6\x40"};

        try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

        testing.expectInDelta(@as(f128, 45202.00024), row.f_128, 0.000001);
    }
}

test "iterator scan: blobs, uuids, timeuuids" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

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

    try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

    testing.expectEqualString("Vincent", row.blob);
    testing.expectEqualSlices(u8, "\x02\x9a\x93\xa9\xc3\x27\x4c\x79\xbe\x32\x71\x8e\x22\xb5\x02\x4c", &row.uuid);
    testing.expectEqualSlices(u8, "\xe9\x13\x93\x2e\x7a\xb7\x11\xea\xbf\x1b\x10\xc3\x7b\x6e\x96\xcc", &row.tuuid);
}

test "iterator scan: ascii/varchar" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

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

    try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

    testing.expectEqualString("Ascii vincent", row.ascii);
    testing.expectEqualString("Varchar vincent", row.varchar);
}

test "iterator scan: set/list" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const Row = struct {
        set: []const u32,
        list: []const u32,
        // set_of_uuid: []const []const u8,
        // list_of_uuid: []const []const u8,
    };
    var row: Row = undefined;

    var spec1 = columnSpec(.Set);
    spec1.listset_element_type_option = .Int;
    var spec2 = columnSpec(.List);
    spec2.listset_element_type_option = .Int;
    // var spec3 = columnSpec(.Set);
    // spec3.listset_element_type_option = .UUID;
    // var spec4 = columnSpec(.List);
    // spec4.listset_element_type_option = .UUID;

    const column_specs = &[_]ColumnSpec{ spec1, spec2 };
    const test_data = &[_][]const u8{
        "\x00\x00\x00\x02\x00\x00\x00\x04\x21\x22\x23\x24\x00\x00\x00\x04\x31\x32\x33\x34",
        "\x00\x00\x00\x02\x00\x00\x00\x04\x41\x42\x43\x44\x00\x00\x00\x04\x51\x52\x53\x54",
        // "\x00\x00\x00\x02\x00\x00\x00\x10\x14\x2d\x6b\x2d\x2c\xe6\x45\x80\x95\x53\x15\x87\xa9\x6d\xec\x94\x00\x00\x00\x10\x8a\xa8\xc1\x37\xd0\x53\x41\x12\xbf\xee\x5f\x96\x28\x7e\xe5\x1a",
        // "\x00\x00\x00\x02\x00\x00\x00\x10\x14\x2d\x6b\x2d\x2c\xe6\x45\x80\x95\x53\x15\x87\xa9\x6d\xec\x94\x00\x00\x00\x10\x8a\xa8\xc1\x37\xd0\x53\x41\x12\xbf\xee\x5f\x96\x28\x7e\xe5\x1a",
    };

    try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

    testing.expectEqual(@as(usize, 2), row.set.len);
    testing.expectEqual(@as(u32, 0x21222324), row.set[0]);
    testing.expectEqual(@as(u32, 0x31323334), row.set[1]);

    testing.expectEqual(@as(usize, 2), row.list.len);
    testing.expectEqual(@as(u32, 0x41424344), row.list[0]);
    testing.expectEqual(@as(u32, 0x51525354), row.list[1]);

    // testing.expectEqual(@as(usize, 2), row.set_of_uuid.len);
    // testing.expectEqualSlices(u8, "\x14\x2d\x6b\x2d\x2c\xe6\x45\x80\x95\x53\x15\x87\xa9\x6d\xec\x94", row.set_of_uuid[0]);
    // testing.expectEqualSlices(u8, "\x8a\xa8\xc1\x37\xd0\x53\x41\x12\xbf\xee\x5f\x96\x28\x7e\xe5\x1a", row.set_of_uuid[1]);

    // testing.expectEqual(@as(usize, 2), row.list_of_uuid.len);
    // testing.expectEqualSlices(u8, "\x14\x2d\x6b\x2d\x2c\xe6\x45\x80\x95\x53\x15\x87\xa9\x6d\xec\x94", row.list_of_uuid[0]);
    // testing.expectEqualSlices(u8, "\x8a\xa8\xc1\x37\xd0\x53\x41\x12\xbf\xee\x5f\x96\x28\x7e\xe5\x1a", row.list_of_uuid[1]);
}

test "iterator scan: set of tinyint" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const Row = struct {
        set: []u8,
    };
    var row: Row = undefined;

    var spec1 = columnSpec(.Set);
    spec1.listset_element_type_option = .Tinyint;

    const column_specs = &[_]ColumnSpec{spec1};
    const test_data = &[_][]const u8{
        "\x00\x00\x00\x02\x00\x00\x00\x01\x20\x00\x00\x00\x01\x21",
    };

    try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

    testing.expectEqual(@as(usize, 2), row.set.len);
    testing.expectEqual(@as(u8, 0x20), row.set[0]);
    testing.expectEqual(@as(u8, 0x21), row.set[1]);
}

test "iterator scan: set of tinyint into RawBytes" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const Row = struct {
        list_timestamp: []u64,
        age: u32,
        set: RawBytes,
        name: []const u8,
    };
    var row: Row = undefined;

    var spec1 = columnSpec(.List);
    spec1.listset_element_type_option = .Timestamp;
    var spec2 = columnSpec(.Int);
    var spec3 = columnSpec(.Set);
    spec3.listset_element_type_option = .Tinyint;
    var spec4 = columnSpec(.Varchar);

    const column_specs = &[_]ColumnSpec{ spec1, spec2, spec3, spec4 };
    const test_data = &[_][]const u8{
        "\x00\x00\x00\x02\x00\x00\x00\x08\xbc\xbc\xbc\xbc\xbc\xbc\xbc\xbc\x00\x00\x00\x08\xbe\xbe\xbe\xbe\xbe\xbe\xbe\xbe",
        "\x10\x11\x12\x13",
        "\x00\x00\x00\x02\x00\x00\x00\x01\x20\x00\x00\x00\x01\x21",
        "foobar",
    };

    try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

    testing.expectEqual(@as(u32, 0x10111213), row.age);
    testing.expectEqualSlices(u8, test_data[2], row.set.data);
    testing.expectEqualString("foobar", row.name);
    testing.expectEqual(@as(usize, 2), row.list_timestamp.len);
    testing.expectEqual(@as(u64, 0xbcbcbcbcbcbcbcbc), row.list_timestamp[0]);
    testing.expectEqual(@as(u64, 0xbebebebebebebebe), row.list_timestamp[1]);
}

test "iterator scan: into user provided scanner" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const MyTimestampList = struct {
        data: []u64,

        pub fn scan(self: *@This(), allocator: *mem.Allocator, column_spec: ColumnSpec, data: []const u8) !void {
            var fbs = io.fixedBufferStream(data);
            var in = fbs.inStream();

            const n = @as(usize, try in.readIntBig(u32));

            self.data = try allocator.alloc(u64, n);

            var i: usize = 0;
            while (i < n) : (i += 1) {
                _ = try in.readIntBig(u32); // void the bytes length
                self.data[i] = try in.readIntBig(u64);
            }
        }
    };

    const Row = struct {
        list_timestamp: MyTimestampList,
        age: u32,
    };
    var row: Row = undefined;

    var spec1 = columnSpec(.List);
    spec1.listset_element_type_option = .Timestamp;
    var spec2 = columnSpec(.Int);

    const column_specs = &[_]ColumnSpec{ spec1, spec2 };
    const test_data = &[_][]const u8{
        "\x00\x00\x00\x02\x00\x00\x00\x08\xbc\xbc\xbc\xbc\xbc\xbc\xbc\xbc\x00\x00\x00\x08\xbe\xbe\xbe\xbe\xbe\xbe\xbe\xbe",
        "\x10\x11\x12\x13",
    };

    try testIteratorScan(&arena.allocator, column_specs, test_data, null, &row);

    testing.expectEqual(@as(usize, 2), row.list_timestamp.data.len);
    testing.expectEqual(@as(u64, 0xbcbcbcbcbcbcbcbc), row.list_timestamp.data[0]);
    testing.expectEqual(@as(u64, 0xbebebebebebebebe), row.list_timestamp.data[1]);
    testing.expectEqual(@as(u32, 0x10111213), row.age);
}
