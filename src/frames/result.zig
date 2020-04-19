const std = @import("std");
const mem = std.mem;
const meta = std.meta;

usingnamespace @import("../frame.zig");
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

const ResultKind = packed enum(u32) {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    Prepared = 0x0004,
    SchemaChange = 0x0005,
};

const Rows = struct {
    const Self = @This();

    metadata: RowsMetadata,
    data: []RowData,

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !Self {
        var rows = Self{
            .metadata = undefined,
            .data = undefined,
        };

        rows.metadata = try RowsMetadata.read(allocator, pr);

        // Iterate over rows
        const rows_count = @as(usize, try pr.readInt(u32));

        var data = std.ArrayList(RowData).init(allocator);
        _ = try data.ensureCapacity(rows_count);
        errdefer data.deinit();

        var i: usize = 0;
        while (i < rows_count) : (i += 1) {
            var row_data = std.ArrayList(ColumnData).init(allocator);
            errdefer row_data.deinit();

            // Read a single row
            var j: usize = 0;
            while (j < rows.metadata.column_specs.len) : (j += 1) {
                const column_spec = rows.metadata.column_specs[j];
                // TODO(vincent): can this ever be null ?
                const column_data = (try pr.readBytes(allocator)) orelse unreachable;

                _ = try row_data.append(ColumnData{
                    .slice = column_data,
                });
            }

            _ = try data.append(RowData{
                .slice = row_data.toOwnedSlice(),
            });
        }

        rows.data = data.toOwnedSlice();

        return rows;
    }
};

const Prepared = struct {
    const Self = @This();

    query_id: []const u8,
    result_metadata_id: ?[]const u8,
    metadata: PreparedMetadata,
    rows_metadata: RowsMetadata,

    pub fn read(allocator: *mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        var prepared = Self{
            .query_id = undefined,
            .result_metadata_id = null,
            .metadata = undefined,
            .rows_metadata = undefined,
        };

        prepared.query_id = (try pr.readShortBytes(allocator)) orelse return error.NoQueryIDInPreparedFrame;
        if (protocol_version.is(5)) {
            prepared.result_metadata_id = (try pr.readShortBytes(allocator)) orelse return error.NoResultMetadataIDInPreparedFrame;
        }
        prepared.metadata = try PreparedMetadata.read(allocator, pr);
        prepared.rows_metadata = try RowsMetadata.read(allocator, pr);

        return prepared;
    }
};

pub const Result = union(ResultKind) {
    Void: void,
    Rows: Rows,
    SetKeyspace: []const u8,
    Prepared: Prepared,
    SchemaChange: SchemaChange,
};

/// RESULT is the result to a query (QUERY, PREPARE, EXECUTE or BATCH messages).
///
/// Described in the protocol spec at §4.2.5.
pub const ResultFrame = struct {
    const Self = @This();

    result: Result,

    pub fn read(allocator: *mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !ResultFrame {
        var frame = Self{
            .result = undefined,
        };

        const kind = @intToEnum(ResultKind, try pr.readInt(u32));

        switch (kind) {
            .Void => frame.result = Result{ .Void = .{} },
            .Rows => {
                const rows = try Rows.read(allocator, pr);
                frame.result = Result{ .Rows = rows };
            },
            .SetKeyspace => {
                const keyspace = try pr.readString(allocator);
                frame.result = Result{ .SetKeyspace = keyspace };
            },
            .Prepared => {
                const prepared = try Prepared.read(allocator, protocol_version, pr);
                frame.result = Result{ .Prepared = prepared };
            },
            .SchemaChange => {
                const schema_change = try SchemaChange.read(allocator, pr);
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
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    testing.expect(frame.result == .Void);
}

test "result frame: rows" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x20\x08\x00\x00\x00\xa2\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x03\x00\x00\x00\x10\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\xd7\x77\xd5\xd7\x58\xc0\x4d\x2b\x8c\xf9\xa3\x53\xfa\x8e\x6c\x96\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\x94\xa4\x7b\xb2\x8c\xf7\x43\x3d\x97\x6e\x72\x74\xb3\xfd\xd3\x31\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(&arena.allocator, raw_frame.header.version, &pr);

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

    const rows = frame.result.Rows;
    testing.expectEqual(@as(usize, 3), rows.data.len);

    const row1 = rows.data[0].slice;
    testing.expectEqualSlices(u8, "\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8", row1[0].slice);
    testing.expectEqualSlices(u8, "\x00", row1[1].slice);
    testing.expectEqualSlices(u8, "\x56\x69\x6e\x63\x65\x6e\x74\x30", row1[2].slice);

    const row2 = rows.data[1].slice;
    testing.expectEqualSlices(u8, "\xd7\x77\xd5\xd7\x58\xc0\x4d\x2b\x8c\xf9\xa3\x53\xfa\x8e\x6c\x96", row2[0].slice);
    testing.expectEqualSlices(u8, "\x01", row2[1].slice);
    testing.expectEqualSlices(u8, "\x56\x69\x6e\x63\x65\x6e\x74\x31", row2[2].slice);

    const row3 = rows.data[2].slice;
    testing.expectEqualSlices(u8, "\x94\xa4\x7b\xb2\x8c\xf7\x43\x3d\x97\x6e\x72\x74\xb3\xfd\xd3\x31", row3[0].slice);
    testing.expectEqualSlices(u8, "\x02", row3[1].slice);
    testing.expectEqualSlices(u8, "\x56\x69\x6e\x63\x65\x6e\x74\x32", row3[2].slice);
}

test "result frame: set keyspace" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x77\x08\x00\x00\x00\x0c\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    testing.expect(frame.result == .SetKeyspace);
    testing.expectEqualString("foobar", frame.result.SetKeyspace);
}

test "result frame: prepared insert" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x80\x08\x00\x00\x00\x4f\x00\x00\x00\x04\x00\x10\x63\x7c\x1c\x1f\xd0\x13\x4a\xb8\xfc\x94\xca\x67\xf2\x88\xb2\xa3\x00\x00\x00\x01\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x04\x00\x00\x00\x00";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    testing.expect(frame.result == .Prepared);

    // check prepared metadata

    {
        const metadata = frame.result.Prepared.metadata;
        testing.expectEqualString("foobar", metadata.global_table_spec.?.keyspace);
        testing.expectEqualString("user", metadata.global_table_spec.?.table);
        testing.expectEqual(@as(usize, 1), metadata.pk_indexes.len);
        testing.expectEqual(@as(u16, 0), metadata.pk_indexes[0]);
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
    }

    // check rows metadata

    {
        const metadata = frame.result.Prepared.rows_metadata;
        testing.expect(metadata.global_table_spec == null);
        testing.expectEqual(@as(usize, 0), metadata.column_specs.len);
    }
}

test "result frame: prepared select" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\xc0\x08\x00\x00\x00\x63\x00\x00\x00\x04\x00\x10\x3b\x2e\x8d\x03\x43\xf4\x3b\xfc\xad\xa1\x78\x9c\x27\x0e\xcf\xee\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x00\x00\x01\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    testing.expect(frame.result == .Prepared);

    // check prepared metadata

    {
        const metadata = frame.result.Prepared.metadata;
        testing.expectEqualString("foobar", metadata.global_table_spec.?.keyspace);
        testing.expectEqualString("user", metadata.global_table_spec.?.table);
        testing.expectEqual(@as(usize, 1), metadata.pk_indexes.len);
        testing.expectEqual(@as(u16, 0), metadata.pk_indexes[0]);
        testing.expectEqual(@as(usize, 1), metadata.column_specs.len);

        const col1 = metadata.column_specs[0];
        testing.expectEqualString("id", col1.name);
        testing.expectEqual(OptionID.UUID, col1.option.id);
    }

    // check rows metadata

    {
        const metadata = frame.result.Prepared.rows_metadata;
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
    }
}
