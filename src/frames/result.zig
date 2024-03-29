const std = @import("std");
const mem = std.mem;
const meta = std.meta;

usingnamespace @import("../primitive_types.zig");
usingnamespace @import("../event.zig");

const PrimitiveReader = @import("../primitive/reader.zig").PrimitiveReader;

const RowData = @import("../frame.zig").RowData;
const ColumnData = @import("../frame.zig").ColumnData;

const PreparedMetadata = @import("../metadata.zig").PreparedMetadata;
const RowsMetadata = @import("../metadata.zig").RowsMetadata;

const checkHeader = @import("../frame.zig").checkHeader;
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

    pub fn read(allocator: *mem.Allocator, protocol_version: ProtocolVersion, pr: *PrimitiveReader) !Self {
        var rows = Self{
            .metadata = undefined,
            .data = undefined,
        };

        rows.metadata = try RowsMetadata.read(allocator, protocol_version, pr);

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
            while (j < rows.metadata.columns_count) : (j += 1) {
                const column_data = (try pr.readBytes(allocator)) orelse &[_]u8{};

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

        prepared.metadata = try PreparedMetadata.read(allocator, protocol_version, pr);
        prepared.rows_metadata = try RowsMetadata.read(allocator, protocol_version, pr);

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
                const rows = try Rows.read(allocator, protocol_version, pr);
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

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    try testing.expect(frame.result == .Void);
}

test "result frame: rows" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x20\x08\x00\x00\x00\xa2\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x03\x00\x00\x00\x10\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\xd7\x77\xd5\xd7\x58\xc0\x4d\x2b\x8c\xf9\xa3\x53\xfa\x8e\x6c\x96\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\x94\xa4\x7b\xb2\x8c\xf7\x43\x3d\x97\x6e\x72\x74\xb3\xfd\xd3\x31\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    try testing.expect(frame.result == .Rows);

    // check metadata

    const metadata = frame.result.Rows.metadata;
    try testing.expect(metadata.paging_state == null);
    try testing.expect(metadata.new_metadata_id == null);
    try testing.expectEqualStrings("foobar", metadata.global_table_spec.?.keyspace);
    try testing.expectEqualStrings("user", metadata.global_table_spec.?.table);
    try testing.expectEqual(@as(usize, 3), metadata.column_specs.len);

    const col1 = metadata.column_specs[0];
    try testing.expectEqualStrings("id", col1.name);
    try testing.expectEqual(OptionID.UUID, col1.option);
    const col2 = metadata.column_specs[1];
    try testing.expectEqualStrings("age", col2.name);
    try testing.expectEqual(OptionID.Tinyint, col2.option);
    const col3 = metadata.column_specs[2];
    try testing.expectEqualStrings("name", col3.name);
    try testing.expectEqual(OptionID.Varchar, col3.option);

    // check data

    const rows = frame.result.Rows;
    try testing.expectEqual(@as(usize, 3), rows.data.len);

    const row1 = rows.data[0].slice;
    try testing.expectEqualSlices(u8, "\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8", row1[0].slice);
    try testing.expectEqualSlices(u8, "\x00", row1[1].slice);
    try testing.expectEqualSlices(u8, "\x56\x69\x6e\x63\x65\x6e\x74\x30", row1[2].slice);

    const row2 = rows.data[1].slice;
    try testing.expectEqualSlices(u8, "\xd7\x77\xd5\xd7\x58\xc0\x4d\x2b\x8c\xf9\xa3\x53\xfa\x8e\x6c\x96", row2[0].slice);
    try testing.expectEqualSlices(u8, "\x01", row2[1].slice);
    try testing.expectEqualSlices(u8, "\x56\x69\x6e\x63\x65\x6e\x74\x31", row2[2].slice);

    const row3 = rows.data[2].slice;
    try testing.expectEqualSlices(u8, "\x94\xa4\x7b\xb2\x8c\xf7\x43\x3d\x97\x6e\x72\x74\xb3\xfd\xd3\x31", row3[0].slice);
    try testing.expectEqualSlices(u8, "\x02", row3[1].slice);
    try testing.expectEqualSlices(u8, "\x56\x69\x6e\x63\x65\x6e\x74\x32", row3[2].slice);
}

test "result frame: rows, don't skip metadata" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x00\x08\x00\x00\x02\x3a\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x00\x00\x28\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x2e\x64\x62\x2e\x6d\x61\x72\x73\x68\x61\x6c\x2e\x42\x79\x74\x65\x54\x79\x70\x65\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x0d\x00\x00\x00\x10\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\x87\x0e\x45\x7f\x56\x4a\x4f\xd5\xb5\xd6\x4a\x48\x4b\xe0\x67\x67\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\xc5\xb1\x12\xf4\x0d\xea\x4d\x22\x83\x5b\xe0\x25\xef\x69\x0c\xe1\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\xd7\x77\xd5\xd7\x58\xc0\x4d\x2b\x8c\xf9\xa3\x53\xfa\x8e\x6c\x96\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31\x00\x00\x00\x10\x65\x54\x02\x89\x33\xe1\x42\x73\x82\xcc\x1c\xdb\x3d\x24\x5e\x40\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\x6a\xd1\x07\x9d\x27\x23\x47\x76\x8b\x7f\x39\x4d\xe3\xb8\x97\xc5\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32\x00\x00\x00\x10\xe9\xef\xfe\xa6\xeb\xc5\x4d\xb9\xaf\xc7\xd7\xc0\x28\x43\x27\x40\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\x96\xe6\xdd\x62\x14\xc9\x4e\x7c\xa1\x2f\x98\x5e\xe9\xe0\x91\x0d\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32\x00\x00\x00\x10\xef\xd5\x5a\x9b\xec\x7f\x4c\x5c\x89\xc3\x8c\xfa\x28\xf9\x6d\xfe\x00\x00\x00\x01\x00\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x30\x00\x00\x00\x10\x94\xa4\x7b\xb2\x8c\xf7\x43\x3d\x97\x6e\x72\x74\xb3\xfd\xd3\x31\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32\x00\x00\x00\x10\xe6\x02\xc7\x47\xbf\xca\x44\xbc\x9d\xc6\x6b\x04\x0f\xb7\x15\xed\x00\x00\x00\x01\x78\x00\x00\x00\x04\x48\x61\x68\x61\x00\x00\x00\x10\xac\x5e\xcc\xa8\x8e\xa1\x42\x2f\x86\xe6\xa0\x93\xbe\xd2\x73\x22\x00\x00\x00\x01\x02\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x32\x00\x00\x00\x10\xbe\x90\x37\x66\x31\xe5\x43\x93\xbc\x99\x43\xd3\x69\xf8\xe6\xba\x00\x00\x00\x01\x01\x00\x00\x00\x08\x56\x69\x6e\x63\x65\x6e\x74\x31";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    try testing.expect(frame.result == .Rows);

    // check metadata

    const metadata = frame.result.Rows.metadata;
    try testing.expect(metadata.paging_state == null);
    try testing.expect(metadata.new_metadata_id == null);
    try testing.expectEqualStrings("foobar", metadata.global_table_spec.?.keyspace);
    try testing.expectEqualStrings("user", metadata.global_table_spec.?.table);
    try testing.expectEqual(@as(usize, 3), metadata.column_specs.len);

    const col1 = metadata.column_specs[0];
    try testing.expectEqualStrings("id", col1.name);
    try testing.expectEqual(OptionID.UUID, col1.option);
    const col2 = metadata.column_specs[1];
    try testing.expectEqualStrings("age", col2.name);
    try testing.expectEqual(OptionID.Custom, col2.option);
    const col3 = metadata.column_specs[2];
    try testing.expectEqualStrings("name", col3.name);
    try testing.expectEqual(OptionID.Varchar, col3.option);

    // check data

    const rows = frame.result.Rows;
    try testing.expectEqual(@as(usize, 13), rows.data.len);

    const row1 = rows.data[0].slice;
    try testing.expectEqualSlices(u8, "\x35\x94\x43\xf3\xb7\xc4\x47\xb2\x8a\xb4\xe2\x42\x39\x79\x36\xf8", row1[0].slice);
    try testing.expectEqualSlices(u8, "\x00", row1[1].slice);
    try testing.expectEqualSlices(u8, "\x56\x69\x6e\x63\x65\x6e\x74\x30", row1[2].slice);
}

test "result frame: rows, list of uuid" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x00\x08\x00\x00\x00\x58\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x02\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x0a\x61\x67\x65\x5f\x74\x6f\x5f\x69\x64\x73\x00\x03\x61\x67\x65\x00\x09\x00\x03\x69\x64\x73\x00\x20\x00\x0c\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x78\x00\x00\x00\x18\x00\x00\x00\x01\x00\x00\x00\x10\xe6\x02\xc7\x47\xbf\xca\x44\xbc\x9d\xc6\x6b\x04\x0f\xb7\x15\xed";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    try testing.expect(frame.result == .Rows);

    // check metadata

    const metadata = frame.result.Rows.metadata;
    try testing.expect(metadata.paging_state == null);
    try testing.expect(metadata.new_metadata_id == null);
    try testing.expectEqualStrings("foobar", metadata.global_table_spec.?.keyspace);
    try testing.expectEqualStrings("age_to_ids", metadata.global_table_spec.?.table);
    try testing.expectEqual(@as(usize, 2), metadata.column_specs.len);

    const col1 = metadata.column_specs[0];
    try testing.expectEqualStrings("age", col1.name);
    try testing.expectEqual(OptionID.Int, col1.option);
    const col2 = metadata.column_specs[1];
    try testing.expectEqualStrings("ids", col2.name);
    try testing.expectEqual(OptionID.List, col2.option);

    // check data

    const rows = frame.result.Rows;
    try testing.expectEqual(@as(usize, 1), rows.data.len);

    const row1 = rows.data[0].slice;
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x78", row1[0].slice);
    try testing.expectEqualSlices(u8, "\x00\x00\x00\x01\x00\x00\x00\x10\xe6\x02\xc7\x47\xbf\xca\x44\xbc\x9d\xc6\x6b\x04\x0f\xb7\x15\xed", row1[1].slice);
}

test "result frame: set keyspace" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x77\x08\x00\x00\x00\x0c\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    try testing.expect(frame.result == .SetKeyspace);
    try testing.expectEqualStrings("foobar", frame.result.SetKeyspace);
}

test "result frame: prepared insert" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x80\x08\x00\x00\x00\x4f\x00\x00\x00\x04\x00\x10\x63\x7c\x1c\x1f\xd0\x13\x4a\xb8\xfc\x94\xca\x67\xf2\x88\xb2\xa3\x00\x00\x00\x01\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d\x00\x00\x00\x04\x00\x00\x00\x00";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    try testing.expect(frame.result == .Prepared);

    // check prepared metadata

    {
        const metadata = frame.result.Prepared.metadata;
        try testing.expectEqualStrings("foobar", metadata.global_table_spec.?.keyspace);
        try testing.expectEqualStrings("user", metadata.global_table_spec.?.table);
        try testing.expectEqual(@as(usize, 1), metadata.pk_indexes.len);
        try testing.expectEqual(@as(u16, 0), metadata.pk_indexes[0]);
        try testing.expectEqual(@as(usize, 3), metadata.column_specs.len);

        const col1 = metadata.column_specs[0];
        try testing.expectEqualStrings("id", col1.name);
        try testing.expectEqual(OptionID.UUID, col1.option);
        const col2 = metadata.column_specs[1];
        try testing.expectEqualStrings("age", col2.name);
        try testing.expectEqual(OptionID.Tinyint, col2.option);
        const col3 = metadata.column_specs[2];
        try testing.expectEqualStrings("name", col3.name);
        try testing.expectEqual(OptionID.Varchar, col3.option);
    }

    // check rows metadata

    {
        const metadata = frame.result.Prepared.rows_metadata;
        try testing.expect(metadata.global_table_spec == null);
        try testing.expectEqual(@as(usize, 0), metadata.column_specs.len);
    }
}

test "result frame: prepared select" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\xc0\x08\x00\x00\x00\x63\x00\x00\x00\x04\x00\x10\x3b\x2e\x8d\x03\x43\xf4\x3b\xfc\xad\xa1\x78\x9c\x27\x0e\xcf\xee\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x00\x00\x01\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72\x00\x04\x75\x73\x65\x72\x00\x02\x69\x64\x00\x0c\x00\x03\x61\x67\x65\x00\x14\x00\x04\x6e\x61\x6d\x65\x00\x0d";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    try checkHeader(Opcode.Result, data.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try ResultFrame.read(&arena.allocator, raw_frame.header.version, &pr);

    try testing.expect(frame.result == .Prepared);

    // check prepared metadata

    {
        const metadata = frame.result.Prepared.metadata;
        try testing.expectEqualStrings("foobar", metadata.global_table_spec.?.keyspace);
        try testing.expectEqualStrings("user", metadata.global_table_spec.?.table);
        try testing.expectEqual(@as(usize, 1), metadata.pk_indexes.len);
        try testing.expectEqual(@as(u16, 0), metadata.pk_indexes[0]);
        try testing.expectEqual(@as(usize, 1), metadata.column_specs.len);

        const col1 = metadata.column_specs[0];
        try testing.expectEqualStrings("id", col1.name);
        try testing.expectEqual(OptionID.UUID, col1.option);
    }

    // check rows metadata

    {
        const metadata = frame.result.Prepared.rows_metadata;
        try testing.expectEqualStrings("foobar", metadata.global_table_spec.?.keyspace);
        try testing.expectEqualStrings("user", metadata.global_table_spec.?.table);
        try testing.expectEqual(@as(usize, 3), metadata.column_specs.len);

        const col1 = metadata.column_specs[0];
        try testing.expectEqualStrings("id", col1.name);
        try testing.expectEqual(OptionID.UUID, col1.option);
        const col2 = metadata.column_specs[1];
        try testing.expectEqualStrings("age", col2.name);
        try testing.expectEqual(OptionID.Tinyint, col2.option);
        const col3 = metadata.column_specs[2];
        try testing.expectEqualStrings("name", col3.name);
        try testing.expectEqual(OptionID.Varchar, col3.option);
    }
}
