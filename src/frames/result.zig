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

const Rows = struct {};

const Prepared = struct {};

const Result = union(ResultKind) {
    Void: void,
    Rows: Rows,
    SetKeyspace: []const u8,
    Prepared: Prepared,
    SchemaChange: SchemaChange,

    pub fn deinit(self: @This(), allocator: *mem.Allocator) void {
        switch (self) {
            Result.Void => return,
            Result.Rows => unreachable,
            Result.SetKeyspace => |r| allocator.free(r),
            Result.Prepared => unreachable,
            Result.SchemaChange => |r| r.deinit(),
        }
    }
};

/// RESULT is the result to a query (QUERY, PREPARE, EXECUTE or BATCH messages).
///
/// Described in the protocol spec at §4.2.5.
const ResultFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    result: Result,

    pub fn deinit(self: Self) void {
        self.result.deinit(self.allocator);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !ResultFrame {
        var frame = Self{
            .allocator = allocator,
            .result = undefined,
        };

        const kind = @intToEnum(ResultKind, try framer.readInt(u32));

        switch (kind) {
            .Void => {},
            .Rows => unreachable,
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

test "result frame: set keyspace" {
    const data = "\x84\x00\x00\x77\x08\x00\x00\x00\x0c\x00\x00\x00\x03\x00\x06\x66\x6f\x6f\x62\x61\x72";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Result, data.len, framer.header);

    const frame = try ResultFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expect(frame.result == .SetKeyspace);
    testing.expectEqualString("foobar", frame.result.SetKeyspace);
}
