const std = @import("std");
const heap = std.heap;
const mem = std.mem;

usingnamespace @import("frame.zig");
usingnamespace @import("primitive_types.zig");

const testing = @import("testing.zig");

pub fn RawConn(comptime InStreamType: type, comptime OutStreamType: type) type {
    const RawFrameReaderType = RawFrameReader(InStreamType);
    const RawFrameWriterType = RawFramewriter(InStreamType);

    return struct {
        const Self = @This();

        in_stream: InStreamType,
        out_stream: OutStreamType,

        arena: heap.ArenaAllocator,

        raw_frame_reader: RawFrameReaderType,
        raw_frame_writer: RawFrameWriterType,
        primitive_reader: PrimitiveReader,
        primitive_writer: PrimitiveWriter,

        pub fn deinit(self: *Self) void {
            self.arena.deinit();
        }

        pub fn init(allocator: *mem.Allocator, in_stream: InStreamType, out_stream: OutStreamType) Self {
            var arena = heap.ArenaAllocator.init(allocator);

            return Self{
                .arena = arena,
                .in_stream = in_stream,
                .out_stream = out_stream,
                .raw_frame_reader = RawFrameReaderType.init(&arena.allocator, in_stream),
                .raw_frame_writer = RawFrameWriterType.init(&arena.allocator, out_stream),
                .primitive_reader = PrimitiveReader.init(&arena.allocator),
                .primitive_writer = PrimitiveWriter.init(),
            };
        }

        const StartupResponseTag = enum {
            Ready,
            Authenticate,
        };
        const StartupResponse = union(StartupResponseTag) {
            Readey: ReadyFrame,
            Authenticate: AuthenticateFrame,
        };

        fn startup(self: *Self) !StartupResponse {
            unreachable;
        }
    };
}

test "raw conn: startup" {
    // TODO(vincent): this is tedious just to test.
    // var buf: [1024]u8 = undefined;
    // var fbs = std.io.fixedBufferStream(&buf);
    // const fbs_type = @TypeOf(fbs);
    // var in_stream = fbs.inStream();

    // var raw_conn = RawConn(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    // defer raw_conn.deinit();
}
