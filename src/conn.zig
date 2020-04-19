const std = @import("std");
const heap = std.heap;
const mem = std.mem;
const net = std.net;

usingnamespace @import("frame.zig");
usingnamespace @import("primitive_types.zig");

// Ordered by Opcode

usingnamespace @import("frames/error.zig");
usingnamespace @import("frames/startup.zig");
usingnamespace @import("frames/ready.zig");
usingnamespace @import("frames/auth.zig");
usingnamespace @import("frames/options.zig");
usingnamespace @import("frames/supported.zig");
usingnamespace @import("frames/query.zig");
usingnamespace @import("frames/result.zig");
usingnamespace @import("frames/prepare.zig");
usingnamespace @import("frames/execute.zig");
usingnamespace @import("frames/register.zig");
usingnamespace @import("frames/event.zig");
usingnamespace @import("frames/batch.zig");

const testing = @import("testing.zig");

pub const Client = struct {
    const Self = @This();

    const InStreamType = @TypeOf(std.fs.File.InStream);
    const OutStreamType = @TypeOf(std.fs.File.OutStream);
    const RawConnType = RawConn();

    username: ?[]const u8,
    password: ?[]const u8,

    socket: std.fs.File,
    raw_conn: RawConnType,

    pub fn init(allocator: *mem.Allocator, seed_address: net.Address) !Client {
        const socket = try net.tcpConnectToAddress(seed_address);
        const socket_in_stream = socket.inStream();
        const socket_out_stream = socket.outStream();

        var raw_conn = RawConnType.init(allocator, socket_in_stream, socket_out_stream);
        var client = Client{
            .username = null,
            .password = null,
            .socket = socket,
            .raw_conn = raw_conn,
        };

        _ = try client.handshake();

        return client;
    }

    pub fn setUsername(self: *Self, username: []const u8) void {
        self.username = username;
    }
    pub fn setPassword(self: *Self, password: []const u8) void {
        self.password = password;
    }

    fn handshake(self: *Self) !void {
        // Sequence diagram to establish the connection:
        //
        // +---------+                 +---------+
        // | Client  |                 | Server  |
        // +---------+                 +---------+
        //      |                           |
        //      | OPTIONS                   |
        //      |-------------------------->|
        //      |                           |
        //      |                 SUPPORTED |
        //      |<--------------------------|
        //      |                           |
        //      | STARTUP                   |
        //      |-------------------------->|
        //      |                           |
        //      |      (READY|AUTHENTICATE) |
        //      |<--------------------------|
        //      |                           |
        //      | AUTH_RESPONSE             |
        //      |-------------------------->|
        //      |                           |
        try self.raw_conn.writeOptions();

        var startup_response = try self.raw_conn.writeStartup();

        switch (startup_response) {
            .Ready => return,
            .Authenticate => {
                var auth_result = try self.authenticate();
                switch (auth_result) {
                    .Challenge => unreachable,
                    .Success => return,
                    .Error => |err| {
                        std.debug.panic("error code: {} message: {}\n", .{ err.error_code, err.message });
                    },
                }
            },
        }
    }

    fn authenticate(self: *Self) !AuthResult {
        // TODO(vincent): handle authenticator classes
        // TODO(vincent): handle auth challenges
        if (self.username == null) {
            return error.NoUsername;
        }
        if (self.password == null) {
            return error.NoPasswors;
        }

        return try self.raw_conn.writeAuthResponse(
            self.username.?,
            self.password.?,
        );
    }
};

const AuthResultTag = enum {
    Challenge,
    Success,
    Error,
};
const AuthResult = union(AuthResultTag) {
    Challenge: AuthChallengeFrame,
    Success: ReadyFrame,
    Error: ErrorFrame,
};

// TODO(vincent): for now taking InStreamType/OutStreamType in is broken with Zig refusing to compile.
// See https://github.com/ziglang/zig/issues/5090
// Use std.fs.File directly so we can test with a TCP socket.
fn RawConn() type {
    const InStreamType = std.fs.File.InStream;
    const OutStreamType = std.fs.File.OutStream;

    const RawFrameReaderType = RawFrameReader(InStreamType);
    const RawFrameWriterType = RawFrameWriter(OutStreamType);

    return struct {
        const Self = @This();

        in_stream: InStreamType,
        out_stream: OutStreamType,

        arena: heap.ArenaAllocator,

        raw_frame_reader: RawFrameReaderType,
        raw_frame_writer: RawFrameWriterType,
        primitive_reader: PrimitiveReader,
        primitive_writer: PrimitiveWriter,

        // Negotiated with the server
        protocol_version: ProtocolVersion,
        cql_version: CQLVersion,

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
                .raw_frame_writer = RawFrameWriterType.init(out_stream),
                .primitive_reader = PrimitiveReader.init(&arena.allocator),
                .primitive_writer = PrimitiveWriter.init(),
                .protocol_version = undefined,
                .cql_version = undefined,
            };
        }

        fn writeOptions(self: *Self) !void {
            // Write OPTIONS
            {
                const raw_frame = RawFrame{
                    .header = FrameHeader{
                        .version = ProtocolVersion{ .version = 3 },
                        .flags = 0,
                        .stream = 0,
                        .opcode = .Options,
                        .body_len = 0,
                    },
                    .body = &[_]u8{},
                };
                _ = try self.raw_frame_writer.write(raw_frame);
            }

            // Read SUPPORTED
            const raw_frame = try self.raw_frame_reader.read();
            self.primitive_reader.reset(raw_frame.body);

            var supported_frame = try SupportedFrame.read(&self.arena.allocator, &self.primitive_reader);

            // TODO(vincent): handle compression
            self.protocol_version = supported_frame.protocol_versions[0];
            self.cql_version = supported_frame.cql_versions[0];
        }

        fn writeStartup(self: *Self) !StartupResponse {
            var startup_frame = StartupFrame{
                .cql_version = self.cql_version,
                .compression = null,
            };

            // Write STARTUP
            {
                // Encode body
                var buf: [128]u8 = undefined;
                self.primitive_writer.reset(&buf);

                _ = try startup_frame.write(&self.primitive_writer);

                // Write raw frame
                const raw_frame = RawFrame{
                    .header = FrameHeader{
                        .version = self.protocol_version,
                        .flags = 0,
                        .stream = 0,
                        .opcode = .Startup,
                        .body_len = @intCast(u32, self.primitive_writer.getWritten().len),
                    },
                    .body = self.primitive_writer.getWritten(),
                };
                _ = try self.raw_frame_writer.write(raw_frame);
            }

            // Read either READY or AUTHENTICATE
            {
                const raw_frame = try self.raw_frame_reader.read();
                self.primitive_reader.reset(raw_frame.body);

                return switch (raw_frame.header.opcode) {
                    .Ready => StartupResponse{ .Ready = .{} },
                    .Authenticate => StartupResponse{
                        .Authenticate = try AuthenticateFrame.read(&self.arena.allocator, &self.primitive_reader),
                    },
                    else => error.InvalidResponseFrame,
                };
            }
        }

        fn writeAuthResponse(self: *Self, username: []const u8, password: []const u8) !AuthResult {
            unreachable;
        }
    };
}

const StartupResponseTag = enum {
    Ready,
    Authenticate,
};
const StartupResponse = union(StartupResponseTag) {
    Ready: ReadyFrame,
    Authenticate: AuthenticateFrame,
};

test "raw conn: startup" {
    // TODO(vincent): this is tedious just to test.
    // var buf: [1024]u8 = undefined;
    // var fbs = std.io.fixedBufferStream(&buf);
    // const fbs_type = @TypeOf(fbs);
    // var in_stream = fbs.inStream();

    // var raw_conn = RawConn(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    // defer raw_conn.deinit();
}
