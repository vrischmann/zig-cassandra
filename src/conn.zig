const std = @import("std");
const heap = std.heap;
const mem = std.mem;
const net = std.net;

usingnamespace @import("frame.zig");
usingnamespace @import("primitive_types.zig");
usingnamespace @import("iterator.zig");
usingnamespace @import("query_parameters.zig");

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

pub const QueryResultTag = enum {
    None,
    Iter,
};

pub const QueryResult = union(QueryResultTag) {
    None: void,
    Iter: Iterator,
};

pub const Client = struct {
    const Self = @This();

    const InStreamType = @TypeOf(std.fs.File.InStream);
    const OutStreamType = @TypeOf(std.fs.File.OutStream);
    const RawConnType = RawConn();

    arena: heap.ArenaAllocator,

    username: ?[]const u8,
    password: ?[]const u8,

    socket: std.fs.File,
    raw_conn: RawConnType,

    pub fn init(allocator: *mem.Allocator, seed_address: net.Address) !Client {
        const socket = try net.tcpConnectToAddress(seed_address);
        const socket_in_stream = socket.inStream();
        const socket_out_stream = socket.outStream();

        var client = Client{
            .arena = heap.ArenaAllocator.init(allocator),
            .username = null,
            .password = null,
            .socket = socket,
            .raw_conn = undefined,
        };
        client.raw_conn = RawConnType.init(&client.arena.allocator, socket_in_stream, socket_out_stream);

        _ = try client.handshake();

        return client;
    }

    pub fn setUsername(self: *Self, username: []const u8) void {
        self.username = username;
    }
    pub fn setPassword(self: *Self, password: []const u8) void {
        self.password = password;
    }

    // TODO(vincent): maybe add not comptime equivalent ?
    // TODO(vincent): return an iterator
    // TODO(vincent): parse the query string to match the args tuple

    pub fn cquery(self: *Self, comptime query_string: []const u8, args: var) !QueryResult {
        std.debug.assert(self.socket.handle > 0);

        var parameters: QueryParameters = undefined;
        var result = try self.raw_conn.writeQuery(query_string, parameters);
        switch (result) {
            .Rows => |rows| {
                var iter = Iterator.init(&self.arena.allocator, rows.metadata, rows.data);
                return QueryResult{ .Iter = iter };
            },
            else => return QueryResult{ .None = .{} },
        }
    }

    fn handshake(self: *Self) !void {
        defer self.raw_conn.reset();

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

        allocator: *mem.Allocator,
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
            var result = Self{
                .allocator = allocator,
                .arena = heap.ArenaAllocator.init(allocator),
                .in_stream = in_stream,
                .out_stream = out_stream,
                .raw_frame_reader = undefined,
                .raw_frame_writer = RawFrameWriterType.init(out_stream),
                .primitive_reader = undefined,
                .primitive_writer = PrimitiveWriter.init(),
                .protocol_version = undefined,
                .cql_version = undefined,
            };
            result.raw_frame_reader = RawFrameReaderType.init(&result.arena.allocator, in_stream);
            result.primitive_reader = PrimitiveReader.init(&result.arena.allocator);

            return result;
        }

        fn reset(self: *Self) void {
            self.arena.deinit();
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
            if (supported_frame.protocol_versions.len > 0) {
                self.protocol_version = supported_frame.protocol_versions[0];
            } else {
                self.protocol_version = ProtocolVersion{ .version = 3 };
            }
            self.cql_version = supported_frame.cql_versions[0];
        }

        fn writeStartup(self: *Self) !StartupResponse {
            // Write STARTUP
            {
                var startup_frame = StartupFrame{
                    .cql_version = self.cql_version,
                    .compression = null,
                };

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

        fn writeAuthResponse(self: *Self, username: []const u8, password: []const u8) !AuthResult {
            unreachable;
        }

        fn writeQuery(self: *Self, query: []const u8, query_parameters: QueryParameters) !Result {
            @breakpoint();
            // Write QUERY
            {
                var query_frame = QueryFrame{
                    .query = query,
                    .query_parameters = query_parameters,
                };
                // Encode body

                // TODO(vincent): this sucks, will change later
                var buf: [4096]u8 = undefined;
                self.primitive_writer.reset(&buf);

                _ = try query_frame.write(self.protocol_version, &self.primitive_writer);

                // Write raw frame
                const raw_frame = RawFrame{
                    .header = FrameHeader{
                        .version = self.protocol_version,
                        .flags = 0,
                        .stream = 0,
                        .opcode = .Query,
                        .body_len = @intCast(u32, self.primitive_writer.getWritten().len),
                    },
                    .body = self.primitive_writer.getWritten(),
                };
                _ = try self.raw_frame_writer.write(raw_frame);
            }

            // Read RESULT
            @breakpoint();
            const raw_frame = try self.raw_frame_reader.read();
            self.primitive_reader.reset(raw_frame.body);

            if (raw_frame.header.opcode != .Result) {
                return error.InvalidServerResponseOpcode;
            }

            var result_frame = try ResultFrame.read(&self.arena.allocator, self.protocol_version, &self.primitive_reader);
            return result_frame.result;
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
