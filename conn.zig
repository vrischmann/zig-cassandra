const std = @import("std");
const debug = std.debug;
const time = std.time;
const heap = std.heap;
const io = std.io;
const mem = std.mem;
const net = std.net;
const posix = std.posix;

const protocol = @import("protocol.zig");

const Frame = protocol.Frame;

const Envelope = protocol.Envelope;
const EnvelopeFlags = protocol.EnvelopeFlags;
const EnvelopeHeader = protocol.EnvelopeHeader;
const EnvelopeWriter = protocol.EnvelopeWriter;

const CQLVersion = protocol.CQLVersion;
const CompressionAlgorithm = protocol.CompressionAlgorithm;
const MessageReader = protocol.MessageReader;
const MessageWriter = protocol.MessageWriter;
const Opcode = protocol.Opcode;
const ProtocolVersion = protocol.ProtocolVersion;

const AuthChallengeMessage = protocol.AuthChallengeMessage;
const AuthResponseMessage = protocol.AuthResponseMessage;
const AuthSuccessMessage = protocol.AuthSuccessMessage;
const AuthenticateMessage = protocol.AuthenticateMessage;
const BatchMessage = protocol.BatchMessage;
const ErrorMessage = protocol.ErrorMessage;
const EventMessage = protocol.EventMessage;
const ExecuteMessage = protocol.ExecuteMessage;
const PrepareMessage = protocol.PrepareMessage;
const QueryMessage = protocol.QueryMessage;
const ReadyMessage = protocol.ReadyMessage;
const ResultMessage = protocol.ResultMessage;
const StartupMessage = protocol.StartupMessage;
const SupportedMessage = protocol.SupportedMessage;

const lz4 = @import("lz4");
const snappy = @import("snappy");

pub const Message = union(Opcode) {
    @"error": ErrorMessage,
    startup: StartupMessage,
    ready: ReadyMessage,
    authenticate: AuthenticateMessage,
    options: void,
    supported: SupportedMessage,
    query: QueryMessage,
    result: ResultMessage,
    prepare: PrepareMessage,
    execute: ExecuteMessage,
    register: void,
    event: EventMessage,
    batch: BatchMessage,
    auth_challenge: AuthChallengeMessage,
    auth_response: AuthResponseMessage,
    auth_success: AuthSuccessMessage,
};

const log = std.log.scoped(.connection);

pub const Connection = struct {
    pub const InitOptions = struct {
        /// the protocl version to use.
        protocol_version: ProtocolVersion = ProtocolVersion{ .version = @as(u8, 4) },

        /// The compression algorithm to use if possible.
        compression: ?CompressionAlgorithm = null,
    };

    allocator: mem.Allocator,
    options: InitOptions = .{},

    socket: posix.socket_t = undefined,

    read_buffer: []u8,
    read_start: usize = 0,
    read_end: usize = 0,

    write_buffer: std.ArrayList(u8),

    message_writer: MessageWriter,
    message_reader: MessageReader,

    envelope_buffer: std.ArrayList(u8),

    framing: struct {
        enabled: bool = false,
        format: Frame.Format = .compressed,
    } = .{},

    state: enum {
        handshake,
        nominal,
    } = undefined,
    handshake: union(enum) {
        opcode: protocol.Opcode,
        done,
    } = undefined,

    /// Contains the state that is negotiated with a node as part of the handshake.
    negotiated_state: struct {
        cql_version: CQLVersion = CQLVersion.fromString("3.0.0") catch unreachable,
        compression: ?CompressionAlgorithm = null,
    } = .{},

    /// The protocol version to use.
    protocol_version: ProtocolVersion,

    pub fn init(allocator: mem.Allocator, options: InitOptions) !Connection {
        return Connection{
            .allocator = allocator,
            .options = options,
            .read_buffer = try allocator.alloc(u8, 8192),
            .write_buffer = try std.ArrayList(u8).initCapacity(allocator, 16384),
            .message_writer = try MessageWriter.init(allocator),
            .message_reader = MessageReader.init(),
            .envelope_buffer = try std.ArrayList(u8).initCapacity(allocator, 8192),
            .protocol_version = options.protocol_version,
        };
    }

    pub fn deinit(self: *Connection) void {
        self.envelope_buffer.deinit();
        self.message_writer.deinit();
        self.write_buffer.deinit();
        self.allocator.free(self.read_buffer);
    }

    pub fn connect(self: *Connection, address: net.Address) !void {
        self.socket = blk: {
            const sock_flags = posix.SOCK.STREAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC;
            const sockfd = try posix.socket(address.any.family, sock_flags, posix.IPPROTO.TCP);
            errdefer posix.close(sockfd);

            posix.connect(sockfd, &address.any, address.getOsSockLen()) catch |err| switch (err) {
                error.WouldBlock => {},
                else => return err,
            };

            break :blk sockfd;
        };

        self.state = .handshake;
        self.handshake = .{ .opcode = .options };
        try self.appendMessage(.options, protocol.OptionsMessage{});
    }

    fn appendMessage(self: *Connection, opcode: protocol.Opcode, message: anytype) !void {
        const MessageType = @TypeOf(message);

        self.message_writer.reset();

        //
        // Prepare the envelope
        //

        var envelope = Envelope{
            .header = EnvelopeHeader{
                .version = self.protocol_version,
                .flags = 0,
                .stream = 0,
                .opcode = opcode,
                .body_len = 0,
            },
            .body = &[_]u8{},
        };

        if (self.protocol_version.is(5)) {
            envelope.header.flags |= EnvelopeFlags.UseBeta;
        }

        if (std.meta.hasMethod(MessageType, "write")) {
            // Encode body
            switch (@typeInfo(@TypeOf(MessageType.write))) {
                .@"fn" => |info| {
                    if (info.params.len == 3) {
                        try message.write(self.protocol_version, &self.message_writer);
                    } else {
                        try message.write(&self.message_writer);
                    }
                },
                else => unreachable,
            }

            // This is the actual bytes of the encoded body.
            const written = self.message_writer.getWritten();

            // Default to using the uncompressed body.
            envelope.header.body_len = @intCast(written.len);
            envelope.body = written;

            // Compress the body if we can use it.
            // Only relevant for Protocol <= v4, Protocol v5 does compression using the framing format.
            if (self.protocol_version.isAtMost(4)) {
                if (self.options.compression) |compression| {
                    switch (compression) {
                        .LZ4 => {
                            const compressed_data = try lz4.compress(self.allocator, written);

                            envelope.header.flags |= EnvelopeFlags.Compression;
                            envelope.header.body_len = @intCast(compressed_data.len);
                            envelope.body = compressed_data;
                        },
                        .Snappy => {
                            const compressed_data = try snappy.compress(self.allocator, written);

                            envelope.header.flags |= EnvelopeFlags.Compression;
                            envelope.header.body_len = @intCast(compressed_data.len);
                            envelope.body = compressed_data;
                        },
                    }
                }
            }
        }

        try protocol.writeEnvelope(envelope, &self.envelope_buffer);

        //
        // Write a frame if protocol v5
        // Write the envelope directly otherwise
        //

        const envelope_data = self.envelope_buffer.items;
        defer self.envelope_buffer.clearRetainingCapacity();

        const final_payload = if (self.framing.enabled)
            try Frame.encode(self.allocator, envelope_data, true, .uncompressed)
        else
            envelope_data;

        try self.write_buffer.appendSlice(final_payload);
    }

    fn onRead(self: *Connection, pfd: *posix.pollfd) !void {
        var result = std.ArrayList(Message).init(self.allocator);

        const read_buffer = self.read_buffer[self.read_start..self.read_end];

        const consumed = if (self.framing.enabled)
            try self.readMessagesV5(self.allocator, read_buffer, &result)
        else
            try self.readMessageV4(self.allocator, read_buffer, &result);

        self.read_start += consumed;

        log.info("got messages: {any}", .{result.items});
        log.info("consumed {}, remaining: {}", .{ consumed, self.read_end - self.read_start });

        const remaining_to_read = self.read_end - self.read_start;
        if (remaining_to_read == 0) {
            self.read_start = 0;
            self.read_end = 0;
        }

        // Process messages

        if (result.items.len <= 0) return;

        switch (self.state) {
            .handshake => {
                debug.assert(result.items.len == 1);

                const message = result.items[0];

                switch (message) {
                    .supported => |msg| {
                        debug.assert(self.handshake.opcode == .options);
                        log.info("got SUPPORTED (compression: {s}), sending STARTUP", .{msg.compression_algorithms});

                        debug.assert(msg.protocol_versions.len > 0);

                        for (msg.protocol_versions) |protocol_version| {
                            if (protocol_version.eql(self.protocol_version)) {
                                break;
                            }
                        } else return error.UnsupportedProtocolVersion;

                        for (msg.cql_versions) |cql_version| {
                            if (cql_version.major == 3 and cql_version.minor == 0 and cql_version.patch == 0) {
                                break;
                            }
                        } else return error.UnsupportedCQLVersion;

                        for (msg.compression_algorithms) |compression_algorithm| {
                            if (compression_algorithm == .LZ4) {
                                self.negotiated_state.compression = .LZ4;
                            }
                        }

                        self.handshake = .{ .opcode = .startup };
                        try self.appendMessage(.startup, protocol.StartupMessage{
                            .cql_version = self.negotiated_state.cql_version,
                            .compression = self.negotiated_state.compression,
                        });
                        pfd.events |= posix.POLL.OUT;
                    },
                    .ready => {
                        debug.assert(self.handshake.opcode == .startup);

                        log.info("got READY", .{});

                        self.handshake = .done;
                        if (self.protocol_version.isAtLeast(5)) self.framing.enabled = true;
                    },
                    else => log.info("got message: {}", .{message}),
                }
            },

            .nominal => {
                log.info("nominal", .{});
            },
        }
    }

    fn readMessagesV5(self: *Connection, allocator: mem.Allocator, data: []const u8, messages: *std.ArrayList(Message)) !usize {
        const result = try Frame.decode(allocator, data, self.framing.format);
        debug.assert(result.frame.is_self_contained);
        debug.assert(result.frame.payload.len > 0);

        const message = blk: {
            const tmp = try Envelope.decode(result.frame.payload);
            break :blk try self.decodeMessage(allocator, tmp.envelope);
        };

        try messages.append(message);

        return result.consumed;
    }

    fn readMessageV4(self: *Connection, allocator: mem.Allocator, data: []const u8, messages: *std.ArrayList(Message)) !usize {
        const result = try Envelope.decode(data);
        const message = try self.decodeMessage(allocator, result.envelope);

        try messages.append(message);

        return result.consumed;
    }

    fn decodeMessage(self: *Connection, message_allocator: mem.Allocator, envelope: Envelope) !Message {
        self.message_reader.reset(envelope.body);

        const message = switch (envelope.header.opcode) {
            .@"error" => Message{ .@"error" = try ErrorMessage.read(message_allocator, &self.message_reader) },
            .startup => Message{ .startup = try StartupMessage.read(message_allocator, &self.message_reader) },
            .ready => Message{ .ready = ReadyMessage{} },
            .options => Message{ .options = {} },
            .supported => Message{ .supported = try SupportedMessage.read(message_allocator, &self.message_reader) },
            .result => Message{ .result = try ResultMessage.read(message_allocator, self.options.protocol_version, &self.message_reader) },
            .register => Message{ .register = {} },
            .event => Message{ .event = try EventMessage.read(message_allocator, &self.message_reader) },
            .authenticate => Message{ .authenticate = try AuthenticateMessage.read(message_allocator, &self.message_reader) },
            .auth_challenge => Message{ .auth_challenge = try AuthChallengeMessage.read(message_allocator, &self.message_reader) },
            .auth_success => Message{ .auth_success = try AuthSuccessMessage.read(message_allocator, &self.message_reader) },
            else => std.debug.panic("invalid read message {}\n", .{envelope.header.opcode}),
        };

        return message;
    }
};

pub const EventLoop = struct {
    const Timer = struct {
        used: bool,
        deadline_ns: i128,
        data: *anyopaque,
        cb: *const fn (data: *anyopaque) anyerror!void,
    };

    running: bool = true,
    pfds: [32]posix.pollfd = [_]posix.pollfd{.{
        .fd = 0,
        .events = 0,
        .revents = 0,
    }} ** 32,

    timers: [32]Timer = undefined,

    /// The current timestamp with a nanosecond resolution
    now: i128 = 0,

    pub fn register(self: *EventLoop, fd: posix.fd_t, events: i16) !void {
        for (&self.pfds) |*pfd| {
            if (pfd.events == 0) {
                pfd.events = events;
                pfd.fd = fd;
                return;
            }
        } else {
            return error.NoAvailableSlot;
        }
    }

    pub fn unregister(self: *EventLoop, fd: posix.fd_t) void {
        debug.assert(fd > 0);

        for (&self.pfds) |*pfd| {
            if (pfd.fd == fd) {
                pfd.fd = 0;
                pfd.events = 0;
                pfd.revents = 0;

                log.info("lol", .{});
                return;
            }
        } else {
            debug.panic("fd {} not registered", .{fd});
        }
    }

    pub fn addTimer(self: *EventLoop, data: *anyopaque, cb: *const fn (data: *anyopaque) anyerror!void, duration_ns: u64) !void {
        debug.assert(duration_ns >= 1);

        for (&self.timers) |*timer| {
            if (!timer.used) {
                timer.used = true;
                timer.data = data;
                timer.cb = cb;
                timer.deadline_ns = time.nanoTimestamp() + @as(i128, @intCast(duration_ns));
                return;
            }
        } else {
            return error.NoAvailableSlot;
        }
    }

    fn executeTimers(self: *EventLoop) !void {
        for (&self.timers, 0..) |*timer, i| {
            if (!timer.used) continue;

            if (self.now >= timer.deadline_ns) {
                log.debug("timer {} is ready to fire", .{i});

                try timer.cb(timer.data);

                timer.* = undefined;
                timer.used = false;
            }
        }
    }

    fn pollForEvents(self: *EventLoop, connections: *std.AutoArrayHashMap(posix.fd_t, Connection)) !void {
        const n = try posix.poll(&self.pfds, 1 * time.ms_per_s);
        log.debug("poll, n: {}", .{n});

        for (&self.pfds, 0..) |*pfd, i| {
            if (i >= n) break;

            log.info("pfd: {any}, i: {}", .{ pfd.*, i });

            const entry = connections.getEntry(pfd.fd).?;
            var connection = entry.value_ptr;

            if (pfd.revents & posix.POLL.OUT != 0) {
                pfd.events &= ~@as(i16, posix.POLL.OUT);

                log.info("fd {} writable", .{pfd.fd});

                if (connection.write_buffer.items.len > 0) {
                    // Try to write. If we can't write, fail.
                    // TODO(vincent): handle WouldBlock ?

                    const written = posix.write(connection.socket, connection.write_buffer.items) catch |err| {
                        log.err("unable to write to socket {}, got err: {}", .{ connection.socket, err });

                        self.unregister(connection.socket);

                        connection.deinit();
                        const removed = connections.swapRemove(connection.socket);
                        debug.assert(removed);

                        continue;
                    };

                    try connection.write_buffer.replaceRange(0, written, "");

                    log.info("written {} bytes to fd {}", .{ written, pfd.fd });

                    if (connection.write_buffer.items.len > 0) {
                        // More data to write
                        pfd.events = posix.POLL.OUT;
                    } else {
                        connection.write_buffer.clearRetainingCapacity();

                        // No more data to write, start reading
                        pfd.events = posix.POLL.IN;
                    }
                }
            }

            if (pfd.revents & posix.POLL.IN != 0) {
                pfd.events &= ~@as(i16, posix.POLL.IN);

                log.info("fd {} readable", .{pfd.fd});

                const buffer = connection.read_buffer[connection.read_end..];
                const read = posix.read(connection.socket, buffer) catch |err| switch (err) {
                    error.WouldBlock => {
                        log.info("nothing to read", .{});
                        continue;
                    },
                    else => return err,
                };
                if (read <= 0) return error.EndOfStream;

                log.info("read {} bytes from fd {}", .{ read, pfd.fd });
                connection.read_end += read;

                try connection.onRead(pfd);
            }
        }
    }

    pub fn run(self: *EventLoop, connections: *std.AutoArrayHashMap(posix.fd_t, Connection)) !void {
        while (self.running) {
            self.now = time.nanoTimestamp();

            try self.executeTimers();
            try self.pollForEvents(connections);
        }
    }
};
