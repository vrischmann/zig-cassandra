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
    socket: posix.socket_t = undefined,
    socket_connected: bool = false,
    socket_read_buffer: []u8 = undefined,

    framing: struct {
        enabled: bool = false,
        format: Frame.Format = .compressed,
    } = .{},

    pfd_slot: ?usize =null,

    pub fn init(allocator: mem.Allocator, address: net.Address) !Connection {
        var self = Connection{
            .socket_read_buffer = try allocator.alloc(u8, 8192),
        };

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

        return self;
    }

    pub fn deinit(self: Connection, allocator: mem.Allocator) void {
        allocator.free(self.socket_read_buffer);
    }

    pub fn tick(self: *Connection, event_loop: *EventLoop, events: i16) !void {
        log.debug("ticking {*}", .{self});

        if (slef)

        if (!self.socket_connected) {


            event_loop.register(@intCast(self.socket), posix.POLL.OUT);
        }
    }
};

pub const EventLoop = struct {
    running: bool = true,

    fds: [32]posix.pollfd = [_]posix.pollfd{.{
        .fd = 0,
        .events = 0,
        .revents = 0,
    }} ** 32,

    pub fn register(self: *EventLoop, fd: posix.fd_t, events: i16) !void {
        for (&self.fds) |*pfd| {
            if (pfd.events == 0) {
                pfd.events = events;
                pfd.fd = fd;
                return;
            }
        } else {
            return error.NoAvailableSlot;
        }
    }

    pub fn run(self: *EventLoop, connections: *std.AutoArrayHashMap(posix.fd_t, Connection)) !void {
        while (self.running) {
            const n = try posix.poll(&self.fds, 1 * time.ms_per_s);
            log.debug("poll, n: {}", .{n});

            for (&self.fds, 0..) |fd, i| {
                if (i >= n) break;

                // TODO(vincent): handle local fds ?

                const entry = connections.getEntry(fd.fd).?;

                const conn = entry.value_ptr;
                conn.socket_state = 0;

                if (fd.revents & posix.POLL.IN != 0) conn.socket_state |= Connection.socket_readable;
                if (fd.revents & posix.POLL.OUT != 0) conn.socket_state |= Connection.socket_writable;

                try conn.tick();
            }
        }
    }
};
