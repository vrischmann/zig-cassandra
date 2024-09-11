const std = @import("std");
const debug = std.debug;
const fmt = std.fmt;
const log = std.log;
const mem = std.mem;
const os = std.os;
const posix = std.posix;

const io_uring_cqe = std.os.linux.io_uring_cqe;

const Self = @This();

allocator: mem.Allocator,
ring: os.linux.IoUring,

pub fn init(allocator: mem.Allocator) !Self {
    return .{
        .allocator = allocator,
        .ring = try os.linux.IoUring.init(512, 0),
    };
}

pub fn deinit(self: *Self) void {
    self.ring.deinit();
}

pub fn submitAndProcess(self: *Self) !void {
    // Submit
    const nb = try self.ring.submit();

    // Process
    var cqes: [32]io_uring_cqe = undefined;
    const ready = try self.ring.copy_cqes(&cqes, nb);

    log.debug("ready: {}", .{ready});

    for (cqes[0..@intCast(ready)]) |cqe| {
        const callback: *Callback = @ptrFromInt(cqe.user_data);
        defer self.allocator.destroy(callback);

        callback.internal_cb(callback.context, cqe) catch |err| {
            log.err("got error while executing callback: {}", .{err});
        };
    }
}

pub fn drain(self: *Self) !void {
    while (self.ring.cq_ready() > 0) {
        try self.submitAndProcess();
    }
}

// pub const SocketError = error{} || os.linux.IoUring.Error || mem.Allocator.Error;

pub const SocketError = error{};

pub fn createSocket(self: *Self, comptime Context: type, context: *Context, domain: u32, @"type": u32, comptime cb: *const fn (context: *Context, result: SocketError!posix.socket_t) anyerror!void) !void {
    // const Callback = struct {
    //     context: *Context,
    //     cb: *const fn (context: *Context, res: i32) anyerror!void,
    // };
    //
    // const callback = try self.allocator.create(Callback);
    // callback.* = .{
    //     .context = context,
    //     .cb = cb,
    // };

    const callback = try self.allocator.create(Callback);
    callback.* = .{
        .context = context,
        .internal_cb = struct {
            fn do(ctx: *anyopaque, cqe: io_uring_cqe) anyerror!void {
                const provided_context: *Context = @ptrCast(@alignCast(ctx));

                const socket: posix.socket_t = @intCast(cqe.res);

                try cb(provided_context, socket);
            }
        }.do,
    };

    _ = try self.ring.socket(
        @intFromPtr(callback),
        domain,
        @"type",
        0,
        0,
    );
}

const Callback = struct {
    context: *anyopaque,
    internal_cb: *const fn (context: *anyopaque, cqe: io_uring_cqe) anyerror!void,
};
