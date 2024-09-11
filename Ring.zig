const std = @import("std");
const debug = std.debug;
const os = std.os;
const fmt = std.fmt;
const mem = std.mem;
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

pub fn socket(self: *Self, comptime Context: type, context: *Context, domain: u32, @"type": u32, cb: *const fn (context: *Context, res: i32) anyerror!void) void {
    const Callback = struct {
        context: *Context,
        cb: *const fn (context: *Context, res: i32) anyerror!void,
    };

    const callback = try self.allocator.create(Callback);
    callback.* = .{
        .context = context,
        .cb = cb,
    };

    self.ring.socket(
        @intFromPtr(callback),
        domain,
        @"type",
        0,
        0,
    );
}
