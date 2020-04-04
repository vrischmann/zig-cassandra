const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

const Framer = @import("framer.zig").Framer;
const sm = @import("string_map.zig");
usingnamespace @import("primitive_types.zig");
usingnamespace @import("query_parameters.zig");
const testing = @import("testing.zig");

/// AUTH_CHALLENGE is a server authentication challenge.
///
/// Described in the protocol spec at §4.2.7.
const AuthChallengeFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    token: []const u8,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.token);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .token = undefined,
        };

        frame.token = try framer.readBytes();

        return frame;
    }
};

/// AUTH_SUCCESS indicates the success of the authentication phase.
///
/// Described in the protocol spec at §4.2.8.
const AuthSuccessFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    token: []const u8,

    pub fn deinit(self: *const Self) void {
        self.allocator.free(self.token);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .token = undefined,
        };

        frame.token = try framer.readBytes();

        return frame;
    }
};

test "auth challenge frame" {
    // TODO(vincent): how do I get one of these frame ?
}

test "" {
    _ = @import("frames/startup.zig");
    _ = @import("frames/auth_response.zig");
    _ = @import("frames/options.zig");
    _ = @import("frames/query.zig");
    _ = @import("frames/prepare.zig");
    _ = @import("frames/execute.zig");
    _ = @import("frames/batch.zig");
    _ = @import("frames/register.zig");
    _ = @import("frames/error.zig");
    _ = @import("frames/ready.zig");
    _ = @import("frames/authenticate.zig");
    _ = @import("frames/supported.zig");
    _ = @import("frames/result.zig");
    _ = @import("frames/event.zig");
    // _ = @import("frames/auth_challenge.zig");
    // _ = @import("frames/auth_success.zig");
}
