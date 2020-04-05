const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// AUTH_RESPONSE is sent to a node to answser a authentication challenge.
///
/// Described in the protocol spec at §4.1.2.
const AuthResponseFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    token: ?[]const u8,

    pub fn deinit(self: *const Self) void {
        if (self.token) |token| {
            self.allocator.free(token);
        }
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

/// AUTH_CHALLENGE is a server authentication challenge.
///
/// Described in the protocol spec at §4.2.7.
const AuthChallengeFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    token: ?[]const u8,

    pub fn deinit(self: *const Self) void {
        if (self.token) |token| {
            self.allocator.free(token);
        }
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

    token: ?[]const u8,

    pub fn deinit(self: *const Self) void {
        if (self.token) |token| {
            self.allocator.free(token);
        }
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

test "auth response frame" {
    const data = "\x04\x00\x00\x02\x0f\x00\x00\x00\x18\x00\x00\x00\x14\x00\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x00\x63\x61\x73\x73\x61\x6e\x64\x72\x61";

    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.AuthResponse, data.len, framer.header);

    const frame = try AuthSuccessFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    const exp_token = "\x00cassandra\x00cassandra";
    testing.expectEqualSlices(u8, exp_token, frame.token.?);
}

test "auth success frame" {
    const data = "\x84\x00\x00\x02\x10\x00\x00\x00\x04\xff\xff\xff\xff";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.AuthSuccess, data.len, framer.header);

    const frame = try AuthSuccessFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expect(frame.token == null);
}
