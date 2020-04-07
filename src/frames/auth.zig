const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// AUTHENTICATE is sent by a node in response to a STARTUP frame if authentication is required.
///
/// Described in the protocol spec at §4.2.3.
const AuthenticateFrame = struct {
    authenticator: []const u8,

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !AuthenticateFrame {
        return AuthenticateFrame{
            .authenticator = try framer.readString(),
        };
    }
};

/// AUTH_RESPONSE is sent to a node to answser a authentication challenge.
///
/// Described in the protocol spec at §4.1.2.
const AuthResponseFrame = struct {
    token: ?[]const u8,

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !AuthResponseFrame {
        return AuthResponseFrame{
            .token = try framer.readBytes(),
        };
    }
};

/// AUTH_CHALLENGE is a server authentication challenge.
///
/// Described in the protocol spec at §4.2.7.
const AuthChallengeFrame = struct {
    token: ?[]const u8,

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !AuthChallengeFrame {
        return AuthChallengeFrame{
            .token = try framer.readBytes(),
        };
    }
};

/// AUTH_SUCCESS indicates the success of the authentication phase.
///
/// Described in the protocol spec at §4.2.8.
const AuthSuccessFrame = struct {
    token: ?[]const u8,

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !AuthSuccessFrame {
        return AuthSuccessFrame{
            .token = try framer.readBytes(),
        };
    }
};

test "authenticate frame" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x00\x03\x00\x00\x00\x31\x00\x2f\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x2e\x61\x75\x74\x68\x2e\x50\x61\x73\x73\x77\x6f\x72\x64\x41\x75\x74\x68\x65\x6e\x74\x69\x63\x61\x74\x6f\x72";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(&arena.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Authenticate, data.len, framer.header);

    const frame = try AuthenticateFrame.read(&arena.allocator, @TypeOf(framer), &framer);

    testing.expectEqualString("org.apache.cassandra.auth.PasswordAuthenticator", frame.authenticator);
}

test "auth challenge frame" {
    // TODO(vincent): how do I get one of these frame ?
}

test "auth response frame" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x04\x00\x00\x02\x0f\x00\x00\x00\x18\x00\x00\x00\x14\x00\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x00\x63\x61\x73\x73\x61\x6e\x64\x72\x61";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(&arena.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.AuthResponse, data.len, framer.header);

    const frame = try AuthSuccessFrame.read(&arena.allocator, @TypeOf(framer), &framer);

    const exp_token = "\x00cassandra\x00cassandra";
    testing.expectEqualSlices(u8, exp_token, frame.token.?);
}

test "auth success frame" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x84\x00\x00\x02\x10\x00\x00\x00\x04\xff\xff\xff\xff";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(&arena.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.AuthSuccess, data.len, framer.header);

    const frame = try AuthSuccessFrame.read(&arena.allocator, @TypeOf(framer), &framer);

    testing.expect(frame.token == null);
}
