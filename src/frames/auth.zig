const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

usingnamespace @import("../frame.zig");
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// AUTHENTICATE is sent by a node in response to a STARTUP frame if authentication is required.
///
/// Described in the protocol spec at §4.2.3.
pub const AuthenticateFrame = struct {
    authenticator: []const u8,

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !AuthenticateFrame {
        return AuthenticateFrame{
            .authenticator = try pr.readString(allocator),
        };
    }
};

/// AUTH_RESPONSE is sent to a node to answser a authentication challenge.
///
/// Described in the protocol spec at §4.1.2.
pub const AuthResponseFrame = struct {
    token: ?[]const u8,

    pub fn write(self: @This(), pw: *PrimitiveWriter) !void {
        return pw.writeBytes(self.token);
    }

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !AuthResponseFrame {
        return AuthResponseFrame{
            .token = try pr.readBytes(allocator),
        };
    }
};

/// AUTH_CHALLENGE is a server authentication challenge.
///
/// Described in the protocol spec at §4.2.7.
pub const AuthChallengeFrame = struct {
    token: ?[]const u8,

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !AuthChallengeFrame {
        return AuthChallengeFrame{
            .token = try pr.readBytes(allocator),
        };
    }
};

/// AUTH_SUCCESS indicates the success of the authentication phase.
///
/// Described in the protocol spec at §4.2.8.
const AuthSuccessFrame = struct {
    token: ?[]const u8,

    pub fn read(allocator: *mem.Allocator, pr: *PrimitiveReader) !AuthSuccessFrame {
        return AuthSuccessFrame{
            .token = try pr.readBytes(allocator),
        };
    }
};

test "authenticate frame" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x84\x00\x00\x00\x03\x00\x00\x00\x31\x00\x2f\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x2e\x61\x75\x74\x68\x2e\x50\x61\x73\x73\x77\x6f\x72\x64\x41\x75\x74\x68\x65\x6e\x74\x69\x63\x61\x74\x6f\x72";
    const raw_frame = try testing.readRawFrame(&arena.allocator, exp);

    checkHeader(Opcode.Authenticate, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try AuthenticateFrame.read(&arena.allocator, &pr);

    testing.expectEqualString("org.apache.cassandra.auth.PasswordAuthenticator", frame.authenticator);
}

test "auth challenge frame" {
    // TODO(vincent): how do I get one of these frame ?
}

test "auth response frame" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x04\x00\x00\x02\x0f\x00\x00\x00\x18\x00\x00\x00\x14\x00\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x00\x63\x61\x73\x73\x61\x6e\x64\x72\x61";
    const raw_frame = try testing.readRawFrame(&arena.allocator, exp);

    checkHeader(Opcode.AuthResponse, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try AuthResponseFrame.read(&arena.allocator, &pr);

    const exp_token = "\x00cassandra\x00cassandra";
    testing.expectEqualSlices(u8, exp_token, frame.token.?);

    // write

    testing.expectSameRawFrame(frame, raw_frame.header, exp);
}

test "auth success frame" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    // read

    const exp = "\x84\x00\x00\x02\x10\x00\x00\x00\x04\xff\xff\xff\xff";
    const raw_frame = try testing.readRawFrame(&arena.allocator, exp);

    checkHeader(Opcode.AuthSuccess, exp.len, raw_frame.header);

    var pr = PrimitiveReader.init();
    pr.reset(raw_frame.body);

    const frame = try AuthSuccessFrame.read(&arena.allocator, &pr);

    testing.expect(frame.token == null);
}
