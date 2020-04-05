const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// AUTHENTICATE is sent by a node in response to a STARTUP frame if authentication is required.
///
/// Described in the protocol spec at §4.2.3.
const AuthenticateFrame = struct {
    const Self = @This();

    allocator: *mem.Allocator,

    authenticator: []const u8,

    pub fn deinit(self: Self) void {
        self.allocator.free(self.authenticator);
    }

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !Self {
        var frame = Self{
            .allocator = allocator,
            .authenticator = undefined,
        };

        frame.authenticator = try framer.readString();

        return frame;
    }
};

test "authenticate frame" {
    const data = "\x84\x00\x00\x00\x03\x00\x00\x00\x31\x00\x2f\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x63\x61\x73\x73\x61\x6e\x64\x72\x61\x2e\x61\x75\x74\x68\x2e\x50\x61\x73\x73\x77\x6f\x72\x64\x41\x75\x74\x68\x65\x6e\x74\x69\x63\x61\x74\x6f\x72";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Authenticate, data.len, framer.header);

    const frame = try AuthenticateFrame.read(testing.allocator, @TypeOf(framer), &framer);
    defer frame.deinit();

    testing.expectEqualString("org.apache.cassandra.auth.PasswordAuthenticator", frame.authenticator);
}
