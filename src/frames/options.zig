const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// OPTIONS is sent to a node to ask which STARTUP options are supported.
///
/// Described in the protocol spec at §4.1.3.
const OptionsFrame = struct {};

test "options frame" {
    const data = "\x04\x00\x00\x05\x05\x00\x00\x00\x00";
    var fbs = std.io.fixedBufferStream(data);
    var in_stream = fbs.inStream();

    var framer = Framer(@TypeOf(in_stream)).init(testing.allocator, in_stream);
    _ = try framer.readHeader();

    checkHeader(Opcode.Options, data.len, framer.header);
}
