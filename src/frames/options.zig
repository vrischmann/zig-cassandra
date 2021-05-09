const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

usingnamespace @import("../frame.zig");
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// OPTIONS is sent to a node to ask which STARTUP options are supported.
///
/// Described in the protocol spec at §4.1.3.
pub const OptionsFrame = struct {};

test "options frame" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();

    const data = "\x04\x00\x00\x05\x05\x00\x00\x00\x00";
    const raw_frame = try testing.readRawFrame(&arena.allocator, data);

    try checkHeader(Opcode.Options, data.len, raw_frame.header);
}
