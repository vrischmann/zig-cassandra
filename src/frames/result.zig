const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;

const Framer = @import("../framer.zig").Framer;
usingnamespace @import("../primitive_types.zig");
const testing = @import("../testing.zig");

/// RESULT is the result to a query (QUERY, PREPARE, EXECUTE or BATCH messages).
///
/// Described in the protocol spec at §4.2.5.
const ResultFrame = struct {};

test "result frame" {}
