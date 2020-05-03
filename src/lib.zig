const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

pub const CompressionAlgorithm = @import("client.zig").CompressionAlgorithm;

pub const InitOptions = @import("client.zig").InitOptions;
pub const QueryOptions = @import("client.zig").QueryOptions;

pub const Client = @import("client.zig").Client;
pub const TCPClient = @import("client.zig").TCPClient;
pub const Iterator = @import("iterator.zig").Iterator;

const testing = @import("testing.zig");

test "" {
    _ = @import("client.zig");
    _ = @import("frame.zig");
    _ = @import("iterator.zig");
}
