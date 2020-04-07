const std = @import("std");
const mem = std.mem;

usingnamespace @import("primitive_types.zig");
usingnamespace @import("frames.zig");

const Iterator = struct {
    metadata: RowsMetadata,
    rows: [][]const u8,
};
