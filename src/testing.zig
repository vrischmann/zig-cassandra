const std = @import("std");

pub const allocator = std.testing.allocator;
pub const expect = std.testing.expect;
pub const expectError = std.testing.expectError;
pub const expectEqual = std.testing.expectEqual;
pub const expectEqualSlices = std.testing.expectEqualSlices;

// Temporary function while waiting for Zig to have something like this.
pub fn expectEqualString(a: []const u8, b: []const u8) void {
    if (!std.mem.eql(u8, a, b)) {
        std.debug.panic("expected string \"{}\", got \"{}\"", .{ a, b });
    }
}

pub fn arenaAllocator() std.heap.ArenaAllocator {
    return std.heap.ArenaAllocator.init(std.testing.allocator);
}
