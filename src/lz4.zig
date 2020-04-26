const std = @import("std");
const mem = std.mem;

const c = @import("c.zig");
const testing = @import("testing.zig");

pub fn lz4Compress(allocator: *mem.Allocator, data: []const u8) ![]const u8 {
    const max_dst_size = c.LZ4_compressBound(@intCast(c_int, data.len));

    const buf = try allocator.alloc(u8, @intCast(usize, max_dst_size));
    errdefer allocator.free(buf);

    const compressed_data_size = c.LZ4_compress_default(
        @ptrCast([*c]const u8, data),
        @ptrCast([*c]u8, buf),
        @intCast(c_int, data.len),
        max_dst_size,
    );
    if (compressed_data_size <= 0) {
        return error.CompressionFailed;
    }

    return buf[0..@intCast(usize, compressed_data_size)];
}

pub fn lz4Decompress(allocator: *mem.Allocator, data: []const u8, max_decompressed_size: usize) ![]const u8 {
    const buf = try allocator.alloc(u8, max_decompressed_size);
    errdefer allocator.free(buf);

    const decompressed_size = c.LZ4_decompress_safe(
        @ptrCast([*c]const u8, data),
        @ptrCast([*c]u8, buf),
        @intCast(c_int, data.len),
        @intCast(c_int, buf.len),
    );
    if (decompressed_size <= 0) {
        return error.DecompressionFailed;
    }

    return buf[0..@intCast(usize, decompressed_size)];
}

test "lz4: compress and decompress" {
    const exp = "Dolorem in eos repellat facilis voluptatum sed. Autem ipsum quaerat voluptas ut cum impedit. Ut sapiente dolor eos sit. Dolorum nihil nobis voluptas est et sunt voluptatem. Veniam labore quae explicabo.";

    const compressed = try lz4Compress(testing.allocator, exp);
    defer testing.allocator.free(compressed);

    const decompressed = try lz4Decompress(testing.allocator, compressed, exp.len);
    defer testing.allocator.free(decompressed);

    testing.expectEqualString(exp, decompressed);
}
