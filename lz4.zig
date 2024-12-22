const std = @import("std");
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;

const c = @cImport(@cInclude("lz4/lz4.h"));

pub const CompressError = error{
    CompressionFailed,
} || mem.Allocator.Error;

pub fn compress(allocator: mem.Allocator, data: []const u8) CompressError![]const u8 {
    const max_dst_size = c.LZ4_compressBound(@intCast(data.len));

    var buf = try allocator.alloc(u8, @as(usize, @intCast(max_dst_size)));
    errdefer allocator.free(buf);

    const compressed_data_size = c.LZ4_compress_default(
        @as([*c]const u8, @ptrCast(data)),
        @as([*c]u8, @ptrCast(buf)),
        @intCast(data.len),
        max_dst_size,
    );
    if (compressed_data_size <= 0) {
        return error.CompressionFailed;
    }

    buf = try allocator.realloc(buf, @intCast(compressed_data_size));

    return buf;
}

pub const DecompressError = error{
    DecompressionFailed,
} || mem.Allocator.Error;

pub fn decompress(allocator: mem.Allocator, compressed_data: []const u8, uncompressed_length: usize) DecompressError![]const u8 {
    const buf = try allocator.alloc(u8, uncompressed_length);
    errdefer allocator.free(buf);

    const n = c.LZ4_decompress_safe(
        @as([*c]const u8, @ptrCast(compressed_data)),
        @as([*c]u8, @ptrCast(buf)),
        @intCast(compressed_data.len),
        @intCast(buf.len),
    );
    if (n < 0) {
        return error.DecompressionFailed;
    }

    assert(n == uncompressed_length);

    return buf[0..@intCast(n)];
}

test "lz4: compress and decompress" {
    const exp = "Dolorem in eos repellat facilis voluptatum sed. Autem ipsum quaerat voluptas ut cum impedit. Ut sapiente dolor eos sit. Dolorum nihil nobis voluptas est et sunt voluptatem. Veniam labore quae explicabo." ** 100;

    const compressed = try compress(testing.allocator, exp);
    defer testing.allocator.free(compressed);

    const decompressed = try decompress(testing.allocator, compressed, exp.len);
    defer testing.allocator.free(decompressed);

    try testing.expectEqualStrings(exp, decompressed);
}
