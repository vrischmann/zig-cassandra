const std = @import("std");
const mem = std.mem;
const testing = std.testing;

const c = @cImport(@cInclude("snappy-c.h"));

pub const CompressError = error{
    CompressionFailed,
} || mem.Allocator.Error;

pub fn compress(allocator: mem.Allocator, data: []const u8) CompressError![]const u8 {
    var max_dst_size = c.snappy_max_compressed_length(data.len);

    var buf = try allocator.alloc(u8, max_dst_size);
    errdefer allocator.free(buf);

    const status = c.snappy_compress(
        @as([*c]const u8, @ptrCast(data)),
        data.len,
        @as([*c]u8, @ptrCast(buf)),
        &max_dst_size,
    );
    if (status != c.SNAPPY_OK) {
        return error.CompressionFailed;
    }

    buf = try allocator.realloc(buf, max_dst_size);

    return buf;
}

pub const DecompressError = error{
    DecompressionFailed,
} || mem.Allocator.Error;

pub fn decompress(allocator: mem.Allocator, data: []const u8) DecompressError![]const u8 {
    var max_decompressed_size: usize = 0;

    var status = c.snappy_uncompressed_length(
        @as([*c]const u8, @ptrCast(data)),
        data.len,
        &max_decompressed_size,
    );
    if (status != c.SNAPPY_OK) {
        return error.DecompressionFailed;
    }

    const buf = try allocator.alloc(u8, max_decompressed_size);
    errdefer allocator.free(buf);

    status = c.snappy_uncompress(
        @as([*c]const u8, @ptrCast(data)),
        data.len,
        @as([*c]u8, @ptrCast(buf)),
        @as(*align(8) usize, @alignCast(&max_decompressed_size)),
    );
    if (status != c.SNAPPY_OK) {
        return error.DecompressionFailed;
    }

    return buf;
}

test "compress and decompress" {
    const exp = "Dolorem in eos repellat facilis voluptatum sed. Autem ipsum quaerat voluptas ut cum impedit. Ut sapiente dolor eos sit. Dolorum nihil nobis voluptas est et sunt voluptatem. Veniam labore quae explicabo." ** 100;

    const compressed = try compress(testing.allocator, exp);
    defer testing.allocator.free(compressed);

    const decompressed = try decompress(testing.allocator, compressed);
    defer testing.allocator.free(decompressed);

    try testing.expectEqualStrings(exp, decompressed);
}
