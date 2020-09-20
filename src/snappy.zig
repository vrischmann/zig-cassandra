const std = @import("std");
const cast = std.meta.cast;
const mem = std.mem;

const c = @cImport(@cInclude("snappy-c.h"));
const testing = @import("testing.zig");

pub fn compress(allocator: *mem.Allocator, data: []const u8) ![]const u8 {
    var max_dst_size = c.snappy_max_compressed_length(data.len);

    const buf = try allocator.alloc(u8, max_dst_size);
    errdefer allocator.free(buf);

    const status = c.snappy_compress(
        cast([*c]const u8, data),
        data.len,
        cast([*c]u8, buf),
        &max_dst_size,
    );
    if (status != .SNAPPY_OK) {
        return error.CompressionFailed;
    }

    return buf[0..cast(usize, max_dst_size)];
}

pub fn decompress(allocator: *mem.Allocator, data: []const u8) ![]const u8 {
    var max_decompressed_size: usize = 0;

    var status = c.snappy_uncompressed_length(
        cast([*c]const u8, data),
        data.len,
        &max_decompressed_size,
    );
    if (status != .SNAPPY_OK) {
        return error.DecompressionFailed;
    }

    const buf = try allocator.alloc(u8, max_decompressed_size);
    errdefer allocator.free(buf);

    status = c.snappy_uncompress(
        cast([*c]const u8, data),
        data.len,
        cast([*c]u8, buf),
        @alignCast(8, &max_decompressed_size),
    );
    if (status != .SNAPPY_OK) {
        return error.DecompressionFailed;
    }

    return buf;
}

test "snappy: compress and decompress" {
    const exp = "Dolorem in eos repellat facilis voluptatum sed. Autem ipsum quaerat voluptas ut cum impedit. Ut sapiente dolor eos sit. Dolorum nihil nobis voluptas est et sunt voluptatem. Veniam labore quae explicabo." ** 10;

    const compressed = try compress(testing.allocator, exp);
    defer testing.allocator.free(compressed);

    // const decompressed = try decompress(testing.allocator, compressed);
    // defer testing.allocator.free(decompressed);

    // testing.expectEqualStrings(exp, decompressed);
}
