const std = @import("std");
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;

const c = @cImport(@cInclude("lz4.h"));
const testutils = @import("testutils.zig");

const length_size = 4;

pub fn compress(allocator: mem.Allocator, data: []const u8) ![]const u8 {
    const max_dst_size = c.LZ4_compressBound(@intCast(data.len));

    var buf = try allocator.alloc(u8, @as(usize, @intCast(max_dst_size)) + length_size);
    errdefer allocator.free(buf);

    // Encode the uncompressed length before the compressed byted.
    mem.writeInt(u32, buf[0..length_size], @intCast(data.len), .big);

    const compressed_bytes = buf[length_size..buf.len];

    const compressed_data_size = c.LZ4_compress_default(
        @as([*c]const u8, @ptrCast(data)),
        @as([*c]u8, @ptrCast(compressed_bytes)),
        @intCast(data.len),
        max_dst_size,
    );
    if (compressed_data_size <= 0) {
        return error.CompressionFailed;
    }

    buf = try allocator.realloc(buf, @as(usize, @intCast(compressed_data_size)) + length_size);

    return buf;
}

pub fn decompress(allocator: mem.Allocator, data: []const u8) ![]const u8 {
    // The uncompressed length is encoded as 4 bytes before the compressed byted.
    const max_decompressed_size = mem.readInt(u32, data[0..length_size], .big);

    // The rest are the compressed bytes.
    const compressed_data = data[length_size..];

    const buf = try allocator.alloc(u8, max_decompressed_size);
    errdefer allocator.free(buf);

    const decompressed_size = c.LZ4_decompress_safe(
        @as([*c]const u8, @ptrCast(compressed_data)),
        @as([*c]u8, @ptrCast(buf)),
        @intCast(compressed_data.len),
        @intCast(buf.len),
    );
    if (decompressed_size < 0) {
        return error.DecompressionFailed;
    }

    assert(max_decompressed_size == decompressed_size);

    return buf[0..@intCast(decompressed_size)];
}

test "lz4: compress and decompress" {
    const exp = "Dolorem in eos repellat facilis voluptatum sed. Autem ipsum quaerat voluptas ut cum impedit. Ut sapiente dolor eos sit. Dolorum nihil nobis voluptas est et sunt voluptatem. Veniam labore quae explicabo." ** 100;

    const compressed = try compress(testing.allocator, exp);
    defer testing.allocator.free(compressed);

    const decompressed = try decompress(testing.allocator, compressed);
    defer testing.allocator.free(decompressed);

    try testing.expectEqualStrings(exp, decompressed);
}
