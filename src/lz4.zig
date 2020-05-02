const std = @import("std");
const mem = std.mem;

const c = @cImport(@cInclude("lz4.h"));
const testing = @import("testing.zig");

const length_size = 4;

pub fn compress(allocator: *mem.Allocator, data: []const u8) ![]const u8 {
    const max_dst_size = c.LZ4_compressBound(@intCast(c_int, data.len));

    const buf = try allocator.alloc(u8, @intCast(usize, max_dst_size) + length_size);
    errdefer allocator.free(buf);

    // Encode the uncompressed length before the compressed byted.
    mem.writeIntBig(u32, buf[0..length_size], @intCast(u32, data.len));

    var compressed_bytes = buf[length_size..buf.len];

    const compressed_data_size = c.LZ4_compress_default(
        @ptrCast([*c]const u8, data),
        @ptrCast([*c]u8, compressed_bytes),
        @intCast(c_int, data.len),
        max_dst_size,
    );
    if (compressed_data_size <= 0) {
        return error.CompressionFailed;
    }

    return buf[0 .. @intCast(usize, compressed_data_size) + length_size];
}

pub fn decompress(allocator: *mem.Allocator, data: []const u8) ![]const u8 {
    // The uncompressed length is encoded as 4 bytes before the compressed byted.
    var max_decompressed_size = mem.readIntBig(u32, data[0..length_size]);

    // The rest are the compressed bytes.
    const compressed_data = data[length_size..];

    const buf = try allocator.alloc(u8, max_decompressed_size);
    errdefer allocator.free(buf);

    const decompressed_size = c.LZ4_decompress_safe(
        @ptrCast([*c]const u8, compressed_data),
        @ptrCast([*c]u8, buf),
        @intCast(c_int, compressed_data.len),
        @intCast(c_int, buf.len),
    );
    if (decompressed_size < 0) {
        return error.DecompressionFailed;
    }

    return buf[0..@intCast(usize, decompressed_size)];
}

test "lz4: compress and decompress" {
    const exp = "Dolorem in eos repellat facilis voluptatum sed. Autem ipsum quaerat voluptas ut cum impedit. Ut sapiente dolor eos sit. Dolorum nihil nobis voluptas est et sunt voluptatem. Veniam labore quae explicabo." ** 10;

    const compressed = try compress(testing.allocator, exp);
    defer testing.allocator.free(compressed);

    const decompressed = try decompress(testing.allocator, compressed);
    defer testing.allocator.free(decompressed);

    testing.expectEqualStrings(exp, decompressed);
}
