const std = @import("std");
const assert = std.debug.assert;
const big = std.math.big;
const mem = std.mem;

const testing = @import("testing.zig");

const limb_bits = @typeInfo(big.Limb).Int.bits;
const limb_bytes = limb_bits / 8;

/// fromRawBytes decodes a big.int from the data provided.
///
/// Based on https://github.com/golang/go/blob/master/src/math/big/nat.go#L1514-L1534
fn fromRawBytes(allocator: *mem.Allocator, data: []const u8) !big.int.Managed {
    const nb_limbs: usize = (data.len + limb_bytes - 1) / limb_bytes;

    var limbs = try allocator.alloc(big.Limb, nb_limbs);
    errdefer allocator.free(limbs);

    // Loop until we don't have enough bytes for a full limb.
    // Decode a full limb per iteration.

    var i: usize = data.len;
    var k: usize = 0;
    while (i >= limb_bytes) : (k += 1) {
        const limb = mem.readIntSliceBig(big.Limb, data[i - limb_bytes .. i]);
        limbs[k] = limb;

        i -= limb_bytes;
    }

    // Decode the remaining limb.

    if (i > 0) {
        var limb: big.Limb = 0;

        var s: u6 = 0;
        while (i > 0) : (s += 8) {
            limb |= @intCast(usize, data[i - 1]) << s;
            i -= 1;
        }

        limbs[k] = limb;
    }

    assert(i == 0);

    const n = big.int.Mutable{
        .limbs = limbs,
        .len = limbs.len,
        .positive = true,
    };

    return n.toManaged(allocator);
}

// This is used to make a big.int negative below.
const big_one_limbs = [_]big.Limb{1};
const big_one = big.int.Const{
    .limbs = &big_one_limbs,
    .positive = true,
};

/// fromBytes decodes a big-endian two's complement value stored in data into a big.int.Managed.
///
/// Based on https://github.com/gocql/gocql/blob/5378c8f664e946e421b16a490513675b8419bdc7/marshal.go#L1096-L1108
pub fn fromBytes(allocator: *mem.Allocator, data: []const u8) !big.int.Managed {
    // Get the raw big.int without worrying about the sign.
    var n = try fromRawBytes(allocator, data);

    // Per the spec, a MSB of 1 in the data means it's negative.
    if (data.len > 0 and data[0] & 0x80 > 0) {
        const shift = data.len * 8;
        // Capacity is:
        // * one for the 1 big.int
        // * how many full limbs are in the data slice
        // * one more to store the remaining bytes
        const capacity = big_one.limbs.len + (data.len / limb_bytes) + 1;

        // Allocate a temporary big.int to hold the shifted value.
        var tmp = try big.int.Managed.initCapacity(allocator, capacity);
        defer tmp.deinit();
        var tmp_mutable = tmp.toMutable();

        // 1 << shift
        tmp_mutable.shiftLeft(big_one, shift);

        // n = n - (1 << shift)
        try n.sub(n.toConst(), tmp_mutable.toConst());
    }

    return n;
}

/// rawBytes encodes n into buf and returns the number of bytes written.
///
/// Based on https://github.com/golang/go/blob/master/src/math/big/nat.go#L1478-L1504
fn rawBytes(buf: []u8, n: big.int.Const) !usize {
    var i: usize = buf.len;
    for (n.limbs) |v| {
        var limb = v;

        var j: usize = 0;
        while (j < limb_bytes) : (j += 1) {
            i -= 1;
            if (i >= 0) {
                buf[i] = @truncate(u8, limb);
            } else if (@truncate(u8, limb) != 0) {
                return error.BufferTooSmall;
            }
            limb >>= 8;
        }
    }

    if (i < 0) {
        i = 0;
    }
    while (i < buf.len and buf[i] == 0) {
        i += 1;
    }

    return i;
}

// Mostly based on https://github.com/gocql/gocql/blob/5378c8f664e946e421b16a490513675b8419bdc7/marshal.go#L1112.
pub fn toBytes(allocator: *mem.Allocator, n: big.int.Const) ![]const u8 {
    // This represents 0.
    if (n.limbs.len == 1 and n.limbs[0] == 0) {
        var b = try allocator.alloc(u8, 1);
        b[0] = 0;
        return b;
    }

    var buf = try allocator.alloc(u8, n.limbs.len * 8);
    errdefer allocator.free(buf);

    var pos = try rawBytes(buf, n);

    // This is for §6.23 https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec#L1037-L1040

    if (n.positive and (buf[0] & 0x80) > 0) {
        buf[pos - 1] = 0;
        return buf[pos - 1 ..];
    }

    // TODO(vincent): implement the case where it's negative
    unreachable;
}

test "bigint: toBytes" {}

test "bigint: fromBytes" {
    const testCase = struct {
        data: []const u8,
        exp: []const u8,
    };

    const testCases = [_]testCase{
        testCase{
            .data = "\x01\x51\x97\x29\x61\xfa\xec\x86\x0d\x18\x06\x93\xd0\x53\xba\xb9\x02\xbd\xde\xe9\xfa\xab\x78\xcf\x52\x2e\x43\x1c\x68\xea\x9e\xb8\xeb\x35\x6e\x96\x31\xfc\x4b\x10\xfc\x9d\x0a\x2b\x4c\x1a\x9c\x8e\x38\xe3\x8e",
            .exp = "3405245950896869895938539859386968968953285938539111111111111111111111111111111111111111122222222222222222222222222222222",
        },
        testCase{
            .data = "\xfe\xae\x68\xd6\x9e\x05\x13\x79\xf2\xe7\xf9\x6c\x2f\xac\x45\x46\xfd\x42\x21\x16\x05\x54\x87\x30\xad\xd1\xbc\xe3\x97\x15\x61\x47\x14\xca\x91\x69\xce\x03\xb4\xef\x03\x62\xf5\xd4\xb3\xe5\x63\x71\xc7\x1c\x72",
            .exp = "-3405245950896869895938539859386968968953285938539111111111111111111111111111111111111111122222222222222222222222222222222",
        },
    };

    inline for (testCases) |tc| {
        var n = try fromBytes(testing.allocator, tc.data);
        defer n.deinit();

        var buf: [1024]u8 = undefined;
        const formatted_n = try std.fmt.bufPrint(&buf, "{}", .{n});

        testing.expectEqualStrings(tc.exp, formatted_n);
    }
}
