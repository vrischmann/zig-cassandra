const std = @import("std");
const big = std.math.big;
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;

const limb_bits = @typeInfo(big.Limb).Int.bits;
const limb_bytes = limb_bits / 8;

/// fromRawBytes decodes a big.int from the data provided.
///
/// Based on https://github.com/golang/go/blob/master/src/math/big/nat.go#L1514-L1534
fn fromRawBytes(allocator: mem.Allocator, data: []const u8) !big.int.Managed {
    const nb_limbs: usize = (data.len + limb_bytes - 1) / limb_bytes;

    var limbs = try allocator.alloc(big.Limb, nb_limbs);
    errdefer allocator.free(limbs);

    // Loop until we don't have enough bytes for a full limb.
    // Decode a full limb per iteration.

    var i: usize = data.len;
    var k: usize = 0;
    while (i >= limb_bytes) : (k += 1) {
        const bytes = data[i - limb_bytes .. i];
        const limb = mem.readInt(big.Limb, bytes[0..8], .big);
        limbs[k] = limb;

        i -= limb_bytes;
    }

    // Decode the remaining limb.

    if (i > 0) {
        var limb: big.Limb = 0;

        var s: u6 = 0;
        while (i > 0) : (s += 8) {
            const n: usize = @intCast(data[i - 1]);
            limb |= n << s;
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
pub fn fromBytes(allocator: mem.Allocator, data: []const u8) !big.int.Managed {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // Get the raw big.int without worrying about the sign.
    const n = try fromRawBytes(arena.allocator(), data);

    // Per the spec, a MSB of 1 in the data means it's negative.

    //
    // Positive number
    //

    if (data.len <= 0 or (data[0] & 0x80 == 0)) {
        return try n.cloneWithDifferentAllocator(allocator);
    }

    //
    // Negative number
    //

    const shift = data.len * 8;
    // Capacity is:
    // * one for the 1 big.int
    // * how many full limbs are in the data slice
    // * one more to store the remaining bytes
    const capacity = big_one.limbs.len + (data.len / limb_bytes) + 1;

    // 1 << shift
    const one_shifted = blk: {
        var one_shifted = try big.int.Managed.initCapacity(arena.allocator(), capacity);
        var one_shifted_mutable = one_shifted.toMutable();

        one_shifted_mutable.shiftLeft(big_one, shift);

        break :blk one_shifted_mutable.toManaged(arena.allocator());
    };

    // res = n - (1 << shift)
    var res = try big.int.Managed.init(allocator);
    try res.sub(&n, &one_shifted);

    return res;
}

/// rawBytes encodes n into buf and returns the number of bytes written.
///
/// Based on https://github.com/golang/go/blob/master/src/math/big/nat.go#L1478-L1504
/// From this method:
///
/// Writes the value of n into buf using big-endian encoding.
/// The value of n is encoded in the slice buf[i:]. If the value of n
/// cannot be represented in buf, bytes panics. The number i of unused
/// bytes at the beginning of buf is returned as result.
fn toRawBytes(buf: []u8, n: big.int.Const) !usize {
    var i: usize = buf.len;
    for (n.limbs) |v| {
        var limb = v;

        var j: usize = 0;
        while (j < limb_bytes) : (j += 1) {
            i -= 1;
            if (i >= 0) {
                buf[i] = @truncate(limb);
            } else if (@as(u8, @truncate(limb)) != 0) {
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
pub fn toBytes(allocator: mem.Allocator, n: big.int.Const) ![]const u8 {
    // This represents 0.
    if (n.limbs.len == 1 and n.limbs[0] == 0) {
        var b = try allocator.alloc(u8, 1);
        b[0] = 0;
        return b;
    }

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var buf = std.ArrayList(u8).init(arena.allocator());

    //
    // Handle positive numbers
    //

    if (n.positive) {
        const result = blk: {
            try buf.resize(n.limbs.len * 8);
            const pos = try toRawBytes(buf.items, n);

            const tmp_result = buf.items[pos..];

            // This is for §6.23 https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec#L1037-L1040
            // Prepend a 0 if the first byte has its most significant bit set.
            if ((tmp_result[0] & 0x80) != 0) {
                break :blk buf.items[pos - 1 ..];
            } else {
                break :blk buf.items[pos..];
            }
        };

        return try allocator.dupe(u8, result);
    }

    //
    // Handle negative numbers
    //

    const shift = (n.bitCountTwosComp() / 8 + 1) * 8;

    // Capacity is:
    // * one for the 1 big.int
    // * how many full limbs are in the data slice
    // * one more to store the remaining bytes
    const capacity = (shift / limb_bytes) + 1;

    // 1 << shift
    const one_shifted = blk: {
        var one_shifted = try big.int.Managed.initCapacity(arena.allocator(), capacity);
        var one_shifted_mutable = one_shifted.toMutable();

        one_shifted_mutable.shiftLeft(big_one, shift);

        break :blk one_shifted_mutable.toManaged(arena.allocator());
    };

    // n = n + (1 << shift)
    var n_tmp = try n.toManaged(arena.allocator());
    try n_tmp.add(&n_tmp, &one_shifted);

    // Encode

    const result = blk: {
        try buf.resize((n_tmp.len() + 1) * limb_bytes);
        const pos = try toRawBytes(buf.items, n_tmp.toConst());

        const tmp_result = buf.items[pos..];

        // This strip operation is taken from here https://github.com/gocql/gocql/blob/34fdeebefcbf183ed7f916f931aa0586fdaa1b40/marshal.go#L1212-L1217
        if (tmp_result.len >= 2 and tmp_result[0] == 0xFF and tmp_result[1] & 0x80 != 0) {
            break :blk tmp_result[1..];
        } else {
            break :blk tmp_result;
        }
    };

    return try allocator.dupe(u8, result);
}

test "bigint: toBytes" {
    const testCase = struct {
        n: []const u8,
        exp_data: []const u8,
    };

    const testCases = [_]testCase{
        testCase{
            .n = "0",
            .exp_data = "\x00",
        },
        testCase{
            .n = "1",
            .exp_data = "\x01",
        },
        testCase{
            .n = "127",
            .exp_data = "\x7F",
        },
        testCase{
            .n = "128",
            .exp_data = "\x00\x80",
        },
        testCase{
            .n = "129",
            .exp_data = "\x00\x81",
        },
        testCase{
            .n = "-1",
            .exp_data = "\xFF",
        },
        testCase{
            .n = "-128",
            .exp_data = "\x80",
        },
        testCase{
            .n = "-129",
            .exp_data = "\xFF\x7F",
        },
        testCase{
            .n = "3405245950896869895938539859386968968953285938539111111111111111111111111111111111111111122222222222222222222222222222222",
            .exp_data = "\x01\x51\x97\x29\x61\xfa\xec\x86\x0d\x18\x06\x93\xd0\x53\xba\xb9\x02\xbd\xde\xe9\xfa\xab\x78\xcf\x52\x2e\x43\x1c\x68\xea\x9e\xb8\xeb\x35\x6e\x96\x31\xfc\x4b\x10\xfc\x9d\x0a\x2b\x4c\x1a\x9c\x8e\x38\xe3\x8e",
        },
        testCase{
            .n = "-3405245950896869895938539859386968968953285938539111111111111111111111111111111111111111122222222222222222222222222222222",
            .exp_data = "\xfe\xae\x68\xd6\x9e\x05\x13\x79\xf2\xe7\xf9\x6c\x2f\xac\x45\x46\xfd\x42\x21\x16\x05\x54\x87\x30\xad\xd1\xbc\xe3\x97\x15\x61\x47\x14\xca\x91\x69\xce\x03\xb4\xef\x03\x62\xf5\xd4\xb3\xe5\x63\x71\xc7\x1c\x72",
        },
    };

    inline for (testCases) |tc| {
        var n = try big.int.Managed.init(testing.allocator);
        defer n.deinit();

        try n.setString(10, tc.n);

        const data = try toBytes(testing.allocator, n.toConst());
        defer testing.allocator.free(data);

        try testing.expectEqualSlices(u8, tc.exp_data, data);
    }
}
//
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

        try testing.expectEqualStrings(tc.exp, formatted_n);
    }
}
