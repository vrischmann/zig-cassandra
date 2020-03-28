const std = @import("std");
const testing = std.testing;

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

const EntryList = std.ArrayList([]const u8);

pub const Multimap = struct {
    const Self = @This();

    allocator: *std.mem.Allocator,
    map: std.StringHashMap(EntryList),

    const KV = struct {
        key: []const u8,
        value: EntryList,
    };

    const Iterator = struct {
        map_it: std.StringHashMap(EntryList).Iterator,

        pub fn next(it: *Iterator) ?KV {
            if (it.map_it.next()) |entry| {
                return KV{
                    .key = entry.key,
                    .value = entry.value,
                };
            }

            return null;
        }
    };

    pub fn init(allocator: *std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .map = std.StringHashMap(EntryList).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key);
            for (entry.value.span()) |s| {
                self.allocator.free(s);
            }
            entry.value.deinit();
        }
        self.map.deinit();
    }

    pub fn count(self: *Self) usize {
        return self.map.count();
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        const value_dup = try std.mem.dupe(self.allocator, u8, value);
        errdefer self.allocator.free(value_dup);

        if (self.map.get(key)) |kv| {
            _ = try kv.value.append(value_dup);
        } else {
            const key_dup = try std.mem.dupe(self.allocator, u8, key);
            errdefer self.allocator.free(key_dup);

            var list = EntryList.init(self.allocator);
            errdefer list.deinit();

            _ = try list.append(value_dup);
            _ = try self.map.put(key_dup, list);
        }
    }

    pub fn iterator(self: *Self) Iterator {
        return Iterator{
            .map_it = self.map.iterator(),
        };
    }
};

test "multimap" {
    var m = Multimap.init(testing.allocator);
    defer m.deinit();

    _ = try m.put("foo", "bar");
    _ = try m.put("foo", "baz");
    _ = try m.put("fou", "bar");
    _ = try m.put("fou", "baz");

    testing.expectEqual(@as(usize, 2), m.count());

    var it = m.iterator();
    while (it.next()) |entry| {
        testing.expect(std.mem.eql(u8, "foo", entry.key) or std.mem.eql(u8, "fou", entry.key));

        const slice = entry.value.span();
        testing.expectEqualSlices(u8, "bar", slice[0]);
        testing.expectEqualSlices(u8, "baz", slice[1]);
    }
}
