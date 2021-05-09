const std = @import("std");

const testing = @import("testing.zig");

pub const Map = struct {
    const Self = @This();

    const MapType = std.StringHashMap([]const u8);
    const MapEntry = MapType.Entry;
    const Iterator = MapType.Iterator;

    allocator: *std.mem.Allocator,
    map: MapType,

    pub fn init(allocator: *std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .map = MapType.init(allocator),
        };
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !?MapEntry {
        if (self.map.getEntry(key)) |kv| {
            self.allocator.free(kv.value);
            kv.value = value;
            return kv.*;
        } else {
            return try self.map.fetchPut(key, value);
        }
    }

    pub fn count(self: Self) usize {
        return self.map.count();
    }

    pub fn iterator(self: Self) Iterator {
        return self.map.iterator();
    }

    pub fn getEntry(self: Self, key: []const u8) ?*MapEntry {
        return self.map.getEntry(key);
    }
};

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

const EntryList = []const []const u8;

pub const Multimap = struct {
    const Self = @This();

    const MapType = std.StringHashMap(EntryList);

    map: MapType,

    const KV = struct {
        key: []const u8,
        value: EntryList,
    };

    const Iterator = struct {
        map_it: MapType.Iterator,

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
            .map = std.StringHashMap(EntryList).init(allocator),
        };
    }

    pub fn put(self: *Self, key: []const u8, values: []const []const u8) !void {
        _ = try self.map.put(key, values);
    }

    pub fn get(self: Self, key: []const u8) ?[]const []const u8 {
        if (self.map.getEntry(key)) |entry| {
            return entry.value;
        } else {
            return null;
        }
    }

    pub fn count(self: Self) usize {
        return self.map.count();
    }

    pub fn iterator(self: Self) Iterator {
        return Iterator{
            .map_it = self.map.iterator(),
        };
    }
};

test "map" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();
    const allocator = &arena.allocator;

    var m = Map.init(allocator);

    {
        const dupe = std.mem.dupe;

        const k1 = try dupe(allocator, u8, "foo");
        const k2 = try dupe(allocator, u8, "bar");

        const v1 = try dupe(allocator, u8, "bar");
        const v2 = try dupe(allocator, u8, "heo");
        const v3 = try dupe(allocator, u8, "baz");

        _ = try m.put(k1, v1);
        _ = try m.put(k1, v2);
        _ = try m.put(k2, v3);
    }

    try testing.expectEqual(@as(usize, 2), m.count());

    try testing.expectEqualStrings("heo", m.getEntry("foo").?.value);
    try testing.expectEqualStrings("baz", m.getEntry("bar").?.value);
}

test "multimap" {
    var arena = testing.arenaAllocator();
    defer arena.deinit();
    const allocator = &arena.allocator;

    var m = Multimap.init(allocator);

    const dupe = std.mem.dupe;

    {
        const k1 = try dupe(allocator, u8, "foo");
        const v1 = &[_][]const u8{ "bar", "baz" };
        _ = try m.put(k1, v1);
    }

    {
        const k2 = try dupe(allocator, u8, "fou");
        const v2 = &[_][]const u8{ "bar", "baz" };
        _ = try m.put(k2, v2);
    }

    try testing.expectEqual(@as(usize, 2), m.count());

    var it = m.iterator();
    while (it.next()) |entry| {
        try testing.expect(std.mem.eql(u8, "foo", entry.key) or std.mem.eql(u8, "fou", entry.key));

        const slice = entry.value;
        try testing.expectEqualStrings("bar", slice[0]);
        try testing.expectEqualStrings("baz", slice[1]);
    }

    const slice = m.get("foo").?;
    try testing.expectEqualStrings("bar", slice[0]);
    try testing.expectEqualStrings("baz", slice[1]);

    const slice2 = m.get("fou").?;
    try testing.expectEqualStrings("bar", slice[0]);
    try testing.expectEqualStrings("baz", slice[1]);
}
