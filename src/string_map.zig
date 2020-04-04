const std = @import("std");

const testing = @import("testing.zig");

pub const Map = struct {
    const Self = @This();

    const MapType = std.StringHashMap([]const u8);
    const KV = MapType.KV;
    const Iterator = MapType.Iterator;

    allocator: *std.mem.Allocator,
    map: MapType,

    pub fn init(allocator: *std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .map = MapType.init(allocator),
        };
    }

    pub fn deinit(self: *const Self) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key);
            self.allocator.free(entry.value);
        }
        self.map.deinit();
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !?KV {
        if (self.map.get(key)) |kv| {
            self.allocator.free(kv.value);
            kv.value = value;
            return kv.*;
        } else {
            return try self.map.put(key, value);
        }
    }

    pub fn count(self: *const Self) usize {
        return self.map.count();
    }

    pub fn iterator(self: *const Self) Iterator {
        return self.map.iterator();
    }

    pub fn get(self: *const Self, key: []const u8) ?*KV {
        return self.map.get(key);
    }
};

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

const EntryList = std.ArrayList([]const u8);

pub const Multimap = struct {
    const Self = @This();

    const MapType = std.StringHashMap(EntryList);

    allocator: *std.mem.Allocator,
    map: MapType,

    const KV = struct {
        key: []const u8,
        value: EntryList,
    };

    // TODO(vincent): probably can remove this since the multimap
    // is only used in the SupportedFrame and we know the keys beforehand
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
            .allocator = allocator,
            .map = std.StringHashMap(EntryList).init(allocator),
        };
    }

    pub fn deinit(self: *const Self) void {
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

    pub fn put(self: *Self, key: []const u8, values: std.ArrayList([]const u8)) !void {
        _ = try self.map.put(key, values);
    }

    pub fn get(self: *const Self, key: []const u8) ?[][]const u8 {
        if (self.map.get(key)) |entry| {
            return entry.value.span();
        } else {
            return null;
        }
    }

    pub fn count(self: *const Self) usize {
        return self.map.count();
    }

    pub fn iterator(self: *const Self) Iterator {
        return Iterator{
            .map_it = self.map.iterator(),
        };
    }
};

test "map" {
    var m = Map.init(testing.allocator);
    defer m.deinit();

    {
        const dupe = std.mem.dupe;

        const k1 = try dupe(testing.allocator, u8, "foo");
        const k2 = try dupe(testing.allocator, u8, "bar");

        const v1 = try dupe(testing.allocator, u8, "bar");
        const v2 = try dupe(testing.allocator, u8, "heo");
        const v3 = try dupe(testing.allocator, u8, "baz");

        _ = try m.put(k1, v1);
        _ = try m.put(k1, v2);
        _ = try m.put(k2, v3);
    }

    testing.expectEqual(@as(usize, 2), m.count());

    testing.expectEqualString("heo", m.get("foo").?.value);
    testing.expectEqualString("baz", m.get("bar").?.value);
}

test "multimap" {
    var m = Multimap.init(testing.allocator);
    defer m.deinit();

    const dupe = std.mem.dupe;

    {
        const k1 = try dupe(testing.allocator, u8, "foo");
        var v1 = std.ArrayList([]const u8).init(testing.allocator);
        _ = try v1.append(try dupe(testing.allocator, u8, "bar"));
        _ = try v1.append(try dupe(testing.allocator, u8, "baz"));
        _ = try m.put(k1, v1);
    }

    {
        const k2 = try dupe(testing.allocator, u8, "fou");
        var v2 = std.ArrayList([]const u8).init(testing.allocator);
        _ = try v2.append(try dupe(testing.allocator, u8, "bar"));
        _ = try v2.append(try dupe(testing.allocator, u8, "baz"));
        _ = try m.put(k2, v2);
    }

    testing.expectEqual(@as(usize, 2), m.count());

    var it = m.iterator();
    while (it.next()) |entry| {
        testing.expect(std.mem.eql(u8, "foo", entry.key) or std.mem.eql(u8, "fou", entry.key));

        const slice = entry.value.span();
        testing.expectEqualString("bar", slice[0]);
        testing.expectEqualString("baz", slice[1]);
    }

    const slice = m.get("foo").?;
    testing.expectEqualString("bar", slice[0]);
    testing.expectEqualString("baz", slice[1]);

    const slice2 = m.get("fou").?;
    testing.expectEqualString("bar", slice[0]);
    testing.expectEqualString("baz", slice[1]);
}
