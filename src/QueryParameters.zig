const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;

const message = @import("message.zig");
const CompressionAlgorithm = message.CompressionAlgorithm;
const Consistency = message.Consistency;
const NotSet = message.NotSet;
const OptionID = message.OptionID;
const ProtocolVersion = message.ProtocolVersion;
const Value = message.Value;
const NamedValue = message.NamedValue;
const Values = message.Values;
const MessageWriter = message.MessageWriter;
const MessageReader = message.MessageReader;

const Self = @This();

consistency_level: Consistency,
values: ?Values,
skip_metadata: bool,
page_size: ?u32,
paging_state: ?[]const u8, // NOTE(vincent): not a string
serial_consistency_level: ?Consistency,
timestamp: ?u64,
keyspace: ?[]const u8,
now_in_seconds: ?u32,

const FlagWithValues: u32 = 0x0001;
const FlagSkipMetadata: u32 = 0x0002;
const FlagWithPageSize: u32 = 0x0004;
const FlagWithPagingState: u32 = 0x0008;
const FlagWithSerialConsistency: u32 = 0x0010;
const FlagWithDefaultTimestamp: u32 = 0x0020;
const FlagWithNamedValues: u32 = 0x0040;
const FlagWithKeyspace: u32 = 0x0080;
const FlagWithNowInSeconds: u32 = 0x100;

pub fn write(self: Self, protocol_version: ProtocolVersion, mw: *MessageWriter) !void {
    _ = try mw.writeConsistency(self.consistency_level);

    // Build the flags value

    var flags: u32 = 0;
    if (self.values) |v| {
        flags |= FlagWithValues;
        if (v == .Named) {
            flags |= FlagWithNamedValues;
        }
    }
    if (self.skip_metadata) {
        flags |= FlagSkipMetadata;
    }
    if (self.page_size != null) {
        flags |= FlagWithPageSize;
    }
    if (self.paging_state != null) {
        flags |= FlagWithPagingState;
    }
    if (self.serial_consistency_level != null) {
        flags |= FlagWithSerialConsistency;
    }
    if (self.timestamp != null) {
        flags |= FlagWithDefaultTimestamp;
    }
    if (protocol_version.is(5)) {
        if (self.keyspace != null) {
            flags |= FlagWithKeyspace;
        }
        if (self.now_in_seconds != null) {
            flags |= FlagWithNowInSeconds;
        }
    }

    if (protocol_version.is(5)) {
        _ = try mw.writeInt(u32, flags);
    } else {
        _ = try mw.writeInt(u8, @intCast(flags));
    }

    // Write the remaining body

    if (self.values) |values| {
        switch (values) {
            .Normal => |normal_values| {
                _ = try mw.writeInt(u16, @intCast(normal_values.len));
                for (normal_values) |value| {
                    _ = try mw.writeValue(value);
                }
            },
            .Named => |named_values| {
                _ = try mw.writeInt(u16, @intCast(named_values.len));
                for (named_values) |v| {
                    _ = try mw.writeString(v.name);
                    _ = try mw.writeValue(v.value);
                }
            },
        }
    }

    if (self.page_size) |ps| {
        _ = try mw.writeInt(u32, ps);
    }
    if (self.paging_state) |ps| {
        _ = try mw.writeBytes(ps);
    }
    if (self.serial_consistency_level) |consistency| {
        _ = try mw.writeConsistency(consistency);
    }
    if (self.timestamp) |ts| {
        _ = try mw.writeInt(u64, ts);
    }

    if (!protocol_version.is(5)) {
        return;
    }

    // The following flags are only valid with protocol v5

    if (self.keyspace) |ks| {
        _ = try mw.writeString(ks);
    }
    if (self.now_in_seconds) |s| {
        _ = try mw.writeInt(u32, s);
    }
}

pub fn read(allocator: mem.Allocator, protocol_version: ProtocolVersion, mr: *MessageReader) !Self {
    var params = Self{
        .consistency_level = undefined,
        .values = null,
        .skip_metadata = false,
        .page_size = null,
        .paging_state = null,
        .serial_consistency_level = null,
        .timestamp = null,
        .keyspace = null,
        .now_in_seconds = null,
    };

    params.consistency_level = try mr.readConsistency();

    // The remaining data in the frame depends on the flags

    // The size of the flags bitmask depends on the protocol version.
    var flags: u32 = 0;
    if (protocol_version.is(5)) {
        flags = try mr.readInt(u32);
    } else {
        flags = try mr.readInt(u8);
    }

    if (flags & FlagWithValues == FlagWithValues) {
        const n = try mr.readInt(u16);

        if (flags & FlagWithNamedValues == FlagWithNamedValues) {
            var list = std.ArrayList(NamedValue).init(allocator);
            errdefer list.deinit();

            var i: usize = 0;
            while (i < @as(usize, n)) : (i += 1) {
                const nm = NamedValue{
                    .name = try mr.readString(allocator),
                    .value = try mr.readValue(allocator),
                };
                _ = try list.append(nm);
            }

            params.values = Values{ .Named = try list.toOwnedSlice() };
        } else {
            var list = std.ArrayList(Value).init(allocator);
            errdefer list.deinit();

            var i: usize = 0;
            while (i < @as(usize, n)) : (i += 1) {
                const value = try mr.readValue(allocator);
                _ = try list.append(value);
            }

            params.values = Values{ .Normal = try list.toOwnedSlice() };
        }
    }
    if (flags & FlagSkipMetadata == FlagSkipMetadata) {
        params.skip_metadata = true;
    }
    if (flags & FlagWithPageSize == FlagWithPageSize) {
        params.page_size = try mr.readInt(u32);
    }
    if (flags & FlagWithPagingState == FlagWithPagingState) {
        params.paging_state = try mr.readBytes(allocator);
    }
    if (flags & FlagWithSerialConsistency == FlagWithSerialConsistency) {
        const consistency_level = try mr.readConsistency();
        if (consistency_level != .Serial and consistency_level != .LocalSerial) {
            return error.InvalidSerialConsistency;
        }
        params.serial_consistency_level = consistency_level;
    }
    if (flags & FlagWithDefaultTimestamp == FlagWithDefaultTimestamp) {
        const timestamp = try mr.readInt(u64);
        if (timestamp < 0) {
            return error.InvalidNegativeTimestamp;
        }
        params.timestamp = timestamp;
    }

    if (!protocol_version.is(5)) {
        return params;
    }

    // The following flags are only valid with protocol v5

    if (flags & FlagWithKeyspace == FlagWithKeyspace) {
        params.keyspace = try mr.readString(allocator);
    }
    if (flags & FlagWithNowInSeconds == FlagWithNowInSeconds) {
        params.now_in_seconds = try mr.readInt(u32);
    }

    return params;
}
