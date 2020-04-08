const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const net = std.net;
const ArrayList = std.ArrayList;

usingnamespace @import("primitive_types.zig");
usingnamespace @import("value.zig");

const Framer = @import("framer.zig").Framer;
const sm = @import("string_map.zig");

pub const QueryParameters = struct {
    const Self = @This();

    consistency_level: Consistency,
    values: ?Values,
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

    pub fn read(allocator: *mem.Allocator, comptime FramerType: type, framer: *FramerType) !QueryParameters {
        var params = QueryParameters{
            .consistency_level = undefined,
            .values = null,
            .page_size = null,
            .paging_state = null,
            .serial_consistency_level = null,
            .timestamp = null,
            .keyspace = null,
            .now_in_seconds = null,
        };

        params.consistency_level = try framer.readConsistency();

        // The remaining data in the frame depends on the flags

        // The size of the flags bitmask depends on the protocol version.
        var flags: u32 = 0;
        if (framer.header.version == ProtocolVersion.V5) {
            flags = try framer.readInt(u32);
        } else {
            flags = try framer.readInt(u8);
        }

        if (flags & FlagWithValues == FlagWithValues) {
            const n = try framer.readInt(u16);

            if (flags & FlagWithNamedValues == FlagWithNamedValues) {
                var list = std.ArrayList(NamedValue).init(allocator);
                errdefer list.deinit();

                var i: usize = 0;
                while (i < @as(usize, n)) : (i += 1) {
                    const nm = NamedValue{
                        .name = try framer.readString(),
                        .value = try framer.readValue(),
                    };
                    _ = try list.append(nm);
                }

                params.values = Values{ .Named = list.toOwnedSlice() };
            } else {
                var list = std.ArrayList(Value).init(allocator);
                errdefer list.deinit();

                var i: usize = 0;
                while (i < @as(usize, n)) : (i += 1) {
                    const value = try framer.readValue();
                    _ = try list.append(value);
                }

                params.values = Values{ .Normal = list.toOwnedSlice() };
            }
        }
        if (flags & FlagSkipMetadata == FlagSkipMetadata) {
            // Nothing to do
        }
        if (flags & FlagWithPageSize == FlagWithPageSize) {
            params.page_size = try framer.readInt(u32);
        }
        if (flags & FlagWithPagingState == FlagWithPagingState) {
            params.paging_state = try framer.readBytes();
        }
        if (flags & FlagWithSerialConsistency == FlagWithSerialConsistency) {
            const consistency_level = try framer.readConsistency();
            if (consistency_level != .Serial and consistency_level != .LocalSerial) {
                return error.InvalidSerialConsistency;
            }
            params.serial_consistency_level = consistency_level;
        }
        if (flags & FlagWithDefaultTimestamp == FlagWithDefaultTimestamp) {
            const timestamp = try framer.readInt(u64);
            if (timestamp < 0) {
                return error.InvalidNegativeTimestamp;
            }
            params.timestamp = timestamp;
        }

        if (framer.header.version != ProtocolVersion.V5) {
            return params;
        }

        // The following flags are only valid with protocol v5
        if (flags & FlagWithKeyspace == FlagWithKeyspace) {
            params.keyspace = try framer.readString();
        }
        if (flags & FlagWithNowInSeconds == FlagWithNowInSeconds) {
            params.now_in_seconds = try framer.readInt(u32);
        }

        return params;
    }
};
