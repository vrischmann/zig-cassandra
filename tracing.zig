const std = @import("std");
const debug = std.debug;
const fmt = std.fmt;

const protocol = @import("protocol.zig");
const Envelope = protocol.Envelope;
const Message = protocol.Message;

/// A tracing event that contains useful information for observability or debugging.
pub const Event = union(enum) {
    message_read: struct {
        data: []const u8,
        envelope: Envelope,
        message: Message,

        pub fn jsonStringify(self: @This(), jw: anytype) !void {
            try jw.beginObject();

            try jw.objectField("data");
            try jw.print("\"{s}\"", .{fmt.fmtSliceHexLower(self.data)});

            try jw.objectField("envelope");
            try stringifyEnvelope(self.envelope, jw);

            try jw.objectField("message");
            try stringifyMessage(self.message, jw);

            try jw.endObject();
        }
    },
    message_written: struct {
        data: []const u8,
        envelope: Envelope,
        message: Message,

        pub fn jsonStringify(self: @This(), jw: anytype) !void {
            try jw.beginObject();

            try jw.objectField("data");
            try jw.print("\"{s}\"", .{fmt.fmtSliceHexLower(self.data)});

            try jw.objectField("envelope");
            try stringifyEnvelope(self.envelope, jw);

            try jw.objectField("message");
            try stringifyMessage(self.message, jw);

            try jw.endObject();
        }
    },
    handshake_state_transition: struct {
        from: []const u8,
        to: []const u8,
    },

    fn stringifyEnvelope(envelope: Envelope, jw: anytype) !void {
        try jw.beginObject();

        try jw.objectField("header");
        {
            try jw.beginObject();

            try jw.objectField("version");
            try jw.write(envelope.header.version);
            try jw.objectField("flags");
            try jw.write(envelope.header.flags);
            try jw.objectField("stream");
            try jw.write(envelope.header.stream);
            try jw.objectField("opcode");
            try jw.write(envelope.header.opcode);
            try jw.objectField("body_len");
            try jw.write(envelope.header.body_len);

            try jw.endObject();
        }

        if (envelope.tracing_id) |tracing_id| {
            try jw.objectField("tracing_id");
            try jw.print("\"{s}\"", .{fmt.fmtSliceHexLower(&tracing_id)});
        }

        if (envelope.warnings) |warnings| {
            try jw.objectField("warnings");

            try jw.beginArray();

            for (warnings) |warning| {
                try jw.write(warning);
            }

            try jw.endArray();
        }

        if (envelope.custom_payload) |custom_payload| {
            try jw.objectField("custom_payload");

            try jw.beginArray();

            var iter = custom_payload.iterator();
            while (iter.next()) |entry| {
                try jw.beginObject();

                try jw.objectField("key");
                try jw.write(entry.key_ptr);
                try jw.objectField("value");
                try jw.write(entry.value_ptr);

                try jw.endObject();
            }

            try jw.endArray();
        }

        try jw.objectField("body");
        try jw.print("\"{s}\"", .{fmt.fmtSliceHexLower(envelope.body)});

        try jw.endObject();
    }

    fn stringifyMessage(message: Message, jw: anytype) !void {
        try jw.beginObject();

        try jw.objectField("type");
        try jw.write(@tagName(message));

        switch (message) {
            .@"error" => |msg| {
                try jw.objectField("error_code");
                {
                    try jw.beginObject();

                    try jw.objectField("name");
                    try jw.write(@tagName(msg.error_code));

                    try jw.objectField("code");
                    try jw.write(msg.error_code);

                    try jw.endObject();
                }

                try jw.objectField("message");
                try jw.write(msg.message);

                // TODO(vincent): implement all error fields
            },
            .startup => |msg| {
                try jw.objectField("cql_version");
                try jw.print("\"{s}\"", .{msg.cql_version});

                try jw.objectField("compression");
                try jw.write(msg.compression);
            },
            .ready => {},
            .authenticate => |msg| {
                try jw.objectField("authenticator");
                try jw.write(msg.authenticator);
            },
            .options => {},
            .supported => |msg| {
                try jw.objectField("protocol_versions");
                {
                    try jw.beginArray();

                    for (msg.protocol_versions) |protocol_version| {
                        try jw.write(protocol_version);
                    }

                    try jw.endArray();
                }

                try jw.objectField("cql_versions");
                {
                    try jw.beginArray();

                    for (msg.cql_versions) |cql_version| {
                        try jw.print("\"{s}\"", .{cql_version});
                    }

                    try jw.endArray();
                }

                try jw.objectField("compression_algorithms");
                {
                    try jw.beginArray();

                    for (msg.compression_algorithms) |compression_algorithm| {
                        try jw.print("\"{s}\"", .{compression_algorithm});
                    }

                    try jw.endArray();
                }
            },
            // .query => {},
            // .result => {},
            // .prepare => {},
            // .execute => {},
            // .register => {},
            // .event => {},
            // .batch => {},
            // .auth_challenge => {},
            .auth_response => |msg| {
                try jw.objectField("token");
                try jw.write(msg.token);
            },
            .auth_success => |msg| {
                try jw.objectField("token");
                try jw.write(msg.token);
            },
            else => debug.panic("message type {s} not handled", .{@tagName(message)}),
        }

        try jw.endObject();
    }
};

/// Tracer is a generic tracing facility for events that happen on a connection.
/// Mainly useful for observability or debugging.
pub const Tracer = struct {
    context: *const anyopaque,
    traceFn: *const fn (context: *const anyopaque, event: Event) anyerror!void,

    /// Trace a single event that happened.
    pub fn trace(self: *@This(), event: Event) anyerror!void {
        return self.traceFn(self.context, event);
    }
};

const MemoryTracer = struct {
    const EventBuffer = std.fifo.LinearFifo(Event, .{ .Static = 32 });

    events: EventBuffer,

    fn trace(self: *MemoryTracer, event: Event) anyerror!void {
        // TODO(vincent): probably need to dupe the data here
        try self.events.writeItem(event);
    }

    /// Returns a `Tracer` implementation backend by this `MemoryTracer`
    pub fn tracer(self: *MemoryTracer) Tracer {
        return .{
            .context = self,
            .traceFn = struct {
                fn trace(context: *const anyopaque, event: Event) anyerror!void {
                    const ptr: *MemoryTracer = @constCast(@alignCast(@ptrCast(context)));
                    return ptr.trace(event);
                }
            }.trace,
        };
    }
};

/// Returns a memory tracer that stores events in a FIFO buffer.
///
/// Call .tracer() on the `MemoryTracer` to get a `Tracer`.
pub fn memoryTracer() MemoryTracer {
    return .{ .events = MemoryTracer.EventBuffer.init() };
}

pub fn WriterTracer(comptime Writer: type) type {
    return struct {
        const Self = @This();

        writer: Writer,

        fn trace(self: *Self, event: Event) anyerror!void {
            const wrapped_event = .{
                .time = std.time.timestamp(),
                .event = event,
            };

            try std.json.stringify(
                wrapped_event,
                .{
                    .escape_unicode = true,
                },
                self.writer,
            );
            try self.writer.print("\n", .{});
        }

        /// Returns a `Tracer` implementation backend by this `MemoryTracer`
        pub fn tracer(self: *Self) Tracer {
            return .{
                .context = self,
                .traceFn = struct {
                    fn trace(context: *const anyopaque, event: Event) anyerror!void {
                        const ptr: *Self = @constCast(@alignCast(@ptrCast(context)));
                        return ptr.trace(event);
                    }
                }.trace,
            };
        }
    };
}

/// Returns a tracer that writes events to an arbitrary io writer.
///
/// Call .tracer() on the `WriterTracer` to get a `Tracer`.
pub fn writerTracer(writer: anytype) WriterTracer(@TypeOf(writer)) {
    return .{ .writer = writer };
}
