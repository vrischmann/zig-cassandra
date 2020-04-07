usingnamespace @import("frames/auth.zig");
usingnamespace @import("frames/batch.zig");
usingnamespace @import("frames/error.zig");
usingnamespace @import("frames/event.zig");
usingnamespace @import("frames/execute.zig");
usingnamespace @import("frames/options.zig");
usingnamespace @import("frames/prepare.zig");
usingnamespace @import("frames/query.zig");
usingnamespace @import("frames/ready.zig");
usingnamespace @import("frames/register.zig");
usingnamespace @import("frames/result.zig");
usingnamespace @import("frames/startup.zig");
usingnamespace @import("frames/supported.zig");

test "" {
    _ = @import("frames/startup.zig");
    _ = @import("frames/options.zig");
    _ = @import("frames/query.zig");
    _ = @import("frames/prepare.zig");
    _ = @import("frames/execute.zig");
    _ = @import("frames/batch.zig");
    _ = @import("frames/register.zig");
    _ = @import("frames/error.zig");
    _ = @import("frames/ready.zig");
    _ = @import("frames/supported.zig");
    _ = @import("frames/result.zig");
    _ = @import("frames/event.zig");
    _ = @import("frames/auth.zig");
}
