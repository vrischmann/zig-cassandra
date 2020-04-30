const std = @import("std");
const builtin = @import("builtin");
const Builder = std.build.Builder;

pub fn build(b: *Builder) !void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();

    // Verify target

    const cpu_arch = target.cpu_arch orelse builtin.cpu.arch;
    const os_tag = target.os_tag orelse builtin.os.tag;

    switch (cpu_arch) {
        .i386, .x86_64 => {},
        else => |arch| std.debug.panic("cpu architecture {} is not supported yet\n", .{arch}),
    }
    switch (os_tag) {
        .linux, .windows => {},
        else => |tag| std.debug.panic("os tag {} is not supported yet\n", .{tag}),
    }

    // Build library

    const lib = b.addStaticLibrary("zig-cassandra", "src/lib.zig");
    lib.linkLibC();
    lib.linkSystemLibrary("lz4");
    if (os_tag == .windows) try lib.addVcpkgPaths(.Static);
    lib.setTarget(target);
    lib.setBuildMode(mode);
    lib.install();

    var main_tests = b.addTest("src/lib.zig");
    main_tests.linkLibC();
    main_tests.linkSystemLibrary("lz4");
    if (os_tag == .windows) try main_tests.addVcpkgPaths(.Static);
    main_tests.setBuildMode(mode);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);

    // Build CLI

    const cli = b.addExecutable("cqlsh", "src/cli.zig");
    cli.linkLibC();
    cli.linkSystemLibrary("lz4");
    if (os_tag == .windows) try cli.addVcpkgPaths(.Static);
    cli.setTarget(target);
    cli.setBuildMode(mode);
    cli.install();

    const run_cmd = cli.run();
    run_cmd.step.dependOn(b.getInstallStep());

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);
}
