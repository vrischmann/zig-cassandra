const std = @import("std");
const builtin = @import("builtin");
const Builder = std.build.Builder;

    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();

    // Verify target, we only support x86_64-linux for now
    const cpu_arch = target.cpu_arch orelse builtin.cpu.arch;
    if (cpu_arch != .x86_64) {
        std.debug.panic("cpu architecture {} is not supported yet\n", .{cpu_arch});
    }
    const os_tag = target.os_tag orelse builtin.os.tag;
    if (os_tag != .linux) {
        std.debug.panic("os tag {} is not supported yet\n", .{os_tag});
    }

    // Build library

    const lib = b.addStaticLibrary("zig-cassandra", "src/lib.zig");
    lib.linkLibC();
    lib.linkSystemLibrary("lz4");
    lib.setTarget(target);
    lib.setBuildMode(mode);
    lib.install();

    var main_tests = b.addTest("src/lib.zig");
    main_tests.linkLibC();
    main_tests.linkSystemLibrary("lz4");
    main_tests.setBuildMode(mode);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);

    // Build CLI

    const cli = b.addExecutable("cqlsh", "src/cli.zig");
    cli.linkLibC();
    cli.linkSystemLibrary("lz4");
    cli.setTarget(target);
    cli.setBuildMode(mode);
    cli.install();

    const run_cmd = cli.run();
    run_cmd.step.dependOn(b.getInstallStep());

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);
}
