const Builder = @import("std").build.Builder;

pub fn build(b: *Builder) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();

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
    cli.setTarget(target);
    cli.setBuildMode(mode);
    cli.install();

    const run_cmd = cli.run();
    run_cmd.step.dependOn(b.getInstallStep());

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);
}
