const std = @import("std");
const builtin = @import("builtin");

fn setupTools(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) void {
    const hex_convert_mod = b.createModule(.{
        .root_source_file = b.path("tools/hex-convert.zig"),
        .target = target,
        .optimize = optimize,
    });

    const hex_convert = b.addExecutable(.{
        .name = "hex-convert",
        .root_module = hex_convert_mod,
    });

    const hex_convert_install_artifact = b.addInstallArtifact(hex_convert, .{});
    b.getInstallStep().dependOn(&hex_convert_install_artifact.step);

    const hex_convert_run_cmd = b.addRunArtifact(hex_convert);
    hex_convert_run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        hex_convert_run_cmd.addArgs(args);
    }

    const hex_convert_run = b.step("hex-convert", "Run the `hex-convert` tool");
    hex_convert_run.dependOn(&hex_convert_run_cmd.step);
}

fn setupCqldebug(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, cassandra_mod: *std.Build.Module) void {
    const cqldebug_mod = b.createModule(.{
        .root_source_file = b.path("cqldebug/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    const cqldebug_options = b.addOptions();

    cqldebug_mod.addImport("cassandra", cassandra_mod);
    if (b.lazyDependency("linenoise", .{
        .target = target,
        .optimize = optimize,
    })) |dep| {
        cqldebug_mod.addImport("linenoise", dep.module("linenoise"));
    }
    cqldebug_mod.addImport("build_options", cqldebug_options.createModule());

    const cqldebug = b.addExecutable(.{
        .name = "cqldebug",
        .root_module = cqldebug_mod,
    });

    const cqldebug_install_artifact = b.addInstallArtifact(cqldebug, .{});
    b.getInstallStep().dependOn(&cqldebug_install_artifact.step);

    const cqldebug_run_cmd = b.addRunArtifact(cqldebug);
    cqldebug_run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        cqldebug_run_cmd.addArgs(args);
    }

    const cqldebug_run = b.step("cqldebug", "Run cqldebug");
    cqldebug_run.dependOn(&cqldebug_run_cmd.step);
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const snappy_dep = b.dependency("libsnappy", .{
        .target = target,
        .optimize = optimize,
    });
    const snappy_mod = snappy_dep.module("snappy");

    const lz4_dep = b.dependency("lz4", .{
        .target = target,
        .optimize = optimize,
    });
    const lz4_artifact = lz4_dep.artifact("lz4");

    // Define options

    const with_cassandra = b.option(bool, "with_cassandra", "Run tests which need a Cassandra node running to work.") orelse false;
    const enable_tracing = b.option(bool, "enable_tracing", "Enable tracing") orelse false;
    const enable_logging = b.option(bool, "enable_logging", "Enable logging") orelse false;

    //
    // Create the public 'cassandra' module
    //

    const module = b.addModule("cassandra", .{
        .root_source_file = b.path("lib.zig"),
        .link_libc = true,
        .target = target,
        .optimize = optimize,
    });
    module.linkLibrary(lz4_artifact);

    const module_options = b.addOptions();
    module_options.addOption(bool, "enable_tracing", enable_tracing);
    module_options.addOption(bool, "enable_logging", enable_logging);
    module_options.addOption(bool, "with_cassandra", with_cassandra);

    module.addImport("build_options", module_options.createModule());
    module.addImport("snappy", snappy_mod);

    //
    // Add the main tests for the library.
    //

    var main_tests_mod = b.createModule(.{
        .root_source_file = b.path("lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    main_tests_mod.linkLibrary(lz4_artifact);

    const main_tests_options = b.addOptions();
    main_tests_options.addOption(bool, "enable_tracing", true);
    main_tests_options.addOption(bool, "enable_logging", true);
    main_tests_options.addOption(bool, "with_cassandra", with_cassandra);

    main_tests_mod.addImport("build_options", main_tests_options.createModule());
    main_tests_mod.addImport("snappy", snappy_mod);

    const main_tests = b.addTest(.{
        .name = "main",
        .root_module = main_tests_mod,
        .filters = b.args orelse &.{},
    });
    main_tests.linkLibC();

    const run_main_tests = b.addRunArtifact(main_tests);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&run_main_tests.step);

    setupCqldebug(b, target, optimize, module);
    setupTools(b, target, optimize);
}
