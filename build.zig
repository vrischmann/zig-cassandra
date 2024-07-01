const std = @import("std");
const builtin = @import("builtin");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const snappy_dep = b.dependency("libsnappy", .{
        .target = target,
        .optimize = optimize,
    });
    const snappy = snappy_dep.artifact("snappy");
    const snappy_mod = snappy_dep.module("snappy");

    const lz4_dep = b.dependency("liblz4", .{
        .target = target,
        .optimize = optimize,
    });
    const lz4 = lz4_dep.artifact("lz4");
    const lz4_mod = lz4_dep.module("lz4");

    // Define options

    const with_cassandra = b.option(bool, "with_cassandra", "Run tests which need a Cassandra node running to work.") orelse false;

    //
    // Create the public 'cassandra' module
    //

    const module_options = b.addOptions();

    const module = b.addModule("cassandra", .{
        .root_source_file = b.path("src/lib.zig"),
        .link_libc = true,
        .target = target,
        .optimize = optimize,
    });
    module.addIncludePath(b.path("src"));
    module.linkLibrary(lz4);
    module.linkLibrary(snappy);
    module.addImport("build_options", module_options.createModule());
    module.addImport("lz4", lz4_mod);
    module.addImport("snappy", snappy_mod);

    //
    // Add the main tests for the library.
    //

    var main_tests = b.addTest(.{
        .name = "main",
        .target = target,
        .optimize = optimize,
        .root_source_file = b.path("src/lib.zig"),
    });
    main_tests.addIncludePath(b.path("src"));
    main_tests.linkLibC();
    main_tests.linkLibrary(lz4);
    main_tests.linkLibrary(snappy);

    const main_tests_options = b.addOptions();
    main_tests.root_module.addImport("build_options", main_tests_options.createModule());
    main_tests.root_module.addImport("lz4", lz4_mod);
    main_tests.root_module.addImport("snappy", snappy_mod);
    main_tests_options.addOption(bool, "with_cassandra", with_cassandra);

    const run_main_tests = b.addRunArtifact(main_tests);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&run_main_tests.step);

    //
    // Add the example
    //

    const example = b.addExecutable(.{
        .name = "example",
        .root_source_file = b.path("examples/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    example.addIncludePath(b.path("src"));
    example.linkLibC();
    example.linkLibrary(lz4);
    example.linkLibrary(snappy);
    example.root_module.addImport("cassandra", module);
    example.root_module.addImport("lz4", lz4_mod);
    example.root_module.addImport("snappy", snappy_mod);
    example.root_module.addImport("build_options", module_options.createModule());

    const example_install_artifact = b.addInstallArtifact(example, .{});
    b.getInstallStep().dependOn(&example_install_artifact.step);

    const example_run_cmd = b.addRunArtifact(example);
    example_run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        example_run_cmd.addArgs(args);
    }

    const example_run = b.step("example", "Run the example");
    example_run.dependOn(&example_run_cmd.step);
}
