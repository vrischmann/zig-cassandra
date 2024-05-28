const std = @import("std");
const builtin = @import("builtin");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Define options

    const with_snappy = b.option(bool, "with_snappy", "Enable Snappy compression") orelse false;
    const with_cassandra = b.option(bool, "with_cassandra", "Run tests which need a Cassandra node running to work.") orelse false;

    // LZ4
    //
    // To make cross compiling easier we embed the lz4 source code which is small enough and is easily compiled
    // with Zig's C compiling abilities.

    const lz4 = b.addStaticLibrary(.{
        .name = "lz4",
        .target = target,
        .optimize = optimize,
    });
    lz4.linkLibC();
    // lz4 is broken with -fsanitize=pointer-overflow which is added automatically by Zig with -fsanitize=undefined.
    // See here what this flag does: https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html
    lz4.addCSourceFile(.{
        .file = b.path("src/lz4.c"),
        .flags = &[_][]const u8{ "-std=c99", "-fno-sanitize=pointer-overflow" },
    });
    lz4.addIncludePath(b.path("src"));

    var lz4_tests = b.addTest(.{
        .name = "lz4_tests",
        .root_source_file = b.path("src/lz4.zig"),
        .target = target,
        .optimize = optimize,
    });
    lz4_tests.linkLibrary(lz4);
    lz4_tests.addIncludePath(b.path("src"));

    const lz4_test_step = b.step("lz4-test", "Run the lz4 tests");
    lz4_test_step.dependOn(&lz4_tests.step);

    // Snappy
    if (with_snappy) {
        var snappy_tests = b.addTest(.{
            .name = "snappy_tests",
            .root_source_file = b.path("src/snappy.zig"),
            .target = target,
            .optimize = optimize,
        });
        snappy_tests.linkLibC();
        snappy_tests.linkSystemLibrary("snappy");

        const snappy_test_step = b.step("snappy-test", "Run the snappy tests");
        snappy_test_step.dependOn(&snappy_tests.step);
    }

    //
    // Create the public 'cassandra' module
    //

    const module_options = b.addOptions();
    module_options.addOption(bool, "with_snappy", with_snappy);

    const module = b.addModule("cassandra", .{
        .root_source_file = b.path("src/lib.zig"),
        .link_libc = true,
        .target = target,
        .optimize = optimize,
    });
    module.addIncludePath(b.path("src"));
    module.linkLibrary(lz4);
    if (with_snappy) {
        module.linkSystemLibrary("snappy", .{});
    }
    module.addImport("build_options", module_options.createModule());

    //
    // Add the main tests for the library.
    //

    var main_tests = b.addTest(.{
        .name = "main",
        .target = target,
        .optimize = optimize,
        .root_source_file = b.path("src/lib.zig"),
    });
    main_tests.linkLibrary(lz4);
    main_tests.addIncludePath(b.path("src"));

    if (with_snappy) {
        main_tests.linkLibC();
        main_tests.linkSystemLibrary("snappy");
    }

    const main_tests_options = b.addOptions();
    main_tests.root_module.addImport("build_options", main_tests_options.createModule());
    main_tests_options.addOption(bool, "with_cassandra", with_cassandra);
    main_tests_options.addOption(bool, "with_snappy", with_snappy);

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
    example.linkLibrary(lz4);
    example.addIncludePath(b.path("src"));
    example.root_module.addImport("cassandra", module);
    example.root_module.addImport("build_options", module_options.createModule());

    if (with_snappy) {
        example.linkLibC();
        example.linkSystemLibrary("snappy");
    }

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
