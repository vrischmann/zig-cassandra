const std = @import("std");
const builtin = @import("builtin");
const Builder = std.build.Builder;

fn maybeLinkSnappy(obj: *std.build.LibExeObjStep, with_snappy: bool) void {
    obj.addBuildOption(bool, "with_snappy", with_snappy);
    if (!with_snappy) return;

    linkSnappy(obj);
}

fn linkSnappy(obj: *std.build.LibExeObjStep) void {
    obj.linkLibC();
    obj.linkSystemLibrary("snappy");
}

pub fn build(b: *Builder) !void {
    var target = b.standardTargetOptions(.{
        .default_target = .{
            .abi = .musl,
        },
    });

    const mode = b.standardReleaseOptions();

    // Define options

    const with_snappy = b.option(bool, "with_snappy", "Enable Snappy compression") orelse false;
    const with_cassandra = b.option(bool, "with_cassandra", "Run tests which need a Cassandra node running to work.") orelse false;
    const compression_algorithm = b.option([]const u8, "compression_algorithm", "Compress the CQL frames using this algorithm in the tests.");
    const protocol_version = b.option(u8, "protocol_version", "Talk to cassandra using this protocol version in the tests.") orelse 4;

    // LZ4
    //
    // To make cross compiling easier we embed the lz4 source code which is small enough and is easily compiled
    // with Zig's C compiling abilities.

    const lz4 = b.addStaticLibrary("lz4", null);
    lz4.linkLibC();
    // lz4 is broken with -fsanitize=pointer-overflow which is added automatically by Zig with -fsanitize=undefined.
    // See here what this flag does: https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html
    lz4.addCSourceFile("src/lz4.c", &[_][]const u8{ "-std=c99", "-fno-sanitize=pointer-overflow" });
    lz4.setTarget(target);
    lz4.setBuildMode(mode);
    lz4.addIncludeDir("src");

    var lz4_tests = b.addTest("src/lz4.zig");
    lz4_tests.linkLibrary(lz4);
    lz4_tests.setTarget(target);
    lz4_tests.setBuildMode(mode);
    lz4_tests.addIncludeDir("src");

    const lz4_test_step = b.step("lz4-test", "Run the lz4 tests");
    lz4_test_step.dependOn(&lz4_tests.step);

    // Snappy
    if (with_snappy) {
        var snappy_tests = b.addTest("src/snappy.zig");
        linkSnappy(snappy_tests);
        snappy_tests.setTarget(target);
        snappy_tests.setBuildMode(mode);

        const snappy_test_step = b.step("snappy-test", "Run the snappy tests");
        snappy_test_step.dependOn(&snappy_tests.step);
    }

    // Build library
    //
    const lib = b.addStaticLibrary("zig-cassandra", "src/lib.zig");
    lib.linkLibrary(lz4);
    lib.setTarget(target);
    lib.setBuildMode(mode);
    lib.addIncludeDir("src");
    maybeLinkSnappy(lib, with_snappy);

    lib.install();

    // Add the main tests for the library.
    //

    var main_tests = b.addTest("src/lib.zig");
    main_tests.linkLibrary(lz4);
    main_tests.setTarget(target);
    main_tests.setBuildMode(mode);
    main_tests.addIncludeDir("src");
    maybeLinkSnappy(main_tests, with_snappy);
    main_tests.addBuildOption(bool, "with_cassandra", with_cassandra);
    main_tests.addBuildOption(?[]const u8, "compression_algorithm", compression_algorithm);
    main_tests.addBuildOption(u8, "protocol_version", protocol_version);

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);

    // Add the example
    //
    const example = b.addExecutable("example", "src/example.zig");
    example.linkLibrary(lz4);
    example.setTarget(target);
    example.setBuildMode(mode);
    example.install();
    example.addIncludeDir("src");
    maybeLinkSnappy(example, with_snappy);
}
