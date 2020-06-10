const std = @import("std");
const builtin = @import("builtin");
const Builder = std.build.Builder;

pub fn build(b: *Builder) !void {
    const target = b.standardTargetOptions(.{});
    const mode = b.standardReleaseOptions();

    // Build lz4
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

    // Build library
    const lib = b.addStaticLibrary("zig-cassandra", "src/lib.zig");
    lib.linkLibrary(lz4);
    lib.setTarget(target);
    lib.setBuildMode(mode);
    lib.addIncludeDir("src");
    lib.install();

    var main_tests = b.addTest("src/lib.zig");
    main_tests.linkLibrary(lz4);
    main_tests.setTarget(target);
    main_tests.setBuildMode(mode);
    main_tests.addIncludeDir("src");

    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&main_tests.step);

    // Build example
    const example = b.addExecutable("example", "src/example.zig");
    example.linkLibrary(lz4);
    example.setTarget(target);
    example.setBuildMode(mode);
    example.install();
    example.addIncludeDir("src");
}
