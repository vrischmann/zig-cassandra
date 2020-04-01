Cassandra client in Zig
=======================

Experiment with writing a Cassandra client in Zig.

The goal is only to support CQL.

TODO
====

* Try out the arena allocator

If we use an arena allocator we could get by not caring about deinitialization in the frames and framer.
