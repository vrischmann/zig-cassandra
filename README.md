# Cassandra client in Zig

Experiment with writing a Cassandra client in Zig.

The goal is only to support CQL.

# Available features

* Executing a single query without preparation
* Preparing a query and executing it later, with type-checked arguments.
* Comprehensive errors and diagnostics in case of failure.

TODO
====

* Use proper error sets for stuff
* Cleanup code that won't be used, some frames are never written by a client for example.
* Add an abstraction for a Stream
* Compression (snappy)
* Handle named values in a statement
* Batching
* Cluster client / client pool capable of maintaining a connection to each node in the cluster + autodiscovery with events
* Implement token-aware routing
* Need to add custom types for thins we can't infer with Zig's meta programming (things like Counter, Time, Timestamp, Timeuuid).
* Expose as a C library ?

# License

The files `src/lz4.c` and `src/lz4.h` are from [github.com/lz4/lz4](https://github.com/lz4/lz4/tree/dev) and use the license in `LICENSE.lz4`.

All other files use the license in `LICENSE`.
