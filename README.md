# Cassandra client in Zig

Experiment with writing a Cassandra client in Zig.

The goal is only to support CQL.

# Building

## Linux

You need to have lz4 and its headers installed:

* Fedora: install `lz4-devel`

NOTE: cross compiling from `x86_64` to `i386` doesn't work yet.

## Windows

We're using [vcpkg](https://github.com/microsoft/vcpkg). Once you set it up correctly, install `lz4` like this:

```
$ vcpkg install --triplet x64-windows-static lz4
```

If you're building for another architecture reinstall the correct triplet.

TODO
====

* Use proper error sets for stuff
* Cleanup code that won't be used, some frames are never written by a client for example.
* Add an abstraction for a Stream
* Compression (snappy)
* Handle named values in a statement
* Batching
* Setting a keyspace
* Paging
* Cluster client / client pool capable of maintaining a connection to each node in the cluster + autodiscovery with events
* Implement token-aware routing
* Expose the low level parsing stuff so users can simply give us bytes and get back frames ?

# License

The files `src/lz4.c` and `src/lz4.h` are from [github.com/lz4/lz4](https://github.com/lz4/lz4/tree/dev) use the license in `LICENSE.lz4`.

All other files use the license in `LICENSE`.
