Cassandra client in Zig
=======================

Experiment with writing a Cassandra client in Zig.

The goal is only to support CQL.

TODO
====

* Force the use of an arena where possible
* Use proper error sets for stuff
* Take a ScanOptions in Iterator and add a "diagonistics" field, see [here](https://github.com/ziglang/zig/issues/2647#issuecomment-589829306)
* Cleanup code that won't be used, some frames are never written by a client for example.
* Add an abstraction for a Stream
* Compression
