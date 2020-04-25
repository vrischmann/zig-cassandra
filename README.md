Cassandra client in Zig
=======================

Experiment with writing a Cassandra client in Zig.

The goal is only to support CQL.

TODO
====

* prepared statement
* Use proper error sets for stuff
* Cleanup code that won't be used, some frames are never written by a client for example.
* Add an abstraction for a Stream
* Compression
* Handle named values in a statement
