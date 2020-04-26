Cassandra client in Zig
=======================

Experiment with writing a Cassandra client in Zig.

The goal is only to support CQL.

TODO
====

* Use proper error sets for stuff
* Cleanup code that won't be used, some frames are never written by a client for example.
* Add an abstraction for a Stream
* Compression (snappy)
* Handle named values in a statement
* Store the prepared statement metadata for use with EXECUTE
* Safe prepared statement (prepare a query string with a dummy struct for type binding and keeps this information for future execute calls)
