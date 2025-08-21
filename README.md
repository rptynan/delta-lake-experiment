A small sample project trying to implement some data storage systems. Initially
based on [this
post](https://notes.eatonphil.com/2024-09-29-build-a-serverless-acid-database-with-this-one-neat-trick.html)
by Phil Eaton (plus the code), I'm planning on adding to it in order to learn
how various approaches work, and to learn golang.

## Random TODOs

- [ ] If you call flushRows with less than DATAOBJECT_SIZE in the unflushed rows, you'll save lots of nulls to disk.
- [ ] Try something other than JSON serialisation (pluggable?). Real delta lake: "store data in-memory in Apache Arrow format, and write to disk as Parquet. "
