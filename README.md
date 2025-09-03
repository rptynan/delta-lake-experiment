A small sample project trying to implement some data storage systems. Initially based on [this
post](https://notes.eatonphil.com/2024-09-29-build-a-serverless-acid-database-with-this-one-neat-trick.html) (and the
included code) by Phil Eaton, I'm planning on adding to it in order to learn how various approaches work, and to learn
golang.

## Implementation Notes

- Deletion is implemented as copy-on-write.

## TODOs

General features ideas:

- [ ] Set up minio with some latency to mimic S3 obj storage, write an object storage layer for it.
- [x] Implement deletes
- [ ] Add compaction of dataobject files
- [ ] Try something other than JSON serialisation (pluggable?). Real delta lake: "store data in-memory in Apache Arrow
      format, and write to disk as Parquet. "
- [ ] Set up containers to run as server
- [ ] Benchmark, perf ideas:
  - [ ] Column stats (bloom filter) on each data object
  - [ ] (Deletion) Implement deletion vectors instead of copy-on-write

Known problems:

- If you call flushRows with less than DATAOBJECT_SIZE in the unflushed rows, you'll save lots of nulls to disk. This
  also includes the current implementation of deletion tombstones (which are just nils). Removing these would simplify a
  lot of scan rows and delete rows.
- Schema changes aren't great. E.g. Look at inRange for deletions, if the schema has been changed to add columns and
  then a delete is done on one of the new columns, any flushed rows won't have values for those columns and it explodes.
- Similarly types are a problem. Serialising anys to JSON means all our numbers come back as floats, so for now there is
  just a cast in there to make them all ints.
