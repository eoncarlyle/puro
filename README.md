# README

> **Puro** (Finnish): stream, streamlet, brook, creek (body of running water smaller than a river)

## Motivation
This is probably wildly ambitious but this is an attempt to make an 'SQLite of event streams'. This is an alternative to 
running Apache Kafka when producers and consumers are not separated by a network.  Rather than any daemonised processed, 
there will just be a log format and client libraries for producers and consumers.

## Action Items
- [ ] Make, test working consumer
- [ ] Batching producer writes, compare benchmarks
- [x] Benchmarks on same level as `src`
- [ ] Active segment transition race condition handling
- [ ] Fix spurious segment
- [ ] Multithreaded producer tests

## Development Log

### 2025-10-03
While `io.methvin.watcher.DirectoryWatcher` works, it does not provide the new file offset with incoming events. There
is apparently no issue with keeping a file channel open while waiting for new offsets to come in. A default logger is
applied which can be worked around without issue. Note that `DirectoryWatcher#watch` is a blocking operation (which
definitely makes sense); clients should define if the consumer should return after listening, but that might be 
something for the future.

I just realised there is something of a 'spurious segment' problem: if the listener gets a file creation event for a 
.puro file but it can demonstrate that there is not a segment tombstone on the current segment, that is something
that should be logged. The thing this defends against is accidental creation of a segment file that was not created by
a Puro 'steward'. When a producer is starting up it needs to be congnisant of spurious segments. The producers are not
in a good spot to do anything about this but consumers will and stewards may have directory listening capabilities. If a
consumer sees a new segment and then checks that it a) is at the end of it's currently active segment and b) a segment
tombstone has not been placed then it could place a spurious segment tombstone onto the spurious topic. Producers can/
should be setup such that if there is a segment without a segment tombstone of lower order than the highest order
segment, that it will wait until a predetermined timeout. This way if it is first to read the spurious segment it can
wait until a client or steward can act on it first. This of course leaves open the possibility that, if there are no
active consumers, the producer cannot start. But in this case the

I do not think that active consumers will really be impacted by this. They only respond to a segment tombstone, and the 
producers are not rotating segments on their own volition. The steward that places the segment tombstone could place a
spurious segment tombstone on the spurious segment before forcing a producer rollover - order is important here, but
it is also pretty easy to establish. Ultimately this is congruent with many _Little Book of Sempahores_ problems, but
files are of course more complicated than sempahores just because of partial and nonexclusive file locking.

### 2025-10-02
```text
Benchmark                                 Mode  Cnt  Score   Error  Units
PuroProducerBenchmark.sendBatched         avgt   15  2.108 ± 0.345  ms/op
PuroProducerBenchmark.sendUnbatched       avgt   15  3.329 ± 0.932  ms/op
PuroProducerBenchmark.serialiseBatched    avgt   15  0.939 ± 0.038  ms/op
PuroProducerBenchmark.serialiseUnbatched  avgt   15  0.915 ± 0.026  ms/op
```

### 2025-10-01
The following is dog-slow on MacOS because NIO polls rather than working with native filesystem notifications on Linux
or Win32.
```kotlin
var fs = FileSystems.getDefault()
var watchService = fs.newWatchService()
var watchPath = fs.getPath("/tmp")
watchPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY)

while (true) {
    val key = watchService.take()
    val watchEvents = key.pollEvents()
    watchEvents.forEach { event ->
        val pathEvent = event as WatchEvent<Path>
        val path = pathEvent.context()
        println("${pathEvent.kind()} on path $path")
    }
    key.reset()
}
// https://learning.oreilly.com/library/view/learning-java-4th/9781449372477/ch12s03.html#id2038584
```
[Directory Watcher](https://github.com/gmethvin/directory-watcher/tree/main) seems to be the way forward.

At this point I am pretty sure the way to work around single-file sizes is multiple segments in a directory. This is a 
true 'broker' level configuration, so it shouldn't be configurable by the clients. I also don't really want this to be
dependent on a configuration file if at all possible. It would be nicer to place all settings onto a special stream, but
I don't see much of a reason to worry about this yet. Both for segment deletion and for log compaction the best path 
forward may be to have a third type of actor in the system that is neither consumer nor producer, and acquires locks as
they are available to delete old segments and compact unused logs. This 'steward' would need to be configured, but if
someone decides to have two different stewards with different compaction and deletion standards that's kind of their
problem.

I am not using much `ByteBuffer` functionality. Right now I'm sort of using them as a less convenient array. If a 
smaller `recordBuffer` is created after a larger one, the initial buffer can be reused with a smaller limit.

Benchmarking should be done with `kotlinx-benchmark`, blackholes will be need to make sure operations don't get deopted.

We should only make one syscall per batch.

Segment rollover tombstones should use a sentinel topic of `0xFF`. This will mean that consumers and producers will need
to be able to work with non-string byte arrays as topics and handle them accordingly (i.e. explicitly test this) code 
path. There should not be a public method that accepts a record with a non-string topic. There should be a method that
carries out `rollover()`, but a) that should be on the steward and b) it will internally call the tombstone.

Thought that I just had on rollover detection: because all writes append, a writer knows when it aquires a lock that the
previous record will not change out from under it. Also, with only one writer at a time and shared read locks, it shouldn't 
be an issue to acquire a read lock within the write lock to check for rollover before write.

### 2025-09-29
The incremental CRC calculations were introducing the zero calculation twice.
There are a couple of places where I am returning an empty result or null when something invalid happens.
Result types should be used instead, but probably not the full Arrow result type just for dependency management reasons.
It wouldn't even be that bad of an idea to provide an extension function in another Gradle module that could do translation,
but at this point I should read some ways that F#/Rust/Haskell libraries design result types for operations like these

### 2025-09-28
`ConsumerTest#Happy Path getRecord` has a different crc8 for the encoded total length on serialisation and deserialisation.
This is not because they differ on VLQ - there is some subtle difference between immediate and incremental calculation of CRC8s.

## Implementation

### Storage
- File locking used to prevent writes where readers are operating, and to make sure only one writer is active at a time
- If I am at all interested in segment rollover, it makes more sense to operate on the level of directories
- Alerting reader clients of segment turnover will be best accomplished by a special control message
- Going multi-file is kinda breaking the 'one file' idea in 'Kafka in One File', but, hey, even SQLite has WAL files
  - Recording most recent offsets of messages across topics in another file may be nice

### Stream Format
Header: fixed size to indicate if active segment?
- Maintaining consistency could get complicated: it shouldn't be possible to have multiple segment files
- I don't really know what this buys us, because readers are reading up to the high-water mark
- Any control message would necessarily be an append operation

Message Format
```text
crc: uint8
totalLength: varlong
topicLength: varlong
topic: byte[]
keyLength: varlong
key: byte[]
value: byte[]
```

I needed a word for it so the 

The CRC covers the entire rest of the message. Message length computed from total, topic, and key lengths.
I am not super confident in the 'what happens if the write is incomplete' which makes me think that an index in the directory makes sense

Message Limits
- Topic, key no more than 1 kilobytes
- Value no more than 10 megabyte

## Experimentation

### Producer
Everything that I have heard about performance-aware Java is that aggressive benchmark evaluation is really important.
As 'forks in the road' come up performance wise, it would be good to evaluate the following
- Byte buffers or byte arrays (length will matter)
- VLQ vs. long encoding
- Aggressive nulling out of allocated objects vs. leaving the garbage collector to do so
- Running Tail recursive IO monad vs. not
- Arena allocation vs. not
- Object pools vs. not

These are already a dizzying number of configurations, but with four options cobbling together the functions for each shouldn't be a huge pain.
Keep in mind that GC impacting options may matter more for message latency than message throughput.
