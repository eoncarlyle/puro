# README

> **Puro** (Finnish): stream, streamlet, brook, creek (body of running water smaller than a river)

## Motivation
This is probably wildly ambitious but this is an attempt to make an 'SQLite of event streams'. This is an alternative to 
running Apache Kafka when producers and consumers are not separated by a network.  Rather than any daemonised processed, 
there will just be a log format and client libraries for producers and consumers.

## Development Log

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
