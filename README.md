# README

> **Puro** (Finnish): stream, streamlet, brook, creek (body of running water smaller than a river)

## Motivation
This is probably wildly ambitious but this is an attempt to make an 'SQLite of event streams'. This is an alternative to 
running Apache Kafka when producers and consumers are not separated by a network.  Rather than any daemonised processed, 
there will just be a log format and client libraries for producers and consumers.

## Action Items
- [ ] Make, test working consumers
  - [ ] Fetch process buffer check: I think not all opertaions will work
  - [x] Event loop
  - [x] Consumer result types
  - [ ] `onHardProducerTransition`
- [ ] Producer result types
- [ ] Active segment transition race condition handling
- [ ] Protection on VLQs
- [ ] Fetch interior failures
- [x] Batching producer writes, compare benchmarks
- [x] Benchmarks on same level as `src`
- [ ] Write size ceiling specification (small) 
- [ ] Benchmarks: investigation on read buffer size
- [ ] Reader indexes
- [ ] Fix spurious segment
- [ ] Multithreaded producer tests

## Development Log

### 2025-10-12
The event loop is now constructed. The `onHardProducerTransition` really won't take that long to write but as soon as it
is written it will be time to do some manual testing on this. Also, the difference between 'read from the beginning' and
'read new events as they come in' will require setting the offset to zero and placing the current file size onto the 
queue vs. setting the offset to the current segment file size.

### 2025-10-10
If the producers are producing faster than the consumers can consume I think the logic is completely broken with the 
callback, so I will need to build some simple event loop and keep track of producer offsets. Also, if the a message was 
truncated, but we do have the length of the message that gives us a great hint about hard producer resets, and so may be 
worth keeping. Also, as of the current commit the mechanism for hard producer transition will only work if there is a live consumer. This wouldn't be true if the
messages had inverted length as the last field, but that can probably wait. There are also no current affordances for controll
messages.

### 2025-10-08-1
You _have_ to fix the fact that the consumer and producer have entirely different ways to count bit length. The consumer
method is probably better, but this is very confusing. 

Okay - a 'fetch interior' record batch is an 8k or smaller read buffer inside of a fetch. The final round of deserialisation
is the trickiest and will not only involve the file locking trick below but also something of a lookahead: without some
ceiling on batches where we round down to the nearest message boundary (and, well maybe even _with_ that) we don't want
perfectly intact messages on large writes to be discarded just because they fall across a fetch boundary. This means that
the consumer needs to handle messages across read buffers similarly to messages cut across fetches. If the length and
CRCs match, then the consumer is free to keep on going. But if it _fails_ the CRC check, then the consumers will need to
race to zero out the last truncated message of the previous fetch.

If there is a legal message in the first segment of the next fetch, consumers keep doing what they are doing. The 
nightmare scenario would be fetch _N_ ending with a truncated message, fetch _N+1_ starting with a truncated message 
followed by legal messages that don't look legal because the truncated message has thrown every length off. _That_ is
as situation where the messages ending with reverse length would save us, because then consumers could read in reverse
order until they find the last illegal message and zero it out.

Given that this is theoretically solvable, I am more comfortable saying that an illegal fetch start after an illegal 
fetch end is irrecoverable.

### 2025-10-08-0
While I've done more thinking about incomplete message updates from producers, sitting down to write the consumer 
implementation has made me think about how if a producer writes 100+ Mb of messages in one go we will _not_ want to 
read the whole thing into memory. I haven't yet put a ceiling on write sizes but will need to do so eventually. But 
anyways apparently people like using 8k read buffers (there is some empirical work backing this). As far as 
disambiguating failing writes from incomplete writes, Jesse disappointingly said that it was out of is wheelhouse so
my search continues on this. But we'll be forced to handle this for the batched reads on the consumer.

One thing of note is that if the reader knew when the writer lock was relinquished that would allow the reader to 
distinguish this. Note that if a consumer can access the write lock around the offending segment it is necessarily the
case that the writer is finished because it has relinquished the write lock at that part of the file. But that leaves a 
question: how can a reader tell the difference between a writer that isn't finished and a reader that is 'investigating'
and possibly repairing the segment? Maybe the answer is that they don't _need_ to. This will require saving the offset
and changing over file channels, but as soon as a CRC is off the reader knows an end-file offset and there can be a race
between consumers to acquire the exclusive lock between the last safe offset and the final offset. This can then let the
producers continue on their merry way, with the first consumer converting the message to a control message. Only one 
consumer should ever have to clean the message, but that isn't an invariant we need to depend on.

### 2025-10-07
The question "how large can a write be to produce one `FSEvent` or `inotify` event" isn't super easy to predict. It is
very hard to a consumer to _a priori_ differentiate between a write that is incomplete due to an error and one that is 
incomplete because it is large. Do note that for large writes the time required for event consumption gets larger, so
maybe there is a grace period for sufficiently large writes? My model for this is that the smallest file that could 
force multiple events would be split in roughly two, so the first thing the consumer would see would be a substantial
offset change followed by another substantial offset change. Thresholds on this may be difficult to setup. 

Relevant manpages
- MacOS
  - `man 2 kevent`
  - `man 2 open`
  - `man 2 write`
  - `man 2 fcntl`
  - `man 9 vnode`
  - `man 7 fsevents`
- Linux
  - `man 7 inotify`
  - `man 7 fsnotify`
  - `man 7 fanotify`
  - `man 2 write`
  - `man 2 pwrite`

### 2025-10-06-1

If we assume that any partial message is due to a producer that irrecoverably terminates, then the consumers can mark 
the bits from the last safe offset to the end of the file as garbage. This does no good when there are no consumers, but
when there are no consumers there is no contention for reading the file. It does kinda sound like this would require
consumers and producers scanning through the entire file, unless the inverted total length was left at the end of the
message. I'd have to be careful about buffer allocation but that isn't the end of the world. The crc would still be for
all bits after it in the message, but the value deserialisation would have to stop short of the entire message because
of the `invertedTotalLength` appended on the end.

Message Format
```text
crc: uint8
totalLength: varint
topicLength: varint
topic: byte[]
keyLength: varint
key: byte[]
value: byte[]
invertedTotalLength: inverted varint
```

This all seems clever - but it only works for intact messages so that's kinda moot. I don't love the idea of adding 
different file types that aren't segments, but another possibility is for a very, very small reader index. What I mean
by this is that readers can attempt to acquire an exclusive lock to an `indexN.puro` file for the _N_ active segment at
the end of each batch. This could then be used by a Puro producer to work backwards from a checkpoint. It isn't 
neccessary for the most recent checkpoint to truly be the last good message, but it does need to be _a_ message. Cutting
down on syscalls by only writing the most recent message returned from a run would be possible, or every _M_ messages.
It doesn't really matter as long as the process of doing a full file scan is prevented.

Without a consumer operating at some point on the stream to populate the index, the first producer can't assume that 
any valid messages are on the stream, so they have to scan through the whole stream. This is a reason to keep segments
on the shorter side, but without consumers there also isn't a good reason to even operate the stream in the first place.

Two other notes about consumers
- Consumers are in the best possible position to validate segment integrity
- Consumers will need to relinquish read locks that block bad segments to allow producers to fix the problem

### 2025-10-06-0
What happens if a spurious segment is written to, even accidentally or by a non-Puro process? The existence of a segment 
without a tombstone should be enough, but to allow future consumers to not get too distracted does it make sense to 
place a spurious stream control event on the spurious stream?

Also, what should be done to handle incomplete messages? Should the producers and consumers store a 'last safe' file
offset and have the ability to null out the message? This would be a new control message (I hope we are not running out 
of topics) that would effectively say 'the message is X bytes long, ignore it'. The cost of this that it makes the 
locking a little more complicated: the producer may have to 'rewind' a little bit, and the reader's can't lock the 
entire file otherwise producers couldn't 'rewind': we don't a priori know the size of messages.

Also, there should be some message size ceiling where batches are broken up on the consumer side to prevent someone
from ramming a massive message

As far as an example of results types go, Rust's [ErrorKind](https://doc.rust-lang.org/std/io/enum.ErrorKind.html) seems
like a workable example.

### 2025-10-05
The current `main` function demonstrates how to use `fileChannels` in a persistent way. No issues - during segment
rollover the channel will change, but that is a given. It also doesn't look like the default logger is really doing
anything in this example. Probably still a good idea to muzzle this in some way, but :shrug:.

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
totalLength: varint
topicLength: varint
topic: byte[]
keyLength: varint
key: byte[]
value: byte[]
```

I needed a word for it so the 

The CRC covers the entire rest of the message. Message length computed from total, topic, and key lengths.
I am not super confident in the 'what happens if the write is incomplete' which makes me think that an index in the directory makes sense

Message Limits
- Topic, key no more than 1 kilobyte
- Value no more than 10 megabytes

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
