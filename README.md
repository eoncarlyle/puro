# README

> **Puro** (Finnish): stream, streamlet, brook, creek (body of running water smaller than a river)

## Motivation

This is probably wildly ambitious but this is an attempt to make an 'SQLite of event streams'. This is an alternative to
running Apache Kafka when producers and consumers are not separated by a network. Rather than any daemonised processed,
there will just be a log format and client libraries for producers and consumers.

Also - the `ByteBuffer.getArraySlice` does nothing to guard against when `length` is smaller than the remaining number 
of bytes. Same applies for `ByteBuffer.getBufferSlice`, can reproduce this pretty easily.

`getRecords` was incorrectly using the offsets as if it was operating on the active segment file stream rather than the
read buffer, which introduced some issues. I _think_ I have those resolved now.

## Action Items

- [ ] Make, test working consumers
    - [x] Fetch process buffer check: I think not all opertaions will work
    - [x] Event loop
    - [x] Consumer result types
    - [x] Final message cleanup
    - [x] `onHardProducerTransition`
    - [x] `onConsumedSegmentTransition`
- [ ] `ByteBuffer.getArraySlice` and `ByteBuffer.getBufferSlice` fixes
- [ ] Handling consumer starts with non-current consumer start
- [ ] Active segment transition race condition handling
- [ ] (Small) replace the hardcoded 1s with reference to current CRC size (in case it changes)
- [ ] Check if the nonzero `consumerOffset` initialisation will actually work (ragged start problem)
- [ ] Consumer and producer builders that prevent single-byte topics
- [ ] Consumer builder that allows
  - For consumer to wait for a stream topic that hasn't been created yet (wait-on-start)
  - Different consumer patterns - from latest, beginning, specific offset, etc.
- [ ] Retry delay
- [x] Control message handling and topic optimisation for consumers
- [ ] Active segment transition race condition handling
- [ ] Producer result types
- [ ] Type Protection on VLQs
- [ ] Fetch interior failures
- [x] Batching producer writes, compare benchmarks
- [x] Benchmarks on same level as `src`
- [ ] Write size ceiling specification (small)
- [ ] Benchmarks: investigation on read buffer size
- [ ] Reader indexes
- [ ] Fix spurious segment
- [ ] Multithreaded producer tests

## Development Log

### 2025-11-01
A quick-and-dirty way to do wait-on-start is more or less what we have going on now, sorta. Also, it takes hundreds of 
milliseconds for the consumer `DirectoryWatcher` to be ready to observe so it is easy for the producer to rocket 
through a bunch of writes and then as far as the consumer is concerned no one has written to the stream yet. This really
is not how the consumers should be behaving.



### 2025-10-26
`1 + ceil(log2(y)) + y = x` doesn't appear to have a closed-form solution, so the decrementing option really isn't that
bad. (But that doesn't _exactly_ appear to be the relevant function? Much to consider


### 2025-10-25
How much sense, actually, does it make for the consumer to _create the first segment if it doesn't exist_ I mean in 
isolation that already sounds pretty bad but, more relevant for work on the consumer, it sets up inconsistent behaviour
for the consumer. It seemed like it would be annoying to do but apparently things aren't so bad but I could have null
active segment behaviour to denote an active segment that hasn't been confirmed yet.

```text
                    ┌───────────────┐
              ┌────>│ B) Consuming  │──┐
              │     └───────────────┘  │
              │                        │
              │                        │
              │                        v
     ┌────────────────┐         ┌─────────────┐
     │ A) Waiting for │<────────│ C) Locked   │
     │   Consumed     │         │   Segment   │
     │    Segment     │         └─────────────┘
     └────────────────┘

```

The consumer waiting rather than just creating the segment on start is marked as 'consumer wait on start'.

```kotlin
val subrecordLength =
  encodedTopicLength.capacity() + topicLength + encodedKeyLength.capacity() + keyLength + valueLength
val encodedTotalLength = subrecordLength.toVlqEncoding()
val recordBuffer = ByteBuffer.allocate(1 + encodedTotalLength.capacity() + subrecordLength)

private data class SerialisedPuroRecord(
  val messageCrc: Byte,
  val encodedTotalLength: ByteBuffer,
  val encodedTopicLength: ByteBuffer,
  val encodedTopic: ByteArray,
  val encodedKeyLength: ByteBuffer,
  val key: ByteBuffer,
  val value: ByteBuffer
)
```

A note on `onHardTransitionCleanup` - getting the math on this will be annoying. The `ControlTopic.INVALID_MESSAGE` will
necessarily be `0x01`. The key length, key, and value aren't important and can be kept as the existing byte values. We
know what the length of `recordBuffer` will be, but rephrasing in math terms:

```text
subrecordLength = 1 + 1 + messageTailLength 
messageSize = 1 + capacity(vlq(subrecordLength)) + subrecordLength
```

Where `messageTailLength` is `encodedKeyLength.capacity() + keyLength + valueLength`. The smallest capacity possible is 
1, because even expressing a length of zero requires one byte. This is a profoundly cringe way to do this but I could
just decrement until I hit the answer but every fibre in my body tells me that this is a solveable nonlinear equation. I
think that a function in `Vlq` gives the answer:

```kotlin
fun ceilingDivision(dividend: Int, divisor: Int): Int = (dividend + divisor - 1) / divisor

fun Int.toVlqEncoding(): ByteBuffer {
  //...
  val bitsReq = Int.SIZE_BITS - this.countLeadingZeroBits()
  val bytesReq = ceilingDivision(bitsReq, 7)
  //...
}
```
However - `maxVlqInt` can be expressed in 4 bytes (which, of course, I learned and forgot in CSCI 1933). It's not that 
an inverse function couldn't be created, but if a logarithm needs to be accessed for the equivalent of the 
`countLeadingZeroBits` we're in a bad spot. There are four possible answers based of the integer range so there's no
reason not to just to hardcode those in. That's definitely the way to go.


### 2025-10-22
`fetch` is nearly a pure function at this point. The reason I did this is that I want as few functions as possible 
carrying out field modifications. The `ConsumerResult` can handle if there are records after the tombstone, so that
really doesn't need to be baked into a property. The same cannot be said about the abnormal offset window. That is 
something that needs to be handled by the next fetch.

What should definitely be tested is successive hard transitions that include a batch or two of items that are fine 
before the truncated message. Following these with some continuations would make sense. After all of this work on side
conditions, there isn't much of an excuse to not eke out all the edge cases with how testable this is.

### 2025-10-20
The consumer's current segment is not always the active segment, so I am now calling the currently consumed segment just
the 'consumed segment'.

Now that we have the segment tombstones, I need to consider how to best handle setting `currentConsumerLocked`. These, I
think, should be handled in a similar way to the `FetchSuccess.HardTransition`. Even if there is nothing to 'clean' up,
there still need to be actions taken. We also need to handle the case where further messages are placed after a 
tombstone and re-tombstoning it (probably checking first to make sure another consumer hasn't done the same, using the 
same 'fixed control message size' trick. Any messages between tombstones are illegal but I don't feel the need to
zero them out unlike hard transitions where it really matters.

I'm going to sleep on it, but I think it will look like:

```kotlin
sealed class FetchSuccess() {
    class CleanFetch() : FetchSuccess()
    class CleanFinish() : FetchSuccess()
    class SimpleRecordsAfterTombstone() : FetchSuccess()
    class HardTransition(val abnormalOffsetWindowStart: Long, val abnormalOffsetWindowStop: Long, val recordsAfterTombstone: Bool) : FetchSuccess()
}
```

### 2025-10-21
I am making an implicit assumption that there can only be either a truncation abnormality or a records after segment
abnormality. I think this is a decent assumption - you could make an argument that a segment tombstone inside the
segment would get clobbered, but that is true regardless and we're assuming clients will not add or delete bytes inside
of an otherwise succesful write, although a hard failure that truncates is of course something we're dealing with.

Left some TODOS for re-tombstoning

### 2025-10-19

> There is a bit of an issue here because between calling `getActiveSegment()` and acquiring the lock,
> the active segment could change, this has been marked on the README as 'Active segment transition race
> condition handling'; Best way may be to read if 'tombstone'

This is written in `Consumer#withConsmerLock`. It is wrong: a consumer that is working on segment _N_ and observes
that segment _N_ is the active segment should process all messages before moving over to _N+1_. The happy
path is something like

1) The consumer is on _N_
2) The following takes place with non-deterministic ordering
    - The consumer receives word of a modify/create `DirectoryChangeEvent` for _N+1_
    - The consumer reads a segment tombstone on _N_
3) The consumer observes and assigns the new active segment as _N+1_

Until the consumer reads a tombstone message on _N_, the consumer should not re-assign the observed active segment. This
is the means by which the consumers can protect against spurious segment creation.

If the consumer reads the tombstone before a new segment is created, the active segment is undetermined. The consumer
knows what the next observed active segment _will_ be - _N+1_. In the case where the producer rolls the segment over
and establishes _N+2_ as the global active segment before the consumer observes the creation of _N+1_, the
consumer still needs to read through all of _N+1_ first. What pretty obviously follows from this is the need for some
priority queue. The consumer needs to take _N_ events
before _N+1_ events. 

So that more or less builds the consumer state machine. The _producer_ at this point needs one other step. The producer
should determine if the last message was a segment tombstone before writing. I don't _think_ there is an issue acquiring
a read lock after acquiring a write lock, and the write lock is kinda acting as a semaphore.

### 2025-10-16

'Total length' and 'subrecord length' are actually equivalent but 'subrecord length' (the length of everything other
than crc, the 'total length' field itself, and reverse-encoded length) is more accurate to describe what

### 2025-10-15

While my intuition was correct on the producer offset, the consumer offset was wrong for a symetric reason: my
intention was to begin at the current consumer offset, but nothing was actually enforcing that in the code.

### 2025-10-13

I am 75% sure that the bug is that the consumer does not stop at the incoming consumer offset (there is no current
limit set to actually accomplish this), so the consumer will catch up with the state of the file rather than where it
was 'supposed' to be for the incoming offset. The only thing that gives me pause is that `b` in the following takes the
value of 800 which doesn't make sense for the failing final offset of 1120.

```kotlin
val a = Path("/tmp/puro/stream0.puro")
val b = Files.size(a)
```

### 2025-10-12

The event loop is now constructed. The `onHardProducerTransition` really won't take that long to write but as soon as it
is written it will be time to do some manual testing on this. Also, the difference between 'read from the beginning' and
'read new events as they come in' will require setting the offset to zero and placing the current file size onto the
queue vs. setting the offset to the current segment file size.

### 2025-10-10

If the producers are producing faster than the consumers can consume I think the logic is completely broken with the
callback, so I will need to build some simple event loop and keep track of producer offsets. Also, if the a message was
truncated, but we do have the length of the message that gives us a great hint about hard producer resets, and so may be
worth keeping. Also, as of the current commit the mechanism for hard producer transition will only work if there is a
live consumer. This wouldn't be true if the
messages had inverted length as the last field, but that can probably wait. There are also no current affordances for
controll
messages.

### 2025-10-08-1

You _have_ to fix the fact that the consumer and producer have entirely different ways to count bit length. The consumer
method is probably better, but this is very confusing.

Okay - a 'fetch interior' record batch is an 8k or smaller read buffer inside of a fetch. The final round of
deserialisation
is the trickiest and will not only involve the file locking trick below but also something of a lookahead: without some
ceiling on batches where we round down to the nearest message boundary (and, well maybe even _with_ that) we don't want
perfectly intact messages on large writes to be discarded just because they fall across a fetch boundary. This means
that
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
invertedTotalLength: inverted varint (not yet implemented)
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
previous record will not change out from under it. Also, with only one writer at a time and shared read locks, it
shouldn't
be an issue to acquire a read lock within the write lock to check for rollover before write.

### 2025-09-29

The incremental CRC calculations were introducing the zero calculation twice.
There are a couple of places where I am returning an empty result or null when something invalid happens.
Result types should be used instead, but probably not the full Arrow result type just for dependency management reasons.
It wouldn't even be that bad of an idea to provide an extension function in another Gradle module that could do
translation,
but at this point I should read some ways that F#/Rust/Haskell libraries design result types for operations like these

### 2025-09-28

`ConsumerTest#Happy Path getRecord` has a different crc8 for the encoded total length on serialisation and
deserialisation.
This is not because they differ on VLQ - there is some subtle difference between immediate and incremental calculation
of CRC8s.

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
I am not super confident in the 'what happens if the write is incomplete' which makes me think that an index in the
directory makes sense

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

These are already a dizzying number of configurations, but with four options cobbling together the functions for each
shouldn't be a huge pain.
Keep in mind that GC impacting options may matter more for message latency than message throughput.
