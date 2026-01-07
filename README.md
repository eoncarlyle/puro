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

## Initial Action Items

- [ ] Stale and spurious segment problems
    - If a segment is tombstoned, exclude it from relevant `Segments.kt` places
    - Same as above except for spurious segments (when a segment of lower order than the highest found
- [ ] `ByteBuffer.getArraySlice` and `ByteBuffer.getBufferSlice` fixes one is untombstoned)
- [ ] Active segment transition race condition handling
- [ ] (Small) replace the hardcoded 1s with reference to current CRC size (in case it changes)
- [ ] Check if the nonzero `consumerOffset` initialisation will actually work (ragged start problem)
- [ ] Consumer and producer builders that prevent single-byte topics
- [ ] Consumer builder that allows
    - For consumer to wait for a stream topic that hasn't been created yet (wait-on-start)
    - Different consumer patterns - from latest, beginning, specific offset, etc.
- [ ] Retry delay
- [ ] Active segment transition race condition handling
- [ ] Producer result types
- [ ] Type Protection on VLQs
- [ ] Fetch interior failures
- [ ] Write size ceiling specification (small)
- [ ] Benchmarks: investigation on read buffer size
- [ ] Reader indexes
- [ ] Fix spurious segment
- [ ] Multithreaded producer tests

## Signal Bit Action Items

- [ ] New producer class using signal bits, no cleanup
- [ ] Consumer class working with meaningless signal bits
- [ ] Investigate how annoying iterating down the full length of a nontrivially sized segment will be
- [ ] Producer class bad signal cleanup
- [ ] Producer segment retermination
- [ ] Consumer handling segment cleanup deletion
- [ ] Handling messages larger than the read buffer

## Development Log

At first I was uncomfortable with the idea of reading a mega-event into a buffer. After all, anything that can
incrementally stream large files generally incrementally streams large files. But I am aware of two things about Kafka

1) The maximum record size of Kafka is ~10MB
2) Kafka brokers use well more than 10MB of RAM

Now this makes me very curious as to how something like `jsonrepair` can stream without just loading everything into
memory - there has to be some interesting work there. But as far as how Puro is concerned, I think that records larger
than the buffer size should be treated as an abnormality. Thinking things through I think the only difference between
this and a regular continuation is that this can be 'chained' multiple times and given that singular `getRecord` is
carrying out the reads that is clearly not how things are going here.

I think this can be overcome without completely changing the consumers.

### 2025-01-06

With the 'signal bit' and moving to active producers I now have to shift between thinking a lot about how consumer
concurrency works to thinking a lot about how producer concurrency works. The very first thing to think about is asking
what would happen if a producer iterates down on an empty segment and hits a 0 signal bit. Sure, hypothetically
there could be healthy messages appended onto the end of the segment. But the producer has no clue of this and
the segment is unsound.

The only thing the producer can do at this point is to delete everything after the start of the 0 message, no
matter how many bits are behind it. Maybe this should be configurable, where the producer should throw rather than
delete everything. But it can't go on as normal either way.

The consumers may be more interesting than I first thought, but the entire hard transition logic needs to go. Deletions
will be something they need to handle because of the last paragraph, and there _probably_ is some way to do this
entirely lockless. Because a 1 signal bit means the record is ready to read, the locks aren't actually needed to prevent
reads of records that haven't been finished. And while there is some utility in having a crude notification method, to
be properly defensive I'd need to check the first bit anyway to see if it is ready. So the `do/while` on a lock gets
converted to a `do/while` on the signal bit.

This is complicated by trying to batch reads. On the one hand, being able to batch a large read is more performant.
On the other hand, we might need to introduce wait logic inside of the fetch if a 0 signal bit is hit. I think the way
to square this circle is with a different `GetRecordAbnormality` where the fetch will be retried if a low signal bit
is hit anywhere. This may look awkward because explicit locks are a different way of thinking than with signal bits.

That's enough about consumers. As far as producers go I think an explicit constructor is the best way to enforce
startup. The producers have to check that the segment is sound otherwise they produce garbage results, and that
means iterating down the length of the segment. The producer equivalent of this is handled by explicit threads, and
because this is just a matter of using some callbacks it isn't really the same.

After the time away from the project I think the way to handle messages larger than the read buffer is to use a
`ConsumerResult` that indicates 'start building the single message buffer'. This is going to destinguish between 
'standard' and 'single-message' read patterns, so entering and exiting this mode will need to be done with care. But 
this would prevent any strange games with resizing buffers. This would require appending bits to a buffer as the partial
reads come in. The length of the original message would then be used to understand what the stopping point is. I 
would need to deal with any shorter messages that are emitted after the larger message. More trickily, we might need 
to deal with `|.....long message, short message, short message, long message.....|`, but this shouldn't be awful to 
model with the right return values. Probably should explicitly model this all as an explicit state machine? Will need to
think on this. But that is the final item on the signal bit list.

New checklist made for this side of things, good to get back into this.

### 2025-12-09

Jesse gave me the idea that an in-process message could use a signal bit to indicate if a write was successful or not -
in that the last thing a producer does is flip a bit. This solves the 'abandoned write' problem but may require
batching, not that it would be _terribly_ onerous. This does make happy path consumption a little more complicated but
it completely solves the 'producer dies silently' problem so it seems worthwhile.

Do note that if no write lock is claimed and the final message hasn't been terminated then that is neccessarily
either a dead producer or an illegal write and should be zeroed. Jesse was much more wary about the automatic repairing
and didn't neccessarily use this flag trick in that context, but it works well with the invariants that I want.

### 2025-11-13

The handling of `truncationAbnormality` and not shifting consumer offsets is important for handling truncation
abnormalities. I made an implicit assumption of 'oh well the buffer will be exhausted' which is not true so that is just
a matter of a `break` but in the event that _not a single record_ could fit into the read buffer, the consumer needs to
handle this differently.

The simplest thing that could possibly work would be permanently increasing the buffer size until the current read
succeeds and then keeping the buffer size at that point. This has many obvious drawbacks and I won't do it, because a
slightly better way to go about this is a 'standard' buffer that was initialised and a 'high water mark' buffer that
increases in size to fit the largest message. This brings up an interesting problem: what should a strategy for buffer
sizing be based off of prior performance? If the high water buffer has been needed every single read since it's
inception, should the consumer stop trying to use the standard buffer? This is a class of problems called 'adaptive
buffer allocation', and an example from Netty is shown below. This is something of a speed limit on the size of messages
that the consumer can process, at least with the non-streaming example that I have implemented.

https://github.com/netty/netty/blob/4.2/transport/src/main/java/io/netty/channel/AdaptiveRecvByteBufAllocator.java
https://github.com/netty/netty/blob/4.2/common/src/main/java/io/netty/util/internal/AdaptiveCalculator.java#L28

### 2025-11-12

The record size is smaller than the read buffer size, which is resulting in the `NegativeArraySizeException`. Pardon the
swapping of formats, but the first snippet is the `standardRead` buffer and the second is the segment at the time of the
exception. This is now reproduced in `Get Record Truncation failure`.

I'm also seeing some strange issues on one of my workstations with illegal offsets being sent - almost certainly a
startup
issue

```
[ -16, -121, 4, 9, 116, 101, 115, 116, 84, 111, 112, 105, 99, 4, 107, 101, 121, 49, 118, 97, 108, 117, 101, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44, 32, 44 ]
```

```
00000000: f087 0409 7465 7374 546f 7069 6304 6b65  ....testTopic.ke
00000010: 7931 7661 6c75 652c 202c 202c 202c 202c  y1value, , , , ,
00000020: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000030: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000040: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000050: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000060: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000070: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000080: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000090: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
000000a0: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
000000b0: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
000000c0: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
000000d0: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
000000e0: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
000000f0: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000100: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000110: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000120: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000130: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000140: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000150: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000160: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000170: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000180: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000190: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
000001a0: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
000001b0: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
000001c0: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
000001d0: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
000001e0: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
000001f0: 202c 202c 202c 202c 202c 202c 202c 202c   , , , , , , , ,
00000200: 202c 202c 202c 202c 2021                  , , , , !
```

Okay - the reason that this is throwing is that you can hit `truncationAbnormality` in `getRecords` without the
buffer advancing. We don't want the offset to advance to allow for truncation recovery, but I'm worried about
automatically assuming that you have hit a truncation abnormailtiy. Technically
`lengthData != null && topicLengthData != null && topicMetadata != null && keyMetdata != null && keyData != null && valueData != null`
isn't correct because if element _N_ is null then all elements after _N_ must also be null. This all might be too
defensive?

### 2025-11-06

The most robust way to deal with wait-on-start regardless of `startPoint` is not with a second `ConsumerSegmentEvent`
but rather to check for the existence of a segment before the watcher is started, and one the file exists to manually
place an `segmentChangeQueue` event. Either `getLowestSegmentOrder` or `getHighestSegmentOrder` could be checked and I
don't think it matters?

As remarked in a comment, the answer to 'what happens if something is deleted at the worst time' in
`watcherPreInitialisation` isn't super great, and it isn't like the consumer is able to drop some 'don't delete any
files while I get my bearings'. There are limits to how defensive I can be but at the same time segments being deleted
as part of the operation of a future steward could be something to defend against? Will need to think on this one.

### 2025-11-03

New issue that I hadn't thought of until now - if a single message is split across more than two messages I don't think
that this is interpreted as a normal continuation - this could be pretty easily tested with a comically small buffer
size.

Something close to explicit synchronisation is required to solve the waiting consumer problem.

Throwing any exception is of course a white flag but the exception thrown in the
`val (producerOffset,  incomingSegmentOrder)` definition really doesn't have better alternatives really. This _should_
be an illegal state. The starter `ConsumerSegmentEvent` event is meant to get a start-from-beginning consumer caught up
if the producer has already finished creating events (at least for a moment) by the time the consumer gets ready to go.

### 2025-11-01

A quick-and-dirty way to do wait-on-start is more or less what we have going on now, sorta. Also, it takes hundreds of
milliseconds for the consumer `DirectoryWatcher` to be ready to observe so it is easy for the producer to rocket through
a bunch of writes and then as far as the consumer is concerned no one has written to the stream yet. This really is not
how the consumers should be behaving.

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
same 'fixed control message size' trick. Any messages between tombstones are illegal but I don't feel the need to zero
them out unlike hard transitions where it really matters.

I'm going to sleep on it, but I think it will look like:

```kotlin
sealed class FetchSuccess() {
    class CleanFetch() : FetchSuccess()
    class CleanFinish() : FetchSuccess()
    class SimpleRecordsAfterTombstone() : FetchSuccess()
    class HardTransition(
        val abnormalOffsetWindowStart: Long,
        val abnormalOffsetWindowStop: Long,
        val recordsAfterTombstone: Bool
    ) : FetchSuccess()
}
```

### 2025-10-21

I am making an implicit assumption that there can only be either a truncation abnormality or a records after segment
abnormality. I think this is a decent assumption - you could make an argument that a segment tombstone inside the
segment would get clobbered, but that is true regardless and we're assuming clients will not add or delete bytes inside
of an otherwise succesful write, although a hard failure that truncates is of course something we're dealing with.

Left some TODOS for re-tombstoning

### 2025-10-19

> There is a bit of an issue here because between calling `getActiveSegment()` and acquiring the lock, the active
> segment could change, this has been marked on the README as 'Active segment transition race condition handling'; Best
> way may be to read if 'tombstone'

This is written in `Consumer#withConsmerLock`. It is wrong: a consumer that is working on segment _N_ and observes that
segment _N_ is the active segment should process all messages before moving over to _N+1_. The happy path is something
like

1. The consumer is on _N_
2. The following takes place with non-deterministic ordering
    - The consumer receives word of a modify/create `DirectoryChangeEvent` for _N+1_
    - The consumer reads a segment tombstone on _N_
3. The consumer observes and assigns the new active segment as _N+1_

Until the consumer reads a tombstone message on _N_, the consumer should not re-assign the observed active segment. This
is the means by which the consumers can protect against spurious segment creation.

If the consumer reads the tombstone before a new segment is created, the active segment is undetermined. The consumer
knows what the next observed active segment _will_ be - _N+1_. In the case where the producer rolls the segment over and
establishes _N+2_ as the global active segment before the consumer observes the creation of _N+1_, the consumer still
needs to read through all of _N+1_ first. What pretty obviously follows from this is the need for some priority queue.
The consumer needs to take _N_ events before _N+1_ events.

So that more or less builds the consumer state machine. The _producer_ at this point needs one other step. The producer
should determine if the last message was a segment tombstone before writing. I don't _think_ there is an issue acquiring
a read lock after acquiring a write lock, and the write lock is kinda acting as a semaphore.

### 2025-10-16

'Total length' and 'subrecord length' are actually equivalent but 'subrecord length' (the length of everything other
than crc, the 'total length' field itself, and reverse-encoded length) is more accurate to describe what

### 2025-10-15

While my intuition was correct on the producer offset, the consumer offset was wrong for a symetric reason: my intention
was to begin at the current consumer offset, but nothing was actually enforcing that in the code.

### 2025-10-13

I am 75% sure that the bug is that the consumer does not stop at the incoming consumer offset (there is no current limit
set to actually accomplish this), so the consumer will catch up with the state of the file rather than where it was
'supposed' to be for the incoming offset. The only thing that gives me pause is that `b` in the following takes the
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
callback, so I will need to build some simple event loop and keep track of producer offsets. Also, if a message was
truncated, but we do have the length of the message that gives us a great hint about hard producer resets, and so may be
worth keeping. Also, as of the current commit the mechanism for hard producer transition will only work if there is a
live consumer. This wouldn't be true if the messages had inverted length as the last field, but that can probably wait.
There are also no current affordances for controll messages.

### 2025-10-08-1

You _have_ to fix the fact that the consumer and producer have entirely different ways to count bit length. The consumer
method is probably better, but this is very confusing.

Okay - a 'fetch interior' record batch is an 8k or smaller read buffer inside of a fetch. The final round of
deserialisation is the trickiest and will not only involve the file locking trick below but also something of a
lookahead: without some ceiling on batches where we round down to the nearest message boundary (and, well maybe even
_with_ that) we don't want perfectly intact messages on large writes to be discarded just because they fall across a
fetch boundary. This means that the consumer needs to handle messages across read buffers similarly to messages cut
across fetches. If the length and CRCs match, then the consumer is free to keep on going. But if it _fails_ the CRC
check, then the consumers will need to race to zero out the last truncated message of the previous fetch.

If there is a legal message in the first segment of the next fetch, consumers keep doing what they are doing. The
nightmare scenario would be fetch _N_ ending with a truncated message, fetch _N+1_ starting with a truncated message
followed by legal messages that don't look legal because the truncated message has thrown every length off. _That_ is as
situation where the messages ending with reverse length would save us, because then consumers could read in reverse
order until they find the last illegal message and zero it out.

Given that this is theoretically solvable, I am more comfortable saying that an illegal fetch start after an illegal
fetch end is irrecoverable.

### 2025-10-08-0

While I've done more thinking about incomplete message updates from producers, sitting down to write the consumer
implementation has made me think about how if a producer writes 100+ Mb of messages in one go we will _not_ want to read
the whole thing into memory. I haven't yet put a ceiling on write sizes but will need to do so eventually. But anyways
apparently people like using 8k read buffers (there is some empirical work backing this). As far as disambiguating
failing writes from incomplete writes, Jesse disappointingly said that it was out of is wheelhouse so my search
continues on this. But we'll be forced to handle this for the batched reads on the consumer.

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

Without a consumer operating at some point on the stream to populate the index, the first producer can't assume that any
valid messages are on the stream, so they have to scan through the whole stream. This is a reason to keep segments on
the shorter side, but without consumers there also isn't a good reason to even operate the stream in the first place.

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

Also, there should be some message size ceiling where batches are broken up on the consumer side to prevent someone from
ramming a massive message

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
.puro file but it can demonstrate that there is not a segment tombstone on the current segment, that is something that
should be logged. The thing this defends against is accidental creation of a segment file that was not created by a Puro
'steward'. When a producer is starting up it needs to be congnisant of spurious segments. The producers are not in a
good spot to do anything about this but consumers will and stewards may have directory listening capabilities. If a
consumer sees a new segment and then checks that it a) is at the end of it's currently active segment and b) a segment
tombstone has not been placed then it could place a spurious segment tombstone onto the spurious topic. Producers can/
should be setup such that if there is a segment without a segment tombstone of lower order than the highest order
segment, that it will wait until a predetermined timeout. This way if it is first to read the spurious segment it can
wait until a client or steward can act on it first. This of course leaves open the possibility that, if there are no
active consumers, the producer cannot start. But in this case the

I do not think that active consumers will really be impacted by this. They only respond to a segment tombstone, and the
producers are not rotating segments on their own volition. The steward that places the segment tombstone could place a
spurious segment tombstone on the spurious segment before forcing a producer rollover - order is important here, but it
is also pretty easy to establish. Ultimately this is congruent with many _Little Book of Sempahores_ problems, but files
are of course more complicated than sempahores just because of partial and nonexclusive file locking.

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
shouldn't be an issue to acquire a read lock within the write lock to check for rollover before write.

### 2025-09-29

The incremental CRC calculations were introducing the zero calculation twice. There are a couple of places where I am
returning an empty result or null when something invalid happens. Result types should be used instead, but probably not
the full Arrow result type just for dependency management reasons. It wouldn't even be that bad of an idea to provide an
extension function in another Gradle module that could do translation, but at this point I should read some ways that
F#/Rust/Haskell libraries design result types for operations like these

### 2025-09-28

`ConsumerTest#Happy Path getRecord` has a different crc8 for the encoded total length on serialisation and
deserialisation. This is not because they differ on VLQ - there is some subtle difference between immediate and
incremental calculation of CRC8s.

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

The CRC covers the entire rest of the message. Message length computed from total, topic, and key lengths. I am not
super confident in the 'what happens if the write is incomplete' which makes me think that an index in the directory
makes sense

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
shouldn't be a huge pain. Keep in mind that GC impacting options may matter more for message latency than message
throughput.
