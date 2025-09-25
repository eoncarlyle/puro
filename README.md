# README

> **Puro** (Finnish): stream, streamlet, brook, creek (body of running water smaller than a river)

## Motivation
This is probably wildly ambitious but this is an attempt to make an 'SQLite of event streams'. 
The idea is an alternative to running Apache Kafka when producers and consumers are not separated by a network.
Rather than any daemonised processed, there will just be a log format and client libraries for producers and consumers.

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
- VLQ vs. long encoding
- Aggressive nulling out of allocated objects vs. leaving the garbage collector to do so
- Running Tail recursive IO monad vs. not
- Arena allocation vs. not
- Object pools vs. not

These are already a dizzying number of configurations, but with four options cobbling together the functions for each shouldn't be a huge pain.
Keep in mind that GC impacting options may matter more for message latency than message throughput.
