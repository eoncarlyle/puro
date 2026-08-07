# Journal

## Reference

Canonical 

Variable integer messages
```text
crc: uint8
totalLength: varint
topicLength: varint
topic: byte[]
keyLength: varint
key: byte[]
value: byte[]
```

## 2026.08.06

Bill told me about using the `From<T>` trait which helps. Also, it seems that the question mark operator is simply 
more ergonomic than using long `map`/`and_then` chains which I am more used to.

## 2026.08.05

`inner_segment_order` only exists to make the `?` syntax play nice with my own `Result<T, E>` types. A) is this an 
idiomatic way to roll your own errors and B) Is there a better way? I will also want to actually check the ends of 
segments tomorrow.

## 2026.08.04

VLQ variable integers are a fun challenge but they don't need to be done just quite yet. Not for nothing, a lot of 
bugs can come from these. But they are very much optional and don't need to be in the first attempt.