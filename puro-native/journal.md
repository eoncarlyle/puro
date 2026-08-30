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

## 2026.08.30

Use `hexdump -C` instead, turns out the segment writes were fine

## 2026.08.20

I was pretty wed to the idea of not having a stream header for any reason (which may have made sense when using VLQs)
but without having some indication of the progress that has been made, there is no good alternative to new 
consumers having to stop the entire segment in order to check integrity. The better option is to bite the bullet and 
include the 

Block start format
- First byte: Either `bx11110000`/`0xF0` or `bxc01110000`/`0x70`, the prior if active and the former if inactive.
- Next three bytes: First unverified offset (reading should be start of block)
- Block start message
  - First byte: block start Crc8
  - Next three bytes: subrecord length
  - Next three bytes: topic length
  - Next byte: topic (special: block start, otherwise variable)
  - Next three bytes: key length (special: 0)
  - Next zero bytes: key (zero)
  - Remaining bytes: signal bit followed by int32 length of subblock

`10485760` is 10MiB and fits in 24 bits. That is the largest number that should exist in a message unless If


- A block needs to be smaller than an int32 otherwise will fill up whole segment, and the largest problem is how the 
  signal bits work in bleeding over

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