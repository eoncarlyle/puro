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

## 2026.09.22

From the docs:

> The BufReader<R> struct adds buffering to any reader.
> 
> It can be excessively inefficient to work directly with a Read instance. For example, every call to read on TcpStream > results in a system call. A BufReader<R> performs large, infrequent reads on the underlying Read and maintains an > in-memory buffer of the results.
> 
> BufReader<R> can improve the speed of programs that make small and repeated read calls to the same file or network 
> socket. It does not help when reading very large amounts at once, or reading just one or a few times. It also provides no advantage when reading from a source that is already in memory, like a Vec<u8>.

And as the O'Reily 'Programming Rust' points out:

> In Rust, `File` and `BufReader` are two separate library features, because sometimes you want files without 
> buffering, and sometimes you want buffering without files (for example, you may want to buffer input from the network).

I don't think there are big advantages to `BufReader` for Puro; `File.sync_all` is what matters but that should be 
a seperate concern anyway.

## 2026.09.20

"Taking a reference is a fundamentally different operation from moving" - if all you need to do is to read something,
often times all that is necessary is a reference rather than an owned value. Also, `.iter` provides references while
`.into_iter` transfers ownership.

Also, there is a real question as to if the active segment needs to be stored on the producers and the consumers if 
it will always be inferred from the segment state. This _could_ be used to speed up calculations - but that is a 
later consideration which I can TODO. Given that 'byzantine' (for lack of more specific term) consistency isn't my 
target, I think it is overkill to _then_ check for other segment activity.

The thing that I need to remember about the segment start offset is that it the softest requirement: updating block 
start messages/offsets is a greater priority than the block start segments. Well behaved producers will never update 
that segment offset unless they A) confirm existing segment health and increment or B) repair the segment and modify.
It is to _help_ producers, is is _not_ a strong health measure in and of itself.

This is a `&File` and I'm not crazy comfortable with that; the last thing I want is for the guard to get dropped and 
for it to allow for unlocked writes:

```rust
let first_byte_pairs: Vec<([u8; 1], &File, u32)>;
```

A few things I am thinking about
- I don't _think_ endianness matters to me because I am doing byte-order operations
- Block size must be heeded
- Rather than providing a read buffer as a vector, we could simply allocate a buffer up to 16384 in size and only 
  use a user specified amount of it. It will be easy to use more than what the user specified and I worry that this 
  will be the source of byte-buffer styled bugs. But I like the elegance of keeping things stack allocated: yeah, 
  let's do it this way
- ~~I am worried about the `fsync` of it all.~~ update: this is taken care of by `File.sync_all`.
- Should read and write operations have seperate buffers? It might be possible to populate a write buffer while 
  other operations are going on.
- I don't exactly know the best way to do large file reads is. Should I use cursors? What's the best way to be
  careful about those mentioned read buffer sizes? 
- `File.take` is a little less annoying than `File.read_exact`

## 2026.09.10

The following didn't work because file guard isn't implementing a required trait

```rust
let _guards = maybe_locks
    .iter()
    .flat_map(Option::iter)
    .map(|mut m| {
        let mut buf = [0u8; 1];
        let n = m.read_exact(&mut buf);
        buf
    })
    .collect::<Vec<_>>();
```

This is actually a situation where the compiler pointed out the issue
```text
    = help: trait `DerefMut` is required to modify through a dereference, but it is not implemented for `FileGuard<&File>`
```

## 2026.09.07
I've been trying to think through how acquiring the segment lock is supposed to work. We can't just acquire the 
active segment and continue after relinquishing the lock, because the active segment can change between when one 
producer determines the active segment and when it starts writing. So the actual event production needs to happen as 
the producer has the locks. As I'm writing this I don't see any problem with the producer locking _all_ files in one 
go. Á la that one 'Little Book of Sempahores' chapter the order that the producer needs to acquire the locks needs 
to be deterministic and also the same across all producers, but that isn't hard to guarantee.


It is worth pointing this out explicitly, but the unverified offset is a lower priority change than anything else; 
if the unverified offset isn't changed it doesn't break anything and just forces more work to be done by a 
subsequent producer.

Learning Rust note

```rust
    //This short circuits
    let _files: Vec<File> = orders
        .and_then(|ords| {
        ords.iter()
            .map(|order| open_segment(self.stream_directory, *order))
            .collect::<io::Result<Vec<File>>>()
    })?;

    //THis is way more annoying and wrong
    let files: Vec<io::Result<File>> = orders.map(|res| {
        res.iter()
            .map(|order| open_segment(self.stream_directory, *order))
            .collect()
    })?;
```

## 2026.08.30

Use `hexdump -C` instead, turns out the segment writes were fine

## 2026.08.20

I was pretty wed to the idea of not having a stream header for any reason (which may have made sense when using VLQs)
but without having some indication of the progress that has been made, there is no good alternative to new 
consumers having to stop the entire segment in order to check integrity. The better option is to bite the bullet and 
include the 

Segment start format
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