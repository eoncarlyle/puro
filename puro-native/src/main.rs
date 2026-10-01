mod record {
    // 1 (CRC8) + 3 (subrecord length) + 3 (topic length) + 1 (topic) + 3 (key length) + 0 (key)
    // + 1 (signal byte) + 3 (subblock length)
    pub const BLOCK_START_RECORD_SIZE: u32 = 15;
    const BLOCK_END_RECORD_SIZE: u8 = 9;

    // 10 MiB
    const MAX_SIZE: u32 = 10485760;

    pub(crate) struct PuroRecord {
        pub topic: Vec<u8>,
        pub key: Vec<u8>,
        pub value: Vec<u8>,
    }

    impl PuroRecord {
        fn new(topic: Vec<u8>, key: Vec<u8>, value: Vec<u8>) -> Result<PuroRecord, ()> {
            // Topic has to be specified but technically nothing else does
            // TODO make usize safe
            if topic.is_empty() || (topic.len() + key.len() + value.len() > MAX_SIZE as usize) {
                Err(())
            } else {
                Ok(PuroRecord { topic, key, value })
            }
        }
    }

    pub(crate) enum ControlTopic {
        SegmentTombstone, //(vec![0u8]),
        InvalidBlock,     //(vec![1u8]),
        BlockStart,       //(vec![2u8]),
        BlockEnd,         //(vec![3u8]),
    }
}

mod segment {
    use crate::segment::SegmentErrorKind::{FileError, MangledSegment};
    use byteorder::{ByteOrder, LittleEndian};
    use file_guard::Lock;
    use std::fs::{DirEntry, File, OpenOptions};
    use std::io::{Error, Read};
    use std::path::{Path, PathBuf};
    use std::u32::MAX;
    use std::{fs, io};
    use crate::record::PuroRecord;

    // Note: I dn';
    #[derive(Clone)]
    pub enum SegmentErrorKind {
        BadPath,
        FileError,
        DuplicateActiveSegments,
        MangledSegment,
    }

    //TODO: Error kinds for simple reads rather than segment handling

    impl From<Error> for SegmentErrorKind {
        fn from(_value: Error) -> Self {
            FileError
        }
    }

    const FILE_EXTENSION: &str = "puro";
    const SEGMENT_PREFIX: &str = "segment";

    pub(crate) const ACTIVE_SEGMENT_KEY: u8 = 0xF0;
    const INACTIVE_SEGMENT_KEY: u8 = 0x70;

    // 1 (CRC8) + 3 (subrecord length) + 3 (topic len) + 1 (topic) + 3 (key length) + 0 (key)


    // TODO actually justify this
    pub(crate) const U24_MAX: u32 = 2 << 23; //== 2^24

    fn segment_extension_match(entry: &DirEntry) -> bool {
        match entry.path() {
            path if path.is_file() => {
                let extension = path.extension().and_then(|os_str| os_str.to_str());
                match extension {
                    Some(a) if a.eq(FILE_EXTENSION) => true,
                    _ => false,
                }
            }
            _ => false,
        }
    }

    pub(crate) fn maybe_segment_order_from_dir(entry: &DirEntry) -> Option<u32> {
        maybe_segment_order_from_path(entry.path())
    }

    pub(crate) fn maybe_segment_order_from_path(path: PathBuf) -> Option<u32> {
        match path {
            path if path.is_file() => {
                let stem = path.file_stem()?;
                let stem_str = stem.to_str()?;
                let maybe_digit = stem_str.strip_prefix(SEGMENT_PREFIX)?;
                maybe_digit.parse::<u32>().ok()
            }
            _ => None,
        }
    }

    // This function is only really safe to run in static contexts, because locks are not held
    // while evaluating segment status. This makes it race condition prone.
    pub fn get_active_segment(stream_directory: &Path) -> Result<Option<u32>, SegmentErrorKind> {
        if stream_directory.is_dir() {
            let mut active: Result<Option<u32>, SegmentErrorKind> = Ok(None);
            for entry in fs::read_dir(stream_directory)? {
                // TODO not really sure if `if let` is the best way here
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_file() {
                        if segment_extension_match(&entry) {
                            if let Some(order) = maybe_segment_order_from_dir(&entry) {
                                let r_first_segment_byte = open_segment(stream_directory, order)
                                    .and_then(|mut file| {
                                        let r_guard =
                                            file_guard::lock(&mut file, Lock::Shared, 0, 4);
                                        match r_guard {
                                            Ok(mut guard) => {
                                                let mut buf = [0u8; 1];
                                                guard.read_exact(&mut buf).map(|_| buf[0])
                                            }
                                            Err(err) => Err(err),
                                        }
                                    });

                                let result = match (active.clone(), r_first_segment_byte) {
                                    (_, Err(_)) => Err(FileError), //TODO lame and you know it
                                    (Ok(Some(_)), Ok(ACTIVE_SEGMENT_KEY)) => {
                                        Err(SegmentErrorKind::DuplicateActiveSegments)
                                    }
                                    (Ok(None), Ok(ACTIVE_SEGMENT_KEY)) => Ok(true),
                                    (_, Ok(INACTIVE_SEGMENT_KEY)) => Ok(false),
                                    _ => Err(MangledSegment),
                                };

                                match result {
                                    Ok(true) => active = Ok(Some(order)),
                                    Err(e) => {
                                        active = Err(e);
                                        break;
                                    }
                                    _ => (),
                                }
                            }
                        }
                    }
                }
            }
            active
        } else {
            // TODO get a better error type
            Err(SegmentErrorKind::BadPath)
        }
    }

    pub(crate) fn open_segment(stream_directory: &Path, segment_order: u32) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(stream_directory.join(format!(
                "{}{}.{}",
                SEGMENT_PREFIX, segment_order, FILE_EXTENSION
            )))
    }

    pub(crate) fn parse_block_start() -> Result<PuroRecord, ()> {}

    pub(crate) fn get_u24(a: u8, b: u8, c: u8) -> u32 {
        let a = a as u32;
        let b = (b as u32) << 8;
        let c = (c as u32) << 16;
        a + b + c
    }
}
mod producer {
    use crate::producer::ProducerErrorKind::{IllegalSegments, Io, MangedSegmentOffset, NotImplemented, U24ChangeMeLater};
    use crate::record::{PuroRecord, BLOCK_START_RECORD_SIZE};
    use crate::segment;
    use crate::segment::SegmentErrorKind::FileError;
    use crate::segment::{
        ACTIVE_SEGMENT_KEY, U24_MAX, get_u24, maybe_segment_order_from_dir,
        maybe_segment_order_from_path, open_segment,
    };
    use byteorder::{ByteOrder, LittleEndian};
    use file_guard::Lock;
    use std::fs::File;
    use std::io::Read;
    use std::path::Path;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering::Relaxed;
    use std::{fs, io};

    const MAXIMUM_READ_BUFFER_SIZE: u16 = 16384;

    struct Producer<'a> {
        stream_directory: &'a Path,
        maximum_write_batch_size: u16, //In records, not bytes
        current_segment_order: AtomicU32,
        offset: AtomicU32,
        read_buffer_size: u16,
        read_buffer: [u8; MAXIMUM_READ_BUFFER_SIZE as usize],
        state: ProducerSegmentState,
    }

    impl Producer<'_> {
        fn new(
            stream_directory: &Path,
            maybe_maximum_write_batch_size: Option<u16>,
            read_buffer_size: u16,
        ) -> Result<Producer, ()> {
            if read_buffer_size >= MAXIMUM_READ_BUFFER_SIZE {
                Ok(Producer {
                    stream_directory,
                    maximum_write_batch_size: maybe_maximum_write_batch_size.unwrap_or(8192),
                    current_segment_order: AtomicU32::new(0),
                    offset: AtomicU32::new(0),
                    read_buffer_size,
                    read_buffer: [0; MAXIMUM_READ_BUFFER_SIZE as usize],
                    state: ProducerSegmentState::Init,
                })
            } else {
                Err(())
            }
        }
    }

    //TODO: Need to do validation on the read_buffer

    impl Producer<'_> {
        // Why the dyn for iterator? Virtual method call? Unbounded iterator size?
        fn send(self, puro_records: Vec<PuroRecord>) -> Result<(), ProducerErrorKind> {
            //- Determine if request is legal
            //- Acquire file lock
            //- Check integrity of segment between offset and end-of-file if init, otherwise just
            //      send signal bits and also check if tombstoned.
            //- Check length differential/determine if tombstoning necessary
            //- Write records
            //- Toggle signal bit
            //- Bump length

            let mut total = 0;

            for record in &puro_records {
                let record_size =
                    (record.topic.len() + record.key.len() + record.value.len()) as u32;
                if segment::U24_MAX - total < record_size as u32 {
                    //TODO sloppy sizing
                    return Err(ProducerErrorKind::IllegalRecordSend);
                }
                total = total + record_size;
            }

            self.send_verified(puro_records)
        }

        fn send_verified(self, puro_records: Vec<PuroRecord>) -> Result<(), ProducerErrorKind> {
            // TODO: Use the `current_segment_order`
            //  As written this currently assumes nothing about the segment state, but if the
            //  locks are acquired on the segment implied by `current_segment_order` and those
            //  locks indicate it _is_ the active segment, we are _probably_ good to go.

            // Order passed down the chain
            let order_dir_entry_pairs: Result<Vec<_>, io::Error> =
                fs::read_dir(self.stream_directory).map(|res| {
                    res.filter_map(|entry| {
                        entry
                            .ok()
                            .and_then(|dir_entry| maybe_segment_order_from_dir(&dir_entry))
                    })
                        .collect()
                });

            let file_pairs: Vec<_> = order_dir_entry_pairs
                .and_then(|ords| {
                    ords.into_iter()
                        .map(|order| {
                            open_segment(self.stream_directory, order)
                                .map(|segment_file| (segment_file, order))
                        })
                        .collect::<io::Result<Vec<_>>>()
                })
                .map_err(|_| Io)?;

            let maybe_lock_pairs: Vec<Option<_>> = file_pairs
                .iter()
                .map(|pair| {
                    file_guard::lock(&(pair.0), Lock::Exclusive, 0, 4)
                        .ok()
                        .map(|guard| (guard, pair.1))
                })
                .collect::<Vec<_>>();

            if maybe_lock_pairs.iter().any(Option::is_none) {
                // There's probably a better way to do this
                return Err(Io);
            }

            let first_four_byte_pairs = maybe_lock_pairs
                .iter()
                .flat_map(Option::iter)
                .map(|pair| {
                    let mut file_ref: &File = &((*pair).0);
                    let mut buf = [0u8; 4];
                    let _ = file_ref.read_exact(&mut buf);
                    // TODO make _very_ sure the guards are working here
                    (buf, *pair.0, pair.1)
                })
                .collect::<Vec<_>>();

            let active_segment_first_four_byte_pairs = first_four_byte_pairs
                .into_iter()
                .filter(|pair| {
                    let first_byte = (*pair).0[0];
                    first_byte == ACTIVE_SEGMENT_KEY
                })
                .collect::<Vec<_>>();

            if active_segment_first_four_byte_pairs.len() > 1 {
                return Err(NotImplemented);
            }

            match active_segment_first_four_byte_pairs.get(0) {
                Some(triplet) => {
                    let (first_four_bytes, segment_file, order) = *triplet;
                    self.current_segment_order.store(order, Relaxed);

                    // Only based off of the first four bits
                    let segment_recorded_offset = get_u24(
                        first_four_bytes[1],
                        first_four_bytes[2],
                        first_four_bytes[3],
                    );
                    let first_unconfirmed_segment =
                        self.verify_existing_segment(segment_file, segment_recorded_offset);

                    Ok(())
                }
                None => {
                    Err(NotImplemented) // TODO segment creation: will need to acquire lock, could race here
                }
            }
        }

        fn write_segment(self, segment_file: &File, puro_records: Vec<PuroRecord>) {}

        // We don't need to specify an end, because this will continue until the end.
        // The segment is just checking block boundaries, because ~90% of what we're concerned
        // about are truncations
        fn verify_existing_segment(
            self,
            segment_file: &File,
            segment_recorded_offset: u32,
        ) -> Result<(), ProducerErrorKind> {
            // We are making the assumption that the local offset is always the start of a block...
            // ...or the start of the segment entirely if it is zero
            // Originally I used the `self.offset` but I don't think the producer's offset is
            // actually relevant? The point of this is that another producer has vouched for the
            // offset that is provided on the segment
            match segment_file.metadata().map(|a| a.len()) {
                Some(size) if size > U24_MAX => Err(U24ChangeMeLater),
                // TODO harden predicate, see 2026.10.01 note
                Some(size) if size <= U24_MAX && size >= BLOCK_START_RECORD_SIZE && size && segment_recorded_offset + BLOCK_START_RECORD_SIZE < size => {
                    Err(Io)
                },
                Some(size) if size > segment_recorded_offset => {
                    // TODO cleanup possible, but requires full-segment cleanup...
                    // TODO ...not a very big priority, see 2026.10.01 note
                    Err(MangedSegmentOffset)
                },
                Some(size) if segment_recorded_offset + BLOCK_START_RECORD_SIZE < size  => Err(MangedSegmentOffset),
                _ => Err(Io)
            }
        }
    }

    pub(crate) enum ProducerErrorKind {
        BufferOverflow,
        IllegalRecordSend,
        IllegalSegments,
        Io,
        NotImplemented,
        U24ChangeMeLater,
        MangedSegmentOffset
    }

    enum ProducerSegmentState {
        Init,
        Ready { known_safe_offset: u32 },
        Cleanup { known_safe_offset: u32 },
    }
}

#[cfg(test)]
mod segment_test {
    use crate::segment::get_active_segment;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use tempfile::TempDir;

    #[test]
    fn test_active_segment_happy_path() {
        let dir = TempDir::new().expect("Temporary directory creation failed");

        let mut segment0 =
            File::create(dir.path().join("segment0.puro")).expect("Segment creation failed");
        let mut segment1 =
            File::create(dir.path().join("segment1.puro")).expect("Segment creation failed");
        let mut segment2 =
            File::create(dir.path().join("segment2.puro")).expect("Segment creation failed");
        File::create(&Path::new("spurious.txt")).expect("Spurious file creation failed");

        segment0
            .write_all(&[0x70, 0x00, 0x00, 0x0F])
            .expect("Segment write failed");
        segment1
            .write_all(&[0x70, 0x00, 0x00, 0x0F])
            .expect("Segment write failed");
        segment2
            .write_all(&[0xF0, 0x00, 0x00, 0x0F])
            .expect("Segment write failed");

        let r_segment = get_active_segment(dir.path());

        assert!(match r_segment {
            Ok(Some(2)) => true,
            _ => false,
        })
    }
}

fn main() {
    let a = [1, 2, 3, 4, 5];
    println!("Hello, world!");
}
