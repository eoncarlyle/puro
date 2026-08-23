mod record {
    // 1 byte value, 0 byte key, 1 byte topic
    const BLOCK_START_RECORD_SIZE: u8 = 10;
    const BLOCK_END_RECORD_SIZE: u8 = 9;

    // 10 MiB
    const MAX_SIZE: u32 = 10485760;

    pub(crate) struct PuroRecord {
        pub topic: Vec<u8>,
        pub key: Vec<u8>,
        pub value: Vec<u8>,
    }

    pub(crate) enum ControlTopic {
        SegmentTombstone, //(vec![0u8]),
        InvalidBlock,     //(vec![1u8]),
        BlockStart,       //(vec![2u8]),
        BlockEnd,         //(vec![3u8]),
    }
}

mod segment {
    use crate::record::ControlTopic::SegmentTombstone;
    use crate::segment::SegmentError::{FileError, MangledSegment};
    use file_guard::Lock;
    use std::fs::{DirEntry, OpenOptions};
    use std::io::{Error, ErrorKind, Read};
    use std::path::{Path, PathBuf};
    use std::{fs, io};

    pub enum SegmentError {
        BadPath,
        FileError,
        DuplicateActiveSegments,
        MangledSegment,
    }

    impl From<Error> for SegmentError {
        fn from(value: Error) -> Self {
            FileError
        }
    }

    const FILE_EXTENSION: &str = "puro";
    const SEGMENT_PREFIX: &str = "stream";

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

    fn maybe_segment_order(entry: &DirEntry) -> Option<u32> {
        match entry.path() {
            path if path.is_file() => {
                let stem = path.file_stem()?;
                let stem_str = stem.to_str()?;
                let maybe_digit = stem_str.strip_prefix(SEGMENT_PREFIX)?;
                maybe_digit.parse::<u32>().ok()
            }
            _ => None,
        }
    }

    pub fn get_active_segment(stream_directory: &Path) -> Result<Option<u32>, SegmentError> {
        if stream_directory.is_dir() {
            let mut active: Option<u32> = None;
            for entry in fs::read_dir(stream_directory)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_file() {
                    if segment_extension_match(&entry) {
                        if let Some(order) = maybe_segment_order(&entry) {
                            let r_first_byte = OpenOptions::new()
                                .read(true)
                                .write(true)
                                .create(false)
                                .open(stream_directory.join(format!(
                                    "{}{}.{}",
                                    SEGMENT_PREFIX, order, FILE_EXTENSION
                                )))
                                .and_then(|mut file| {
                                    file_guard::lock(&mut file, Lock::Shared, 0, 4)
                                })
                                .and_then(|guard| {
                                    let file = *guard;
                                    let mut buf = [0u8; 1];
                                    let read = file.read_exact(&mut buf);
                                    read.map(|_| buf[0])
                                });

                            let result = match (active, r_first_byte) {
                                (_, Err(_)) => Err(FileError), //TODO lame and you know it
                                (Some(active), Ok(0xF0)) => Err(SegmentError::DuplicateActiveSegments),
                                (None, Ok(0xF0)) => Ok(true),
                                (_, Ok(0x70)) => Ok(false),
                                _ => Err(MangledSegment),
                            };

                            match result {
                                Err(err) => return Err(err),
                                Some(true) => active = Some(order)
                            }
                        };
                    }
                }
            }
            Ok(active)
        } else {
            // TODO get a better error type
            Err(SegmentError::BadPath)
        }
    }

    mod producer {
        use crate::record::PuroRecord;
        use std::path::Path;
        use std::sync::atomic::AtomicU32;
        struct Producer<'a> {
            stream_directory: &'a Path,
            maximum_write_batch_size: u32,
            read_buffer_size: u32,
            current_segment_order: AtomicU32,
            offset: AtomicU32,
            read_buffer: Vec<u8>,
            state: ProducerSegmentState,
        }

        impl Producer<'_> {
            // Why the dyn for iterator? Virtual method call? Unbounded iterator size?
            fn send(self, puro_records: Vec<PuroRecord>) -> Result<(), ProducerError> {
                //- Determine if request is legal
                //- Acquire file lock
                //- Check integrity of segment between offset and end-of-file if init, otherwise just
                //      send signal bits and also check if tombstoned.
                //- Check length differential/determine if tombstoning necessary
                //- Write records
                //- Toggle signal bit

                for puro_record in puro_records {
                    if (puro_record.key.is_empty() || puro_record.value.is_empty()) {
                        return Err(ProducerError::IllegalRecord);
                    }
                }
                Ok(())
            }
        }

        pub(crate) enum ProducerError {
            BufferOverflow,
            IllegalRecord,
        }

        enum ProducerSegmentState {
            Init,
            Ready { known_safe_offset: u32 },
            Cleanup { known_safe_offset: u32 },
        }
    }
}

#[cfg(test)]
mod segment_test {
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use std::thread::spawn;
    use tempfile::TempDir;

    #[test]
    fn test_active_segment_happy_path() {
        let dir = TempDir::new().expect("Temporary directory creation failed");

        let mut segment0 = File::create(&Path::new("segment0.puro")).expect("Segment creation failed");
        let mut segment1 = File::create(&Path::new("segment1.puro")).expect("Segment creation failed");
        let mut segment2 = File::create(&Path::new("segment2.puro")).expect("Segment creation failed");
        let spurious = File::create(&Path::new("spurious.txt")).expect("Spurious file creation failed");

        segment0.write_all(&*[0x70, 0x00, 0x00, 0x0F]).expect("Segment write failed");
        segment1.write_all(&*[0x70, 0x00, 0x00, 0x0F]).expect("Segment write failed");
        segment2.write_all(&*[0xF0, 0x00, 0x00, 0x0F]).expect("Segment write failed");

        // TODO: two simultaneous threads running the function
    }
}

fn main() {
    let a = [1, 2, 3, 4, 5];
    println!("Hello, world!");
}
