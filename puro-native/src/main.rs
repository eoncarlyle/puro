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
    use crate::segment::SegmentError::FileError;
    use std::fs::DirEntry;
    use std::io::Error;
    use std::path::{Path, PathBuf};
    use std::{fs, io};

    pub enum SegmentError {
        BadPath,
        FileError,
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
                    _ => false
                }
            }
            _ => false
        }
    }

    fn maybe_segment_order(entry: &DirEntry) -> Option<u32> {
        match entry.path() {
            path if path.is_file() =>  {
                let stem = path.file_stem()?;
                let stem_str = stem.to_str()?;
                let maybe_digit = stem_str.strip_prefix(SEGMENT_PREFIX)?;
                maybe_digit.parse::<u32>().ok()
            }
            _ => None
        }
    }

    pub fn get_highest_segment_order(stream_directory: &Path) -> Result<Option<u32>, SegmentError> {
        if stream_directory.is_dir() {
            let mut highest: Option<u32> = None;
            for entry in fs::read_dir(stream_directory)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_file() {
                    if segment_extension_match(&entry) {
                        let prefix = maybe_segment_order(&entry);
                        if prefix
                            .and_then(|this_order| {
                                highest.map(|highest_order| this_order < highest_order)
                            })
                            .is_some()
                        {
                            highest = prefix;
                        }
                    }
                }
            }
            Ok(None)
        } else {
            // TODO get a better error type
            Err(SegmentError::BadPath)
        }
    }
}

mod producer {
    use crate::producer::ProducerError::IllegalRecord;
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
                    return Err(IllegalRecord);
                }
            }
            Ok(())
        }
    }

    enum ProducerError {
        BufferOverflow,
        IllegalRecord,
    }

    enum ProducerSegmentState {
        Init,
        Ready { known_safe_offset: u32 },
        Cleanup { known_safe_offset: u32 },
    }
}

fn main() {
    let a = [1, 2, 3, 4, 5];
    println!("Hello, world!");
}
