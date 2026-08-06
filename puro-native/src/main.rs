mod record {
    // 1 byte value, 0 byte key, 1 byte topic
    const BLOCK_START_RECORD_SIZE: u8 = 10;
    const BLOCK_END_RECORD_SIZE: u8 = 9;

    // Block start: signal bit value
    // Block end:

    pub(crate) struct PuroRecord {
        pub topic: Vec<u8>,
        pub key: Vec<u8>,
        pub value: Vec<u8>,
    }

    pub(crate) enum ControlTopic {
        SegmentTombstone(vec![0u8]),
        InvalidBlock(vec![1u8]),
        BlockStart(vec![2u8]),
        BlockEnd(vec![3u8]),
    }
}

mod segment {
    use std::any::type_name;
    use std::{fs, io};
    use std::fs::DirEntry;
    use std::path::Path;
    use crate::segment::SegmentError::FileError;

    pub enum SegmentError {
        BadPath,
        FileError,
    }

    const FILE_EXTENSION: &str = "puro";
    const SEGMENT_PREFIX: &str = "stream";

    fn segment_extension_match(entry: &DirEntry) -> bool {
        entry.path().is_file()
            && entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str().filter(|ext_str| ext_str.eq(FILE_EXTENSION)))
                .is_some()
    }

    fn maybe_segment_order(entry: &DirEntry) -> Option<u32> {
            Some(entry)
            .filter(|entry| entry.path().is_file())
            .path()
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stm_str| match stm_str.strip_prefix(SEGMENT_PREFIX) {
                Some(maybe_digit) => maybe_digit.parse::<u32>().ok(),
                _ => None,
            });
    }

    // It is a little annoying that I have to define this seperate to get `?` workign with io results
    pub fn inner_segment_order(stream_directory: &Path) -> io::Result<Option<u32>> {
        let mut highest: Option<u32> = None;
        for entry in fs::read_dir(stream_directory)? {
            // I think this is wrong, because it will short-circuit on the first bad entry?
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                if segment_extension_match(&entry) {
                    // A little wasteful to do this with
                    let prefix = maybe_segment_order(&entry);
                    if prefix.map(|this_order| highest.filter(|highest_order| highest_order > *this_order)) {
                        highest = prefix;
                    }
                }
            }
        }
        Ok(None)
    }

    pub fn get_highest_segment_order(stream_directory: &Path) -> Result<Option<u32>, SegmentError> {
        if stream_directory.is_dir() {
            inner_segment_order(stream_directory).map_err(Err(FileError))
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
    struct Consumer {
        stream_directory: Path,
        maximum_write_batch_size: u32,
        read_buffer_size: u32,
        current_segment_order: AtomicU32,
        offset: AtomicU32,
        read_buffer: Vec<u8>,
        state: ProducerSegmentState,
    }

    impl Consumer {
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
