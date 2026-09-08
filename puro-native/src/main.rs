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
    use crate::record::PuroRecord;
    use crate::segment::SegmentError::{FileError, MangledSegment};
    use crate::segment::producer::ProducerError;
    use file_guard::Lock;
    use std::fs::{DirEntry, File, OpenOptions};
    use std::io::{Error, ErrorKind, Read};
    use std::path::{Path, PathBuf};
    use std::{fs, io};

    #[derive(Clone)]
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
    const SEGMENT_PREFIX: &str = "segment";

    const ACTIVE_SEGMENT_KEY: u8 = 0xF0;
    const INACTIVE_SEGMENT_KEY: u8 = 0x70;

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

    // This function is only really safe to run in static contexts, because locks are not held
    // while evaluating segment status. This makes it race condition prone.
    pub fn get_active_segment(stream_directory: &Path) -> Result<Option<u32>, SegmentError> {
        if stream_directory.is_dir() {
            let mut active: Result<Option<u32>, SegmentError> = Ok(None);
            for entry in fs::read_dir(stream_directory)? {
                // TODO not really sure if `if let` is the best way here
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_file() {
                        if segment_extension_match(&entry) {
                            if let Some(order) = maybe_segment_order(&entry) {
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
                                        Err(SegmentError::DuplicateActiveSegments)
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
            Err(SegmentError::BadPath)
        }
    }

    fn open_segment(stream_directory: &Path, segment_order: u32) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(stream_directory.join(format!(
                "{}{}.{}",
                SEGMENT_PREFIX, segment_order, FILE_EXTENSION
            )))
    }

    mod producer {
        use crate::record::PuroRecord;
        use crate::segment::{maybe_segment_order, open_segment};
        use file_guard::Lock;
        use std::fs::File;
        use std::io::{Error, ErrorKind};
        use std::path::Path;
        use std::sync::atomic::AtomicU32;
        use std::{fs, io};

        struct Producer<'a> {
            stream_directory: &'a Path,
            maximum_write_batch_size: u16, //In records, not bytes
            current_segment_order: AtomicU32,
            offset: AtomicU32,
            read_buffer: Vec<u8>,
            state: ProducerSegmentState,
        }

        //TODO: Need to do validation on the read_buffer
        fn new(
            stream_directory: &Path,
            maybe_maximum_write_batch_size: Option<u16>,
            read_buffer: Vec<u8>,
        ) -> Producer {
            Producer {
                stream_directory,
                maximum_write_batch_size: maybe_maximum_write_batch_size.unwrap_or(8192),
                current_segment_order: AtomicU32::new(0),
                offset: AtomicU32::new(0),
                read_buffer,
                state: ProducerSegmentState::Init,
            }
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
                //- Bump length

                for puro_record in puro_records {
                    if puro_record.key.is_empty() || puro_record.value.is_empty() {
                        return Err(ProducerError::IllegalRecord);
                    }
                }
                Ok(())
            }

            pub fn with_active_segment<F>(self, func: F) -> Result<(), ProducerError>
            where
                F: Fn(Vec<PuroRecord>) -> Result<(), ProducerError>,
            {
                let orders: Result<Vec<u32>, io::Error> =
                    fs::read_dir(self.stream_directory).map(|res| {
                        res.filter_map(|entry| entry.ok().and_then(|e| maybe_segment_order(&e)))
                            .collect()
                    });

                let r_files: Result<Vec<io::Result<File>>, Error> = orders.map(|res| {
                    res.iter()
                        .map(|order| {
                            let stream_directory = self.stream_directory;
                            open_segment(stream_directory, *order)
                        })
                        .collect()
                });

                let r_locks: Result<Vec<_>, _> = r_files.map(|files| {
                    files
                        .iter()
                        .map(|mut r_file| match r_file {
                            Ok(mut file) => file_guard::lock(&mut file, Lock::Exclusive, 0, 4),
                            _ =>  Err(ErrorKind::InvalidData.into())
                        })
                        .collect()
                });

                Ok(())
            }
        }

        pub(crate) enum ProducerError {
            BufferOverflow,
            IllegalRecord,
            IllegalSegments,
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
