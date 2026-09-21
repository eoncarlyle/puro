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
    use crate::segment::SegmentError::{FileError, MangledSegment};
    use file_guard::Lock;
    use std::fs::{DirEntry, File, OpenOptions};
    use std::io::{Error, Read};
    use std::path::Path;
    use std::{fs, io};

    // Note: I dn';
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
        use crate::segment::SegmentError::FileError;
        use crate::segment::producer::ProducerError::{IllegalSegments, Io, NotImplemented};
        use crate::segment::{maybe_segment_order, open_segment, ACTIVE_SEGMENT_KEY};
        use file_guard::{FileGuard, Lock};
        use std::fs::{DirEntry, File};
        use std::io::Read;
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
                // TODO run `send_verified`
                Ok(())
            }

            fn send_verified<F>(
                self,
                puro_records: Vec<PuroRecord>,
            ) -> Result<(), ProducerError>
            where
                F: Fn(Vec<PuroRecord>) -> Result<(), ProducerError>,
            {
                // TODO: Use the `current_segment_order`
                //  As written this currently assumes nothing about the segment state, but if the
                //  locks are acquired on the segment implied by `current_segment_order` and those
                //  locks indicate it _is_ the active segment, we are _probably_ good to go.

                // For all I know `Path` can access the name but whatever
                let order_dir_entry_pairs: Result<Vec<(u32, DirEntry)>, io::Error> =
                    fs::read_dir(self.stream_directory).map(|res| {
                        res.filter_map(|entry| {
                            entry.ok().and_then(|dir_entry| {
                                maybe_segment_order(&dir_entry).map(|order| (order, dir_entry))
                            })
                        })
                            .collect()
                    });

                let file_pairs: Vec<(File, DirEntry)> = order_dir_entry_pairs
                    .and_then(|ords| {
                        ords.into_iter()
                            .map(|pair| {
                                open_segment(self.stream_directory, pair.0)
                                    .map(|file| (file, pair.1))
                            })
                            .collect::<io::Result<Vec<(File, DirEntry)>>>()
                    })
                    .map_err(|_| Io)?;

                let maybe_lock_pairs: Vec<Option<(FileGuard<&File>, &DirEntry)>> = file_pairs
                    .iter()
                    .map(|pair| {
                        file_guard::lock(&pair.0, Lock::Exclusive, 0, 4)
                            .ok()
                            .map(|guard| (guard, &pair.1))
                    })
                    .collect::<Vec<_>>();

                if maybe_lock_pairs.iter().any(Option::is_none) {
                    // There's probably a better way to do this
                    return Err(Io);
                }

                let first_bytes = maybe_lock_pairs
                    .iter()
                    .flat_map(Option::iter)
                    .map(|pair| {
                        let mut file_ref: &File = *((*pair).0);
                        let mut buf = [0u8; 1];
                        let _ = file_ref.read_exact(&mut buf);
                        (buf, pair.1)
                    })
                    .collect::<Vec<_>>();

                let active_segment_first_bytes = first_bytes.into_iter().filter(|pair| {
                    let first_byte = (*pair).0[0];
                    first_byte == ACTIVE_SEGMENT_KEY
                }).collect::<Vec<_>>();

                match active_segment_first_bytes.len() {
                    0 => Err(NotImplemented), // TODO segment creation: will need to acquire lock
                    1 =>  Ok(()),
                    _ => Err(IllegalSegments)
                }
            }


            fn verify_segment() {
                
            }
        }

        pub(crate) enum ProducerError {
            BufferOverflow,
            IllegalRecord,
            IllegalSegments,
            Io,
            NotImplemented
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
