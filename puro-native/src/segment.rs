use crate::crc8::Crc8;
use crate::record::{BLOCK_END_RECORD_SIZE, BLOCK_START_RECORD_SIZE, ControlTopic};
use crate::segment::ReadErrorKind::Mangled;
use crate::segment::SegmentErrorKind::{FileError, MangledSegment};
use file_guard::Lock;
use std::fs::{DirEntry, File, OpenOptions};
use std::io::{Error, Read};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::{fs, io};

// Note: I dn';
#[derive(Clone)]
pub enum SegmentErrorKind {
    BadPath,
    FileError,
    DuplicateActiveSegments,
    MangledSegment,
}

//These errors are for general reads rather than segment handling
pub enum ReadErrorKind {
    Checksum,
    Mangled,
    Io,
    LowSignalBit,
    NotImplemented, //TODO remove me
}

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
#[deprecated]
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
                                    let r_guard = file_guard::lock(&mut file, Lock::Shared, 0, 4);
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

pub(crate) fn get_u24_triple(a: u8, b: u8, c: u8) -> u32 {
    let a = a as u32;
    let b = (b as u32) << 8;
    let c = (c as u32) << 16;
    a + b + c
}

pub(crate) fn get_u24_arr(buf: [u8; 3]) -> u32 {
    let a = buf[0] as u32;
    let b = (buf[1] as u32) << 8;
    let c = (buf[2] as u32) << 16;
    a + b + c
}

// I don't love this personally
pub(crate) enum BlockBoundaryRecordType {
    Start,
    End,
}

pub(crate) fn maybe_parse_subblock_length_from_boundary_record(
    mut segment_file: &File,
    offset: u32,
    block_boundary_record_type: BlockBoundaryRecordType,
) -> Result<u32, ReadErrorKind> {
    let mut buf = match block_boundary_record_type {
        BlockBoundaryRecordType::Start => [0u8; BLOCK_START_RECORD_SIZE as usize],
        BlockBoundaryRecordType::End => [0u8; BLOCK_END_RECORD_SIZE as usize],
    };

    if let Err(_) = segment_file.read_exact_at(&mut buf, offset as u64) {
        return Err(ReadErrorKind::Io);
    }
    // This is a special read because normally we get the lengths in order to know how far to
    // read, but here the lengths are in order to check
    let expected_crc8 = buf[0];
    let subrecord_length = get_u24_triple(buf[1], buf[2], buf[3]);
    let topic_length = get_u24_triple(buf[4], buf[5], buf[6]);
    let topic = buf[7];

    let key_length = get_u24_triple(buf[8], buf[9], buf[10]);
    let signal_byte = buf[11];
    let subblock_length = get_u24_triple(buf[12], buf[13], buf[14]);

    let actual_crc8 = Crc8::create_lsb(130).calc(&buf[1..], (BLOCK_START_RECORD_SIZE - 1), 0);

    if actual_crc8 != expected_crc8 {
        Err(ReadErrorKind::Checksum)
    } else if signal_byte | 0xFE != 0xFF {
        Err(ReadErrorKind::LowSignalBit)
    } else if topic != ControlTopic::BlockStart as u8 {
        Err(Mangled)
    } else if (subrecord_length != BLOCK_START_RECORD_SIZE - 1
        || topic_length != 1
        || key_length != 0)
    {
        Err(Mangled)
    } else {
        Ok(subblock_length)
    }
}
