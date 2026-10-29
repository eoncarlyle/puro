// 1 (CRC8) + 3 (subrecord length) + 3 (topic length) + 1 (topic) + 3 (key length) + 0 (key)
// + 1 (signal byte) + 3 (subblock length)
pub const BLOCK_START_RECORD_SIZE: u32 = 15;

// 1 (CRC8) + 3 (subrecord length) + 3 (topic length) + 1 (topic) + 3 (key length) + 0 (key)
// + 3 (subblock length)
pub const BLOCK_END_RECORD_SIZE: u32 = 14;

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

    fn serialised_byte_size(&self) -> u32 {
        //TODO Cast! bad!
        1 + 3 // CRC8 and subrecord length indicator
                + 3 //topic length
                + self.topic.len() as u32
                + 3 // key length indicator
                + self.key.len() as u32
                + self.value.len() as u32
    }
}

#[repr(u8)]
pub(crate) enum ControlTopic {
    SegmentTombstone = 0, //(vec![0u8]),
    InvalidBlock = 1,     //(vec![1u8]),
    BlockStart = 2,       //(vec![2u8]),
    BlockEnd = 3,         //(vec![3u8]),
}
