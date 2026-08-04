mod record {
    // 1 byte value, 0 byte key, 1 byte topic
    const BLOCK_START_RECORD_SIZE: u8 = 10;
    const BLOCK_END_RECORD_SIZE: u8 = 9;

    // Block start: signal bit value
    // Block end:

    pub(crate) struct PuroRecord {
        pub topic: Vec<u8>,
        pub key: Vec<u8>,
        pub value: Vec<u8>
    }

    pub(crate) enum ControlTopic {
        SegmentTombstone(vec!(0u8)),
        InvalidBlock(vec!(1u8)),
        BlockStart(vec!(2u8)),
        BlockEnd(vec!(3u8))
    }
}

mod producer {
    use std::path::Path;
    use crate::producer::ProducerError::{BufferOverflow, IllegalRecord};
    use crate::record::PuroRecord;
    struct Consumer {
        stream_directory: Path,
        maximum_write_batch_size: u32,
        read_buffer_size: u32,
        current_segment_order: u32,
        offset: u32,
        read_buffer: Vec<u8>,
        state: ProducerSegmentState
    }

    impl Consumer {
        // Why the dyn for iterator? Virtual method call? Unbounded iterator size?
        fn send_many(&mut self, puro_records: Vec<PuroRecord>) -> Result<(), ProducerError> {

            //0: Determine if request is legal
            //1: Acquire file lock
            //2: Check integrity of segment between offset and end-of-file if init, otherwise just signal bits
            //3: Check length differential/determine if tombstoning necessary
            //4: Write records
            //5: Toggle signal bit

            for puro_record in puro_records {
                if(puro_record.key.is_empty() || puro_record.value.is_empty()) {
                    return Err(IllegalRecord)
                }
            }
            Ok(())
        }


        fn send_single(&mut self, puro_record: PuroRecord) -> Result<(), ProducerError> {
            Self::send_many(self, vec!(puro_record))
        }
    }

    enum ProducerError {
        BufferOverflow,
        IllegalRecord
    }

    enum ProducerSegmentState {
        Init,
        Ready {known_safe_offset: u32},
        Cleanup {known_safe_offset: u32}
    }
}

fn main() {
    let a = [1, 2, 3, 4, 5];
    println!("Hello, world!");
}
