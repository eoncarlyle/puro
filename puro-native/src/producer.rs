mod producer {
    use crate::producer::producer::ProducerErrorKind::{
        Io, MangedSegmentOffset, NotImplemented, U24ChangeMeLater,
    };
    use crate::record::{BLOCK_END_RECORD_SIZE, BLOCK_START_RECORD_SIZE, PuroRecord};
    use crate::segment;
    use crate::segment::{
        ACTIVE_SEGMENT_KEY, BlockBoundaryRecordType, ReadErrorKind, U24_MAX, get_u24_arr,
        maybe_parse_subblock_length_from_boundary_record, maybe_segment_order_from_dir,
        open_segment,
    };
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
                if U24_MAX - total < record_size {
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

                    let [_, a, b, c] = first_four_bytes;
                    let segment_recorded_offset = get_u24_arr([a, b, c]);

                    let verify_result =
                        self.verify_existing_segment(segment_file, segment_recorded_offset);

                    if verify_result.is_ok() {

                        // TODO
                        // 1) Write the block start record and as much of the record as will fit
                        // into the buffer
                        // 2) Keep writing into the buffer until you reach the end of the block.
                        // If there is enough space to write the block end message then do so,
                        // otherwise write it in the final message
                        // 3) `File.sync_all`
                        // 4) Toggle the block signal bit
                        // 5) `File.sync_all`
                    } else {
                        // TODO Recovery
                        return Err(ProducerErrorKind::MangledSegment);
                    }

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
        // about are truncations. If internal records are broken that isn't the end of the world
        // (probably)
        fn verify_existing_segment(
            self,
            mut segment_file: &File,
            segment_recorded_offset: u32,
        ) -> Result<(), ProducerErrorKind> {
            // We are making the assumption that the local offset is always the start of a block...
            // ...or the start of the segment entirely if it is zero
            // Originally I used the `self.offset` but I don't think the producer's offset is
            // actually relevant? The point of this is that another producer has vouched for the
            // offset that is provided on the segment
            match segment_file.metadata().map(|a| a.len()) {
                Ok(size) if size > U24_MAX as u64 => Err(U24ChangeMeLater),
                // TODO harden predicate, see 2026.10.01 note
                // The cast below is only safe because of the starting U24 max comparison
                Ok(segment_size)
                    if segment_size <= U24_MAX as u64
                        && segment_size >= BLOCK_START_RECORD_SIZE as u64
                        && segment_recorded_offset + BLOCK_START_RECORD_SIZE
                            < segment_size as u32 =>
                {
                    // Iterate through signal blocks to check integrity
                    let mut current_offset = segment_recorded_offset;

                    // TODO bad, unsafe, unnecessary cast
                    while current_offset < segment_size as u32 {
                        //Off-by-1-potential
                        let maybe_start_subblock_length =
                            maybe_parse_subblock_length_from_boundary_record(
                                segment_file,
                                segment_recorded_offset,
                                BlockBoundaryRecordType::Start,
                            );

                        let subblock_length =
                            maybe_start_subblock_length.and_then(|start_subblock_length| {
                                let maybe_end_subblock_length =
                                    maybe_parse_subblock_length_from_boundary_record(
                                        segment_file,
                                        segment_recorded_offset,
                                        BlockBoundaryRecordType::End,
                                    );

                                match maybe_end_subblock_length {
                                    Ok(end_subblock_length)
                                        if start_subblock_length == end_subblock_length =>
                                    {
                                        Ok(start_subblock_length)
                                    }
                                    _ => Err(ReadErrorKind::Mangled), //TODO recovery
                                }
                            });

                        match subblock_length {
                            Ok(subblock_length) => {
                                current_offset =
                                    current_offset + subblock_length + BLOCK_END_RECORD_SIZE
                            } //Off-by-1-potential
                            // TODO Consider this
                            _ => return Err(ProducerErrorKind::MangledSegment),
                        }
                    }
                    Ok(())
                }
                Ok(size) if size < segment_recorded_offset as u64 => {
                    // TODO cleanup possible, but requires full-segment cleanup...
                    // TODO ...not a very big priority, see 2026.10.01 note
                    Err(MangedSegmentOffset)
                }
                Ok(size) if segment_recorded_offset + BLOCK_START_RECORD_SIZE < size as u32 => {
                    Err(MangedSegmentOffset)
                }
                _ => Err(Io),
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
        MangedSegmentOffset,
        MangledSegment, //I'm not sure how good of a name this actually is
    }

    enum ProducerSegmentState {
        Init,
        Ready { known_safe_offset: u32 },
        Cleanup { known_safe_offset: u32 },
    }
}
