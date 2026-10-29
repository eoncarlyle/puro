mod crc8;
mod record;
mod segment;
mod producer;

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
