use anyhow::{Context, Result};
use memmap::Mmap;

use std::{fs::File, time::Instant};

fn compute_distance(file_path: &str) -> Result<i64> {
    // Define the range for 5-digit numbers
    const MIN_VAL: i64 = 10_000;
    const MAX_VAL: i64 = 99_999;
    const RANGE: usize = (MAX_VAL - MIN_VAL + 1) as usize;
    
    // Initialize buckets
    let mut buckets1 = vec![0i32; RANGE];
    let mut buckets2 = vec![0i32; RANGE];
    
    // Open file and memory-map it
    let file = File::open(file_path).context("Failed to open file")?;
    let mmap = unsafe { Mmap::map(&file).context("Failed to memory-map file")? };
    
    // Since we know the exact length in bytes of each line, we can simply step through it
    const STEP: usize = 13;
    for line in mmap.chunks_exact(STEP) {
        if let Some((num1, num2)) = parse_line(line) {
            unsafe {
                *buckets1.get_unchecked_mut((num1 - MIN_VAL) as usize) += 1;
                *buckets2.get_unchecked_mut((num2 - MIN_VAL) as usize) += 1;
            }
        }
    }

    // Compute total distance
    let mut total_distance = 0i64;
    let mut j = 0;
    (0..RANGE).for_each(|i| {
        while buckets1[i] > 0 {
            while j < RANGE && buckets2[j] == 0 {
                j += 1;
            }

            if j >= RANGE {
                break;
            }

            let count = buckets1[i].min(buckets2[j]);
            total_distance += count as i64 * (i as i64 - j as i64).abs();
            buckets1[i] -= count;
            buckets2[j] -= count;
        }
    });

    Ok(total_distance)
}

fn parse_line(bytes: &[u8]) -> Option<(i64, i64)> {
    if bytes.len() < 11 || unsafe { *bytes.get_unchecked(5) } != b' ' {
        return None;
    }

    // Unsafe slice access (validated by line length check)
    let num1 = parse_digits(unsafe { bytes.get_unchecked(0..5) })?;
    let num2 = parse_digits(unsafe { bytes.get_unchecked(6..11) })?;

    Some((num1, num2))
}

#[inline(always)]
fn parse_digits(bytes: &[u8]) -> Option<i64> {
    if bytes.len() != 5 {
        return None;
    }

    // Safe to parse now
    let d0 = (bytes[0] - b'0') as i64;
    let d1 = (bytes[1] - b'0') as i64;
    let d2 = (bytes[2] - b'0') as i64;
    let d3 = (bytes[3] - b'0') as i64;
    let d4 = (bytes[4] - b'0') as i64;

    Some(d0 * 10000 + d1 * 1000 + d2 * 100 + d3 * 10 + d4)
}

// main() remains the same
fn main() -> Result<()> {
    let files = [
        "./data/input_1k.txt",
        "./data/input_10k.txt",
        "./data/input_100k.txt",
        "./data/input_1m.txt",
    ];

    for file in files.iter() {
        println!("Processing {}...", file);

        let now = Instant::now();

        match compute_distance(file) {
            Ok(distance) => {
                println!(
                    "The answer is: {}, completed in {}ms\n",
                    distance,
                    now.elapsed().as_millis()
                );
            }
            Err(e) => {
                eprintln!("Error processing file '{}': {:?}\n", file, e);
            }
        }
    }

    Ok(())
}
