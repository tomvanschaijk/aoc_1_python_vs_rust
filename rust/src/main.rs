#![feature(portable_simd)]

use anyhow::{Context, Result};
use memmap::Mmap;

use std::{fs::File, simd::{i32x8, num::SimdInt}, time::Instant};

fn compute_distance(file_path: &str) -> Result<i64> {
    // Define the range for 5-digit numbers
    const MIN_VAL: i64 = 10_000;
    const MAX_VAL: i64 = 99_999;
    const RANGE: usize = (MAX_VAL - MIN_VAL + 1) as usize;

    // Initialize buckets
    let mut buckets1 = vec![0i64; RANGE];
    let mut buckets2 = vec![0i64; RANGE];

    // Open file and memory-map it
    let file = File::open(file_path).context("Failed to open file")?;
    let mmap = unsafe { Mmap::map(&file).context("Failed to memory-map file")? };

    // Process lines from the memory-mapped file
    for line in mmap.split(|&byte| byte == b'\n') {
        if !line.is_empty() {
            if let Some((num1, num2)) = parse_line(line) {
                buckets1[(num1 - MIN_VAL) as usize] += 1;
                buckets2[(num2 - MIN_VAL) as usize] += 1;
            }
        }
    }

    // Compute total distance
    let mut total_distance = 0;
    let mut j = 0;
    (0..RANGE).for_each(|i| {
        while buckets1[i] > 0 {
            while j < RANGE && buckets2[j] == 0 {
                j += 1;
            }
            if j < RANGE {
                let actual_num1 = i as i64 + MIN_VAL;
                let actual_num2 = j as i64 + MIN_VAL;
                total_distance += (actual_num1 - actual_num2).abs();
                buckets1[i] -= 1;
                buckets2[j] -= 1;
            }
        }
    });

    Ok(total_distance)
}

#[inline(always)]
fn parse_line(bytes: &[u8]) -> Option<(i64, i64)> {
    if bytes.len() < 11 {
        return None;
    }

    let num1_simd = i32x8::from([
        (bytes[0] - b'0') as i32, (bytes[1] - b'0') as i32, 
        (bytes[2] - b'0') as i32, (bytes[3] - b'0') as i32,
        (bytes[4] - b'0') as i32, 0, 0, 0
    ]);
    let num2_simd = i32x8::from([
        (bytes[6] - b'0') as i32, (bytes[7] - b'0') as i32, 
        (bytes[8] - b'0') as i32, (bytes[9] - b'0') as i32,
        (bytes[10] - b'0') as i32, 0, 0, 0
    ]);

    // SIMD vector for powers of 10 for multiplication
    let powers_of_ten = i32x8::from([10000, 1000, 100, 10, 1, 0, 0, 0]);

    // Do the multiplication
    let num1 = (num1_simd * powers_of_ten).reduce_sum() as i64;
    let num2 = (num2_simd * powers_of_ten).reduce_sum() as i64;

    Some((num1, num2))
}

fn main() -> Result<()> {
    let files = [
        "./data/input_1k.txt",
        "./data/input_10k.txt",
        "./data/input_100k.txt",
        "./data/input_1m.txt",
        "./data/input_10m.txt",
        "./data/input_50m.txt",
        "./data/input_100m.txt",
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
