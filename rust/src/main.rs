#![feature(portable_simd)] // Enable nightly SIMD feature

use anyhow::{Context, Result};
use memmap::MmapOptions;

use std::{
    fs::File,
    simd::{i32x8, num::SimdInt},
    time::Instant,
};

fn load_data(file_path: &str) -> Result<(Vec<i64>, Vec<i64>)> {
    let file = File::open(file_path).context("Failed to open file")?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let initial_capacity = mmap.len() / 2;

    let (col1, col2): (Vec<i64>, Vec<i64>) = mmap
        .split(|&line| line == b'\n')
        .filter_map(parse_line)
        .fold(
            (
                Vec::with_capacity(initial_capacity),
                Vec::with_capacity(initial_capacity),
            ),
            |(mut c1, mut c2), (a, b)| {
                c1.push(a);
                c2.push(b);
                (c1, c2)
            },
        );

    Ok((col1, col2))
}

fn parse_line(bytes: &[u8]) -> Option<(i64, i64)> {
    if bytes.len() < 11 {
        return None;
    }

    let num1_simd = i32x8::from([
        (bytes[0] - b'0') as i32,
        (bytes[1] - b'0') as i32,
        (bytes[2] - b'0') as i32,
        (bytes[3] - b'0') as i32,
        (bytes[4] - b'0') as i32,
        0,
        0,
        0,
    ]);
    let num2_simd = i32x8::from([
        (bytes[6] - b'0') as i32,
        (bytes[7] - b'0') as i32,
        (bytes[8] - b'0') as i32,
        (bytes[9] - b'0') as i32,
        (bytes[10] - b'0') as i32,
        0,
        0,
        0,
    ]);

    // SIMD vector for powers of 10 for multiplication
    let powers_of_ten = i32x8::from([10000, 1000, 100, 10, 1, 0, 0, 0]);

    // Do the multiplication
    let num1 = (num1_simd * powers_of_ten).reduce_sum() as i64;
    let num2 = (num2_simd * powers_of_ten).reduce_sum() as i64;

    Some((num1, num2))
}

fn compute_distance(col1: &Vec<i64>, col2: &Vec<i64>) -> i64 {
    // In the source data, we all have 5-digit numbers. So our range is known. This lends itself to bucket sort!
    let min_val = 10_000;
    let max_val = 99_999;
    let range = (max_val - min_val + 1) as usize;

    let mut buckets1 = vec![0; range];
    let mut buckets2 = vec![0; range];

    // Populate buckets (adjust for offset)
    for &num in col1 {
        buckets1[(num - min_val) as usize] += 1;
    }
    for &num in col2 {
        buckets2[(num - min_val) as usize] += 1;
    }

    let mut total_distance = 0;
    let mut j = 0;

    // Process buckets
    (0..range).for_each(|i| {
        while buckets1[i] > 0 {
            while j < range && buckets2[j] == 0 {
                j += 1;
            }
            if j < range {
                let actual_num1 = i as i64 + min_val;
                let actual_num2 = j as i64 + min_val;
                total_distance += (actual_num1 - actual_num2).abs();
                buckets1[i] -= 1;
                buckets2[j] -= 1;
            }
        }
    });

    total_distance
}

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

        match load_data(file) {
            Ok((v1, v2)) => {
                let distance = compute_distance(&v1, &v2);
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
