#![feature(portable_simd)] // Enable nightly SIMD feature

use anyhow::{Context, Result};
use memmap::MmapOptions;
use rayon::prelude::*;

use std::{
    fs::File,
    simd::{i32x8, i64x8, num::SimdInt},
    time::Instant,
};

fn get_sorted_vectors(file_path: &str) -> Result<(Vec<i64>, Vec<i64>)> {
    let file = File::open(file_path).context("Failed to open file")?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let initial_capacity = mmap.len() / 2;

    let (mut col1, mut col2): (Vec<i64>, Vec<i64>) = mmap
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

    rayon::join(|| col1.par_sort_unstable(), || col2.par_sort_unstable());

    Ok((col1, col2))
}

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

fn compute_distance(v1: &[i64], v2: &[i64]) -> i64 {
    const CHUNK_SIZE: usize = 8; // Changed to match SIMD vector size
    let len = v1.len();
    let chunks = len / CHUNK_SIZE;

    let sum: i64 = (0..chunks)
        .into_par_iter()
        .map(|i| {
            let start = i * CHUNK_SIZE;
            let v1_simd = i64x8::from_slice(&v1[start..start + CHUNK_SIZE]);
            let v2_simd = i64x8::from_slice(&v2[start..start + CHUNK_SIZE]);
            let diff = (v1_simd - v2_simd).abs();
            diff.reduce_sum()
        })
        .sum();

    // Handle remaining elements if any
    let remainder_sum: i64 = (chunks * CHUNK_SIZE..len)
        .map(|i| (v1[i] - v2[i]).abs())
        .sum();

    sum + remainder_sum
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

        match get_sorted_vectors(file) {
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
