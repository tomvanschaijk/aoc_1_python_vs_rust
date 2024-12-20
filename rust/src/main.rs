use anyhow::{Context, Result};
use memmap::MmapOptions;
use rayon::prelude::*;

use std::{fs::File, time::Instant};

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

fn get_sorted_vectors(file_path: &str) -> Result<(Vec<i32>, Vec<i32>)> {
    let file = File::open(file_path).context("Failed to open file")?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };

    let (mut col1, mut col2): (Vec<i32>, Vec<i32>) = mmap
        .split(|&line| line == b'\n') // Split by newline
        .filter_map(parse_line) // Parse each line
        .fold((Vec::new(), Vec::new()), |(mut c1, mut c2), (a, b)| {
            c1.push(a);
            c2.push(b);
            (c1, c2)
        });

    col1.par_sort_unstable();
    col2.par_sort_unstable();

    Ok((col1, col2))
}

fn parse_line(bytes: &[u8]) -> Option<(i32, i32)> {
    if bytes.len() < 11 {
        return None;
    }

    // First 5 bytes for the first number (digits)
    let num1 = (bytes[0] - b'0') as i32 * 10000
        + (bytes[1] - b'0') as i32 * 1000
        + (bytes[2] - b'0') as i32 * 100
        + (bytes[3] - b'0') as i32 * 10
        + (bytes[4] - b'0') as i32;

    // Skip 1 space
    // Next 5 bytes for the second number (digits)
    let num2 = (bytes[6] - b'0') as i32 * 10000
        + (bytes[7] - b'0') as i32 * 1000
        + (bytes[8] - b'0') as i32 * 100
        + (bytes[9] - b'0') as i32 * 10
        + (bytes[10] - b'0') as i32;

    Some((num1, num2))
}

fn compute_distance(v1: &[i32], v2: &[i32]) -> i32 {
    v1.par_iter().zip(v2).map(|(a, b)| (a - b).abs()).sum()
}
