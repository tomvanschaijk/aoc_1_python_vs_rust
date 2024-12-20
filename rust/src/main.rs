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
        .par_split(|&line| line == b'\n')
        .filter_map(parse_line)
        .unzip();

    col1.par_sort_unstable();
    col2.par_sort_unstable();

    Ok((col1, col2))
}

fn parse_line(bytes: &[u8]) -> Option<(i32, i32)> {
    if bytes.len() < 13 {
        return None;
    }

    unsafe {
        let num1 = std::str::from_utf8_unchecked(&bytes[0..5])
            .parse::<i32>()
            .ok()?;
        let num2 = std::str::from_utf8_unchecked(&bytes[8..13])
            .parse::<i32>()
            .ok()?;
        Some((num1, num2))
    }
}

fn compute_distance(v1: &[i32], v2: &[i32]) -> i32 {
    v1.par_iter().zip(v2).map(|(a, b)| (a - b).abs()).sum()
}
