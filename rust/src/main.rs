use anyhow::Result;
use polars::{
    frame::DataFrame,
    io::SerReader,
    prelude::{CsvParseOptions, CsvReadOptions},
};

use std::time::Instant;

fn load_data(file_path: &str) -> Result<DataFrame> {
    let df = CsvReadOptions::default()
        .with_has_header(false)
        .with_parse_options(CsvParseOptions {
            separator: b' ',
            ..Default::default()
        })
        .try_into_reader_with_file_path(Some(file_path.into()))
        .unwrap()
        .finish()
        .unwrap();

    Ok(df)
}

fn compute_distance(df: &DataFrame) -> Result<i64> {
    // Define the range for 5-digit numbers
    const MIN_VAL: i64 = 10_000;
    const MAX_VAL: i64 = 99_999;
    const RANGE: usize = (MAX_VAL - MIN_VAL + 1) as usize;

    // Initialize buckets
    let mut buckets1 = vec![0i64; RANGE];
    let mut buckets2 = vec![0i64; RANGE];

    // Get the first and second columns
    let col1 = df.column("column_1")?.i64()?.into_no_null_iter();
    let col2 = df.column("column_2")?.i64()?.into_no_null_iter();

    // Populate buckets
    for num in col1 {
        buckets1[(num - MIN_VAL) as usize] += 1;
    }
    for num in col2 {
        buckets2[(num - MIN_VAL) as usize] += 1;
    }

    // Process buckets
    let mut total_distance = 0;
    let mut j = 0;
    (0..RANGE).for_each(|i| {
        while buckets1[i] > 0 {
            loop {
                if !(j < RANGE && buckets2[j] == 0) {
                    break;
                }
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
            Ok(df) => {
                let distance = compute_distance(&df).unwrap();
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
