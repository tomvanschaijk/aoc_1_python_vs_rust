use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{BufRead, BufReader, Lines},
    path::Path,
    time::Instant,
};

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
    let lines = read_lines(file_path)
        .with_context(|| format!("Failed to read lines from file: {}", file_path))?;

    let (mut col1, mut col2): (Vec<i32>, Vec<i32>) = lines
        .map_while(Result::ok)
        .filter_map(|line| {
            let mut nums = line.split_whitespace().map(|x| x.parse::<i32>());
            match (nums.next(), nums.next()) {
                (Some(Ok(value1)), Some(Ok(value2))) => Some((value1, value2)),
                _ => None,
            }
        })
        .fold(
            (Vec::new(), Vec::new()),
            |(mut col1, mut col2), (v1, v2)| {
                col1.push(v1);
                col2.push(v2);
                (col1, col2)
            },
        );

    col1.sort_unstable();
    col2.sort_unstable();

    Ok((col1, col2))
}

fn compute_distance(v1: &[i32], v2: &[i32]) -> i32 {
    v1.iter().zip(v2).map(|(a, b)| (a - b).abs()).sum()
}

fn read_lines<P>(filename: P) -> Result<Lines<BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(&filename)
        .with_context(|| format!("Failed to open file: {:?}", filename.as_ref()))?;
    Ok(BufReader::new(file).lines())
}
