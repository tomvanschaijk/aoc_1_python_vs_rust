use anyhow::{Context, Result};
use futures::StreamExt;
use std::{path::Path, time::Instant};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
};

#[tokio::main]
async fn main() -> Result<()> {
    let files = [
        "./data/input_1k.txt",
        "./data/input_10k.txt",
        "./data/input_100k.txt",
        "./data/input_1m.txt",
    ];

    for file in files.iter() {
        println!("Processing {}...", file);

        let now = Instant::now();

        match get_sorted_vectors(file).await {
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

async fn get_sorted_vectors(file_path: &str) -> Result<(Vec<i32>, Vec<i32>)> {
    let lines = read_lines(file_path)
        .await
        .with_context(|| format!("Failed to read lines from file: {}", file_path))?;

    let (mut col1, mut col2) = tokio_stream::wrappers::LinesStream::new(lines)
        .filter_map(|line| async move {
            match line {
                Ok(line_str) => {
                    let mut nums = line_str.split_whitespace().map(|x| x.parse::<i32>());
                    match (nums.next(), nums.next()) {
                        (Some(Ok(value1)), Some(Ok(value2))) => Some((value1, value2)),
                        _ => None,
                    }
                }
                Err(_) => None,
            }
        })
        .fold(
            (Vec::new(), Vec::new()),
            |(mut col1, mut col2), (v1, v2)| async move {
                col1.push(v1);
                col2.push(v2);
                (col1, col2)
            },
        )
        .await;

    col1.sort_unstable();
    col2.sort_unstable();
    Ok((col1, col2))
}

fn compute_distance(v1: &[i32], v2: &[i32]) -> i32 {
    v1.iter().zip(v2).map(|(a, b)| (a - b).abs()).sum()
}

async fn read_lines<P>(filename: P) -> Result<tokio::io::Lines<BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename).await.context("Failed to open file")?;
    Ok(BufReader::new(file).lines())
}
