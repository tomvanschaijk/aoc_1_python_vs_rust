use std::fs::File;
use std::io::BufReader;
use std::io::{self, BufRead, Lines, Result};
use std::path::Path;

fn main() {
    let now = std::time::Instant::now();

    let (v1, v2) = get_sorted_vectors("./data/input_1k.txt");
    let distance: i32 = compute_distance(&v1, &v2);

    println!("The answer is: {}, completed in {}ms", distance, now.elapsed().as_millis());
}

fn get_sorted_vectors(file_path: &str) -> (Vec<i32>, Vec<i32>) {
    let mut column1: Vec<i32> = vec![];
    let mut column2: Vec<i32> = vec![];
    match read_lines(file_path) {
        Ok(lines) => {
            for line in lines.map_while(Result::ok) {
                let splitted: Vec<i32> = line
                    .split_whitespace()
                    .map(|x| x.parse::<i32>().unwrap())
                    .collect();
                column1.push(splitted[0]);
                column2.push(splitted[1]);
            }
        }
        Err(_) => panic!("Error reading file"),
    }

    column1.sort();
    column2.sort();

    (column1, column2)
}

fn compute_distance(sorted_v1: &[i32], sorted_v2: &[i32]) -> i32 {
    sorted_v1
        .iter()
        .zip(sorted_v2)
        .map(|(c1, c2)| (c1 - c2).abs())
        .sum()
}

fn read_lines<P>(filename: P) -> Result<Lines<BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}