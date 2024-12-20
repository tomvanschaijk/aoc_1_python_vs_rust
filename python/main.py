import time
import polars as pl
import numpy as np
from numba import jit

def get_sorted_vectors(file_path):
    try:
        df = pl.read_csv(file_path, separator=" ", has_header=False)
        col1 = df[:, 0].to_numpy()
        col2 = df[:, 1].to_numpy()
    except ValueError as e:
        raise ValueError(f"Error parsing the file: {e}")
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {e}")

    return np.sort(col1), np.sort(col2)

# Using Numba's JIT compiler to speed up the distance computation
@jit(nopython=True, cache=True)
def compute_distance(v1, v2):
    return np.sum(np.abs(v1 - v2))

def main():
     # Warm-up call with small dummy inputs to precompile the Numba function
    dummy_v1 = np.array([1, 2, 3], dtype=np.int32)
    dummy_v2 = np.array([4, 5, 6], dtype=np.int32)
    compute_distance(dummy_v1, dummy_v2)  # This triggers the compilation
    
    files = [
        "./data/input_1k.txt",
        "./data/input_10k.txt",
        "./data/input_100k.txt",
        "./data/input_1m.txt",
    ]

    for file in files:
        print(f"Processing {file}...")
        start_time = time.time()

        v1, v2 = get_sorted_vectors(file)
        distance = compute_distance(v1, v2)

        end_time = time.time()
        elapsed_time_ms = (end_time - start_time) * 1000

        print(f"The answer is: {distance}, completed in {elapsed_time_ms:.0f}ms\n")

if __name__ == "__main__":
    main()
