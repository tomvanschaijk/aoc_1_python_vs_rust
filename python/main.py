import time
import polars as pl
import numpy as np
from numba import jit

def load_data(file_path):
    try:
        df = pl.read_csv(file_path, separator=" ", has_header=False)
        return df[:, 0].to_numpy(), df[:, 1].to_numpy()
    except ValueError as e:
        raise ValueError(f"Error parsing the file: {e}")
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {e}")

# Using Numba's JIT compiler for the bucket sort implementation
@jit(nopython=True, cache=True)
def compute_distance(col1, col2):
    #  Define the range for 5-digit numbers
    MIN_VAL = 10_000
    MAX_VAL = 99_999
    RANGE = MAX_VAL - MIN_VAL + 1
    
    # Initialize buckets
    buckets1 = np.zeros(RANGE, dtype=np.int64)
    buckets2 = np.zeros(RANGE, dtype=np.int64)
    
    # Populate buckets
    for num in col1:
        buckets1[num - MIN_VAL] += 1
    for num in col2:
        buckets2[num - MIN_VAL] += 1
    
    # Process buckets
    total_distance = 0
    j = 0
    for i in range(RANGE):
        while buckets1[i] > 0:
            while j < RANGE and buckets2[j] == 0:
                j += 1
            if j < RANGE:
                actual_num1 = i + MIN_VAL
                actual_num2 = j + MIN_VAL
                total_distance += abs(actual_num1 - actual_num2)
                buckets1[i] -= 1
                buckets2[j] -= 1
    
    return total_distance

def main():
    files = [
        "./data/input_1k.txt",
        "./data/input_10k.txt",
        "./data/input_100k.txt",
        "./data/input_1m.txt",
    ]

    # Warm-up call with small dummy inputs to precompile the Numba function
    dummy_v1 = np.array([10000, 20000, 30000], dtype=np.int64)
    dummy_v2 = np.array([40000, 50000, 60000], dtype=np.int64)
    compute_distance(dummy_v1, dummy_v2)

    for file in files:
        print(f"Processing {file}...")
        
        start_time = time.time()
        
        v1, v2 = load_data(file)
        distance = compute_distance(v1, v2)
        
        end_time = time.time()
        elapsed_time_ms = (end_time - start_time) * 1000
        
        print(f"The answer is: {distance}, completed in {elapsed_time_ms:.0f}ms\n")

if __name__ == "__main__":
    main()