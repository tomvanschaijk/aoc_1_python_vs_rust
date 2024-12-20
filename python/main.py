import time

def get_sorted_vectors(file_path):
    col1 = []
    col2 = []
    try:
        with open(file_path) as f:
            for line in f:
                value1, value2 = map(int, line.split())
                col1.append(value1)
                col2.append(value2)
    except ValueError as e:
        raise ValueError(f"Error parsing the file: {e}")
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {e}")
    
    return sorted(col1), sorted(col2)

def compute_distance(v1, v2):
    return sum(abs(e1 - e2) for e1, e2 in zip(v1, v2))


start_time = time.time()

v1, v2 = get_sorted_vectors("./data/input_1k.txt")
distance = compute_distance(v1, v2)

end_time = time.time()
elapsed_time_ms = (end_time - start_time) * 1000

print(f"The answer is: {distance}, completed in {elapsed_time_ms:.0f}ms")