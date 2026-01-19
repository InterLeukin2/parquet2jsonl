"""
Example usage of the parquet-to-jsonl converter
"""

from src.converter import convert_parquet_to_jsonl
import tempfile
import pandas as pd


def create_sample_parquet():
    """Create a sample parquet file for demonstration"""
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'city': ['New York', 'London', 'Paris', 'Tokyo', 'Sydney'],
        'active': [True, False, True, True, False]
    }
    
    df = pd.DataFrame(data)
    
    # Save as parquet
    parquet_path = "sample_data.parquet"
    df.to_parquet(parquet_path)
    print(f"Created sample parquet file: {parquet_path}")
    
    return parquet_path


def main():
    # Create a sample parquet file
    sample_file = create_sample_parquet()
    
    # Convert to JSONL
    output_file = "sample_data.jsonl"
    convert_parquet_to_jsonl(sample_file, output_file)
    
    # Display first few lines of the JSONL file
    print("\nFirst few lines of the JSONL output:")
    with open(output_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 3:  # Just show first 3 lines
                break
            print(f"Line {i+1}: {line.strip()}")
    
    print(f"\nComplete! Converted {sample_file} to {output_file}")


if __name__ == "__main__":
    main()