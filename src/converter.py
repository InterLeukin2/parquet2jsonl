import pandas as pd
import json
import argparse
import sys
import os
from pathlib import Path


def convert_parquet_to_jsonl(parquet_file_path, jsonl_file_path, batch_size=None):
    """
    Convert a parquet file to JSONL format
    
    Args:
        parquet_file_path (str): Path to the input parquet file
        jsonl_file_path (str): Path to the output JSONL file
        batch_size (int, optional): Process in batches to manage memory usage
    """
    parquet_file_path = Path(parquet_file_path)
    jsonl_file_path = Path(jsonl_file_path)
    
    if not parquet_file_path.exists():
        raise FileNotFoundError(f"Parquet file does not exist: {parquet_file_path}")
    
    print(f"Reading parquet file: {parquet_file_path}")
    print(f"Output JSONL file: {jsonl_file_path}")
    
    if batch_size is None:
        # Process the entire file at once
        df = pd.read_parquet(parquet_file_path)
        
        with open(jsonl_file_path, 'w', encoding='utf-8') as f:
            for record in df.to_dict(orient='records'):
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
                
        print(f"Converted {len(df)} records to JSONL format")
    else:
        # Process in batches to manage memory usage
        total_rows = pd.read_parquet(parquet_file_path, columns=[]).shape[0]
        rows_processed = 0
        
        with open(jsonl_file_path, 'w', encoding='utf-8') as f:
            while rows_processed < total_rows:
                df_chunk = pd.read_parquet(
                    parquet_file_path,
                    columns=None,
                    skip_rows=rows_processed,
                    nrows=batch_size
                )
                
                for record in df_chunk.to_dict(orient='records'):
                    f.write(json.dumps(record, ensure_ascii=False) + '\n')
                    
                rows_processed += len(df_chunk)
                print(f"Processed {min(rows_processed, total_rows)}/{total_rows} rows")
    
    print(f"Successfully converted {parquet_file_path} to {jsonl_file_path}")


def main():
    parser = argparse.ArgumentParser(description="Convert Parquet files to JSONL format")
    parser.add_argument("input", help="Input parquet file path")
    parser.add_argument("-o", "--output", help="Output JSONL file path")
    parser.add_argument("--batch-size", type=int, default=None,
                        help="Process in batches to manage memory (default: load entire file)")
    
    args = parser.parse_args()
    
    # Generate output filename if not provided
    if args.output is None:
        input_path = Path(args.input)
        if input_path.suffix != '.parquet':
            print("Warning: Input file doesn't have .parquet extension", file=sys.stderr)
        args.output = str(input_path.with_suffix('.jsonl'))
    
    try:
        convert_parquet_to_jsonl(args.input, args.output, args.batch_size)
    except Exception as e:
        print(f"Error during conversion: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()