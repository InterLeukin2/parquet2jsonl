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
    
    # Create output directory if it doesn't exist
    jsonl_file_path.parent.mkdir(parents=True, exist_ok=True)
    
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


def map_input_to_output(input_path):
    """
    Maps an input file path to an output file path in the output directory.
    If input file is in or under the 'input' directory, output goes to 'output' directory with same relative path.
    """
    input_path = Path(input_path)
    
    # If input is in 'input' directory or subdirectory
    try:
        # Check if the input path contains an 'input' part
        parts = input_path.parts
        if 'input' in parts:
            # Find index of 'input' in the path
            input_idx = parts.index('input')
            if input_idx < len(parts) - 1:  # Make sure there are more parts after 'input'
                # Reconstruct path replacing 'input' with 'output'
                output_parts = list(parts[:input_idx]) + ['output'] + list(parts[input_idx+1:])
                output_path = Path(*output_parts).with_suffix('.jsonl')
                return output_path
    except:
        pass
    
    # Fallback: if input file is directly in 'input' directory
    if input_path.parent.name == 'input':
        output_filename = input_path.stem + '.jsonl'
        return Path('output') / output_filename
    
    # If input is not in input directory, just replace extension
    if input_path.suffix != '.parquet':
        print("Warning: Input file doesn't have .parquet extension", file=sys.stderr)
    return input_path.with_suffix('.jsonl')


def convert_all_parquet_files():
    """Convert all parquet files in the input directory"""
    input_path = Path('input')
    
    if not input_path.exists():
        print(f"Input directory does not exist: {input_path}", file=sys.stderr)
        return
    
    parquet_files = list(input_path.glob('*.parquet'))
    
    if not parquet_files:
        print("No .parquet files found in the input directory.", file=sys.stderr)
        return
    
    print(f"Found {len(parquet_files)} parquet file(s) to convert.")
    
    for parquet_file in parquet_files:
        output_file = map_input_to_output(parquet_file)
        print(f"Converting {parquet_file} -> {output_file}")
        convert_parquet_to_jsonl(parquet_file, output_file)


def main():
    parser = argparse.ArgumentParser(description="Convert Parquet files to JSONL format")
    parser.add_argument("input", nargs='?', default=None, help="Input parquet file path or 'input' directory to convert all files")
    parser.add_argument("-o", "--output", help="Output JSONL file path")
    parser.add_argument("--batch-size", type=int, default=None,
                        help="Process in batches to manage memory (default: load entire file)")
    
    args = parser.parse_args()
    
    # If no input is provided or 'input' is specified, convert all files in input directory
    if args.input is None or args.input == 'input':
        convert_all_parquet_files()
    else:
        # Process single file
        if args.output is None:
            args.output = str(map_input_to_output(args.input))
        
        try:
            convert_parquet_to_jsonl(args.input, args.output, args.batch_size)
        except Exception as e:
            print(f"Error during conversion: {str(e)}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()