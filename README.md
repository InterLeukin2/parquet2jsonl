# Parquet to JSONL Converter

A simple and efficient tool to convert Parquet files to JSONL (JSON Lines) format.

## Overview

This tool converts Parquet files (.parquet) to JSONL format (JSON Lines), where each line contains a valid JSON object. This is particularly useful for:

- Processing large datasets in streaming fashion
- Interfacing with systems that expect JSONL input
- Data pipeline transformations

## Features

- Fast conversion using Pandas and PyArrow
- Memory-efficient batch processing for large files
- Command-line interface for easy automation
- Support for all data types stored in Parquet files
- Automatic input/output directory handling (place files in input/, get results in output/)
- Batch conversion of all files in input directory

## Requirements

- Python 3.7+
- pandas
- pyarrow
- jsonlines (optional, for enhanced JSON handling)

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd parquet2jsonl
   ```

2. Create a virtual environment (recommended):
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Preparing Your Data

Place your Parquet files in the `input` directory:

```bash
# Copy your parquet file(s) to the input directory
cp /path/to/your/files/*.parquet input/
```

### Command Line Interface

#### Convert all files in the input directory:
```bash
python3 -m src.converter input
```

Or simply run without arguments to convert all files:
```bash
python3 -m src.converter
```

Both commands will automatically find all .parquet files in the input directory and convert them to .jsonl files in the output directory, keeping the same filenames.

#### Convert a single file:
```bash
python3 -m src.converter input/your_file.parquet
```

This will create `output/your_file.jsonl`.

To specify a custom output file:

```bash
python3 -m src.converter input/your_file.parquet -o output/custom_output.jsonl
```

To process in batches for memory efficiency:

```bash
python3 -m src.converter input/your_file.parquet --batch-size 10000
```

### Python API

```python
from src.converter import convert_parquet_to_jsonl

convert_parquet_to_jsonl('input/your_file.parquet', 'output/your_file.jsonl')
```

## Options

- `input`: Path to the input parquet file or 'input' directory to convert all files (default: 'input' directory)
- `-o, --output`: Path to the output JSONL file (default: automatically maps input/ to output/ directory)
- `--batch-size`: Process in batches to manage memory (default: load entire file)

## License

MIT License