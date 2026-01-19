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

## Requirements

- Python 3.7+
- pandas
- pyarrow
- jsonlines (optional, for enhanced JSON handling)

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd parquet-to-jsonl-converter
   ```

2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Command Line Interface

```bash
python -m src.converter input.parquet
```

This will create `input.jsonl` in the same directory.

To specify a custom output file:

```bash
python -m src.converter input.parquet -o custom_output.jsonl
```

To process in batches for memory efficiency:

```bash
python -m src.converter input.parquet --batch-size 10000
```

### Python API

```python
from src.converter import convert_parquet_to_jsonl

convert_parquet_to_jsonl('input.parquet', 'output.jsonl')
```

## Options

- `input`: Path to the input parquet file (required)
- `-o, --output`: Path to the output JSONL file (default: input file with .jsonl extension)
- `--batch-size`: Process in batches to manage memory (default: load entire file)

## License

MIT License