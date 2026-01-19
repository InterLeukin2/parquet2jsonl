import unittest
import tempfile
import pandas as pd
import json
from pathlib import Path
from src.converter import convert_parquet_to_jsonl


class TestConverter(unittest.TestCase):
    def setUp(self):
        # Create a temporary parquet file for testing
        self.test_data = {
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'city': ['New York', 'London', 'Tokyo']
        }
        self.df = pd.DataFrame(self.test_data)
        
        self.temp_dir = tempfile.mkdtemp()
        self.parquet_path = Path(self.temp_dir) / "test.parquet"
        self.jsonl_path = Path(self.temp_dir) / "test.jsonl"
        
        # Save test DataFrame as parquet
        self.df.to_parquet(self.parquet_path)

    def tearDown(self):
        # Clean up temporary files
        if self.parquet_path.exists():
            self.parquet_path.unlink()
        if self.jsonl_path.exists():
            self.jsonl_path.unlink()

    def test_convert_parquet_to_jsonl(self):
        """Test basic conversion functionality"""
        convert_parquet_to_jsonl(str(self.parquet_path), str(self.jsonl_path))
        
        # Verify output file exists
        self.assertTrue(self.jsonl_path.exists())
        
        # Read and verify content
        with open(self.jsonl_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        self.assertEqual(len(lines), 3)  # 3 rows in our test data
        
        # Check each line is valid JSON
        for i, line in enumerate(lines):
            obj = json.loads(line)
            # Verify the values match our test data
            self.assertEqual(obj['name'], self.test_data['name'][i])
            self.assertEqual(obj['age'], self.test_data['age'][i])
            self.assertEqual(obj['city'], self.test_data['city'][i])

    def test_large_batch_conversion(self):
        """Test conversion with batching"""
        # Add more data to make it worth testing batch processing
        large_df = pd.concat([self.df] * 1000, ignore_index=True)  # 3000 rows
        large_parquet_path = Path(self.temp_dir) / "large_test.parquet"
        large_jsonl_path = Path(self.temp_dir) / "large_test.jsonl"
        
        large_df.to_parquet(large_parquet_path)
        
        convert_parquet_to_jsonl(str(large_parquet_path), str(large_jsonl_path), batch_size=100)
        
        # Verify output file exists
        self.assertTrue(large_jsonl_path.exists())
        
        # Count lines in the output file
        with open(large_jsonl_path, 'r', encoding='utf-8') as f:
            line_count = sum(1 for _ in f)
        
        self.assertEqual(line_count, 3000)  # 3000 rows
        
        # Clean up large files
        large_parquet_path.unlink()
        large_jsonl_path.unlink()


if __name__ == '__main__':
    unittest.main()