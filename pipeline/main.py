"""
Main Pipeline Orchestration

Entry point for running the complete data pipeline.
"""

import logging
from pathlib import Path
from pipeline.io_utils import stream_jsonl
from pipeline.filters import filter_record
from pipeline.cleaners import mask_pii
import tiktoken
import gzip
import json
import tempfile
from tqdm import tqdm

def process_file(
        input_path: str | Path, 
        output_path: str | Path, 
        *, 
        tokenizer = tiktoken.get_encoding("cl100k_base")
        ) -> bool:
    '''
    Process a single input file: filter records and mask PII, writing to output.
    Args:
        input_path (str | Path): Path to the input .jsonl or .jsonl.gz file.
        output_path (str | Path): Path to the output .jsonl.gz file.
        tokenizer: Tokenizer instance for filtering records based on token count.
    
    Returns:
        bool: True if processing succeeded, False otherwise.
    '''
    num_kept = 0
    num_skipped = 0
    
    input_path = Path(input_path)
    output_path = Path(output_path)
    temp_output_path = tempfile.NamedTemporaryFile(delete=False, dir = output_path.parent, suffix=".jsonl.gz")

    try:
        if input_path and not input_path.exists():
            raise FileNotFoundError(f"Input file {input_path} does not exist")

        if output_path.suffixes[-2:] != [".jsonl", ".gz"]:
            raise ValueError("Output file must have .jsonl.gz extension")

        with gzip.open(temp_output_path.name, "wt", encoding="utf-8") as out_f:
            for record in stream_jsonl(input_path):
                if filter_record(record, tokenizer):
                    num_skipped += 1
                else:
                    text = record.get("text", "")
                    record["text"] = mask_pii(text)

                    out_f.write(json.dumps(record) + "\n")

                    num_kept += 1

        temp_output_path.replace(output_path) # atomic rename
        
        logging.info(f"Processing complete. Kept: {num_kept}, Skipped: {num_skipped}")
        return True

    except Exception as e:
        if temp_output_path and temp_output_path.exists():
            temp_output_path.unlink()

        logging.error(f"Error processing file {input_path}: {e}")
        return False

def process_all(
        input_dir: str | Path = "data/raw", 
        output_dir: str | Path = "data/processed"
        ) -> None:

    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    successful = 0
    total = 0

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory {input_dir} does not exist")
    
    output_dir.mkdir(parents=True, exist_ok=True)

    for input_file in tqdm(list(input_dir.glob("*.jsonl*")), desc="Processing files"):
        output_file = output_dir / (input_file.stem + ".jsonl.gz")
        if process_file(input_file, output_file):
            successful += 1
        total += 1

    logging.info(f"Processing complete. Successful: {successful}, Total: {total}")

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    process_all()

if __name__ == "__main__":
    main()