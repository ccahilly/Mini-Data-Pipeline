"""
Main Pipeline Orchestration

Entry point for running the complete data pipeline.
"""

import logging
from pathlib import Path
from pipeline.io_utils import stream_jsonl, download_dataset
from pipeline.filters import filter_record
from pipeline.cleaners import mask_pii
import tiktoken
import gzip
import json
import tempfile
from tqdm import tqdm
from pipeline.loaders import load_dolly15k

def process_file(
        input_path: str | Path, 
        output_path: str | Path, 
        loader: callable = load_dolly15k,
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

    if not input_path.exists():
        raise FileNotFoundError(f"Input file {input_path} does not exist")

    if output_path.suffixes[-2:] != [".jsonl", ".gz"]:
        raise ValueError("Output file must have .jsonl.gz extension")

    # Create temp file in same directory as output for atomic rename
    with tempfile.NamedTemporaryFile(
        mode='w',
        delete=False,
        dir=output_path.parent,
        suffix=".jsonl.gz"
    ) as temp_file:
        temp_output_path = Path(temp_file.name)

    try:
        with gzip.open(temp_output_path, "wt", encoding="utf-8") as out_f:
            for idx, record in enumerate(stream_jsonl(input_path), start=1):
                text = loader(record) # Extract/load
                text = mask_pii(text) # Clean
                
                if filter_record(text, tokenizer):
                    num_skipped += 1
                else:
                    out_f.write(json.dumps({"text": text}) + "\n")
                    num_kept += 1

        # Atomic rename
        temp_output_path.rename(output_path)

        logging.info(f"Processing complete. Kept: {num_kept}, Skipped: {num_skipped}")
        return True

    except Exception as e:
        # Clean up temp file on error
        if temp_output_path.exists():
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

def download_all():
    manifest_path = Path("manifests/datasets.json")
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    for dataset in tqdm(manifest.get("datasets", [])):
        url = dataset.get("url", "No URL found")
        dest = dataset.get("filename", "unknown_file")
        dest = Path("data/raw") / dest
        download_dataset(url, dest)

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    download_all()
    process_all()

if __name__ == "__main__":
    main()