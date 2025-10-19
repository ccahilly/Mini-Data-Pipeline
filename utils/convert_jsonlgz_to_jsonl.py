import gzip
import json
from pathlib import Path
import logging

def convert_from_gz(input_path: str | Path) -> bool:
    """Convert a .jsonl.gz file to a .jsonl file by extracting the JSONL content.

    Args:
        input_path: Path to the input .jsonl.gz file.

    Returns:
        True if conversion was successful, False otherwise.
    """

    input_path = Path(input_path)
    print(input_path)


    if (input_path.suffixes[-2:] != [".jsonl", ".gz"]):
        raise ValueError("Input file must have .jsonl.gz extension")
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file {input_path} does not exist")
    
    output_path_name = Path(input_path.name.replace(".jsonl.gz", ".jsonl"))
    output_path = input_path.parent / output_path_name

    try:
        with gzip.open(input_path, "rt", encoding="utf-8") as in_f, open(output_path, "wt", encoding="utf-8") as out_f:

            for line in in_f:
                record = json.loads(line)
                out_f.write(json.dumps(record) + "\n")

        logging.info(f"Successfully converted {input_path} to {output_path}")
        return True

    except Exception as e:
        logging.error(f"Error converting file {input_path}: {e}")
        return False
    
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Convert .jsonl.gz file to .jsonl file.")
    parser.add_argument("input_path", type=str, help="Path to the input .jsonl.gz file.")

    args = parser.parse_args()

    success = convert_from_gz(args.input_path)
    if not success:
        exit(1)