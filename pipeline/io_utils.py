"""
I/O Utilities

Functions for downloading data and reading/writing files.
"""

from pathlib import Path
import hashlib
import requests
import logging
import tempfile
from typing import Iterator
from typing import Iterator, Callable, Any, Literal
import json
import gzip

logger = logging.getLogger(__name__)

def download_dataset(url: str, dest: str | Path, timeout: int = 20) -> Path:  # Added type hint for url
    """
    Download a dataset from a given URL to the specified destination path.

    Args:
        url (str): The URL of the dataset to download.
        dest (Path): The destination path where the dataset will be saved.
        timeout (int): Request timeout in seconds (default: 20).

    Returns:
        Path: The path to the downloaded dataset.
        
    Raises:
        requests.RequestException: If download fails due to network issues.
        IOError: If file validation fails.
    """
    
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and dest.stat().st_size > 0:
        logger.info(f"File already exists at {dest}. Skipping download.")
        return dest
    
    logger.info(f"Downloading dataset from {url} to {dest}")
    temp_path = None  # Initialize to avoid UnboundLocalError

    try:
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()

            total = int(r.headers.get('content-length', 0))

            # Use a temporary file to avoid incomplete downloads
            with tempfile.NamedTemporaryFile("wb", delete=False, dir=dest.parent) as tmp:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        tmp.write(chunk)
                
                temp_path = Path(tmp.name)
        
        # Validate file size if content-length was provided
        if total and temp_path.stat().st_size < 0.9 * total:
            raise IOError(f"Downloaded file size ({temp_path.stat().st_size}) is much smaller than expected ({total})")
        
        temp_path.replace(dest)  # atomic rename

        logger.info(f"Successfully downloaded {url.split('/')[-1]} to {dest}")
        return dest
    
    except (requests.RequestException, IOError) as e:
        logger.error(f"Failed to download {url}. Error: {e}")
        if temp_path and temp_path.exists():
            temp_path.unlink()  # Clean up temp file
        raise

def stream_jsonl(
        path: str | Path,
        *,
        encoding: str = "utf-8",
        decode_errors: Literal["strict", "ignore", "replace"] = "strict",
        on_bad_json: Literal["raise", "skip", "log"] = "raise",
        on_bad_decode: Literal["raise", "skip", "log"] = "skip",
        validator: Callable[[dict], bool] | None = None,
        strip_bom: bool = True,
        ) -> Iterator[dict]:
    """
    Stream records from a .jsonl or .jsonl.gz file, one JSON object per line.

    Args:
      path: File path to read (.jsonl or .jsonl.gz).
      encoding: Text encoding used to decode bytes (default 'utf-8').
      decode_errors: What to do if the bytes don't decode cleanly under the specified encoding.
        ('strict' raises an exception, 'ignore' skips bad chars, 'replace' uses U+FFFD).
      on_bad_json: Policy when a line is not valid JSON ('raise' | 'skip' | 'log').
      on_bad_decode: Policy when a line can't be decoded ('raise' | 'skip' | 'log').
      validator: Optional predicate(record) -> bool; records failing are skipped (logged).
      strip_bom: If True, strips UTF-8 Byte Order Mark (BOM) on the first non-empty line.

    Yields:
      dict objects parsed from each valid JSON line.

    Notes:
      - Empty/whitespace-only lines are skipped.
      - Uses text mode streaming (no full-file loads).
      - Line numbers are included in log messages for quick triage.
    """

    p = Path(path)

    # Choose opener based on file extension
    opener = gzip.open if (p.suffix == ".gz") else open

    # Use text mode ('rt') so json.loads gets str, not bytes
    with opener(p, mode="rt", encoding=encoding, errors=decode_errors) as f:
        saw_content = False

        for line_num, raw in enumerate(f, start=1):
            if not raw.strip():
                continue  # skip empty/whitespace-only lines

            # Optionally strip BOM on first non-empty line
            if strip_bom and not saw_content:
                if raw.startswith("\ufeff"):
                    raw = raw.lstrip("\ufeff")
            
            saw_content = True

            if on_bad_decode != "skip" and "\ufffd" in raw:
                msg = f"{p}:{line_num} - Decode error (replacement char found)"
                if on_bad_decode == "raise":
                    raise UnicodeDecodeError(encoding, b"", 0, 0, msg)
                elif on_bad_decode == "log":
                    logger.warning(msg)
            
            try:
                rec = json.loads(raw)
            
            except json.JSONDecodeError as e:
                if on_bad_json == "raise":
                    raise
                elif on_bad_json == "log":
                    logger.warning(f"{p}:{line_num} - Bad JSON at: {e}")
                continue
            
            if validator is not None:
                try:
                    ok = bool(validator(rec))
                
                except Exception as e:
                    logger.warning(f"{p}:{line_num} - Validator raised exception: {e}")
                    ok = False
                
                if not ok:
                    logger.warning(f"{p}:{line_num} - Record failed validation: {rec}")
                    continue

            if not isinstance(rec, dict):
                logger.warning(f"{p}:{line_num} - Expected JSON object (dict), got {type(rec)}")
                continue

            yield rec