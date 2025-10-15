"""
I/O Utilities

Functions for downloading data and reading/writing files.
"""

from pathlib import Path
import requests
import logging
import hashlib
import tempfile

logger = logging.getLogger(__name__)

def download_dataset(url, dest: Path, timeout: int = 20) -> Path:
    """
    Download a dataset from a given URL to the specified destination path.

    Args:
        url (str): The URL of the dataset to download.
        dest (Path): The destination path where the dataset will be saved.

    Returns:
        Path: The path to the downloaded dataset.
    """
    
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and dest.stat().st_size > 0:
        logger.info(f"File already exists at {dest}. Skipping download.")
        return dest
    
    logger.info(f"Downloading dataset from {url} to {dest}")

    try:
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status() # Go to except block if status code is 4xx or 5xx

            total = int(r.headers.get('content-length', 0)) # total size in bytes based on metadata

            # Use a temporary file to avoid incomplete downloads
            with tempfile.NamedTemporaryFile("wb", delete=False, dir=dest.parent) as tmp:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        tmp.write(chunk)
                
                temp_path = Path(tmp.name)
        
        if total and temp_path.stat().st_size < 0.9 * total:
            raise IOError("Downloaded file size is much smaller than expected size.")
        
        temp_path.replace(dest) # atomic rename

        logger.info(f"Saved {url.split('/')[-1]} to {dest}")

        return dest
    
    except (requests.RequestException, IOError) as e:
        logger.error(f"Failed to download {url}. Error: {e}")
        if 'temp_path' in locals() and temp_path.exists():
            temp_path.unlink()
        raise # Pass the exception up the call stack