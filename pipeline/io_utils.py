"""
I/O Utilities

Functions for downloading data and reading/writing files.
"""

from pathlib import Path
import hashlib
import requests
import logging
import tempfile

logger = logging.getLogger(__name__)

def download_dataset(url: str, dest: Path, timeout: int = 20) -> Path:  # Added type hint for url
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