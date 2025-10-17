"""
Text Cleaning Functions

Functions for cleaning and normalizing text data.
"""

import re
from langdetect import detect
import logging

logger = logging.getLogger(__name__)

def mask_pii(text: str) -> str:
    """
    Mask personally identifiable information (PII) in the text.

    Args:
        text (str): The input text.

    Returns:
        str: The text with emails and IPv4 addresses masked.
    """
    
    text = re.sub(r"[A-Za-z0-9._-]+@[A-Za-z0-9._-]+\.[A-Za-z]{2,}", "|||EMAIL|||", text)
    text = re.sub(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", "|||IP|||", text)

    return text

def is_english(text: str) -> bool:
    """
    Detect if the given text is in English.

    Args:
        text (str): The input text.

    Returns:
        bool: True if the text is detected as English, False otherwise.
    """
    
    try:
        lang = detect(text)
        if lang == "en":
            return True
        else:
            logger.warning(f"Detected '{lang}' not 'en'.")
            return False
    except:
        logger.error("Language detection failed.")
        return False