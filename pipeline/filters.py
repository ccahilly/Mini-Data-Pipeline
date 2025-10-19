"""
Data Filtering Functions

Functions for filtering data based on various criteria.
"""

import logging
from pipeline.cleaners import is_english

logger = logging.getLogger(__name__)

def filter_record(text: str, tokenizer, *, min_tokens = 5, max_tokens = 1024) -> bool:
    '''
    Filter a record based on text language and token count.
    Args:
        text (str): The input text to filter.
        tokenizer: Tokenizer instance for counting tokens.
        min_tokens (int): Minimum acceptable token count.
        max_tokens (int): Maximum acceptable token count.
    Returns:
        bool: True if the record should be filtered out, False to keep it.
    '''

    if not text:
        logger.warning("Record text is empty or does not exist: %s", text)
        return True

    if not is_english(text):
        logger.warning("Record text is not English: %s", text)
        return True

    tokens = tokenizer.encode(text)
    token_count = len(tokens)

    if token_count < min_tokens or token_count > max_tokens:
        logger.warning("Record token count %d out of bounds [%d, %d]: %s", token_count, min_tokens, max_tokens, text)
        return True
    
    return False
