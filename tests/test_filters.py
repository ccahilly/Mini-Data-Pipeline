import pytest
from pipeline.filters import filter_record
from tiktoken import get_encoding

tokenizer = get_encoding("cl100k_base")

def test_empty():
    rec = {"text": ""}
    assert filter_record(rec, tokenizer)

def test_non_english():
    rec = {"text": "Oi, bom dia. Tudo bem?"}
    assert filter_record(rec, tokenizer)

def test_too_few_tokens():
    rec = {"text": "Hi"}
    assert filter_record(rec, tokenizer)