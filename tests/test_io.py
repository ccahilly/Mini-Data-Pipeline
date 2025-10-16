"""
IO Tests

Unit tests for the IO components.
"""

import pytest
import json
import gzip
from pathlib import Path
from pipeline.io_utils import download_dataset, stream_jsonl

# download_dataset
def test_download_dataset_basic(tmp_path: Path):
    dest = tmp_path / "data.jsonl"
    url = "https://raw.githubusercontent.com/explosion/projects/refs/heads/v3/tutorials/textcat_docs_issues/assets/gh_issues_raw.jsonl"

    path = download_dataset(url, dest)

    assert path == dest
    assert path.exists()
    assert path.stat().st_size > 0

def write_jsonl(p: Path, rows):
    p.write_text("".join([json.dumps(r) + "\n" for r in rows]), encoding="utf-8")

def write_jsonl_gz(p: Path, rows):
    with gzip.open(p, "wt", encoding="utf-8") as f:
        f.writelines([json.dumps(r) + "\n" for r in rows])

@pytest.mark.parametrize("gz", [False, True])
def test_basic_yields_dicts(tmp_path: Path, gz: bool):
    rows = [{"a": 1}, {"b": 2, "c": 3}]
    p = tmp_path / ("data.jsonl" + (".gz" if gz else ""))
    
    if gz:
        write_jsonl_gz(p, rows)
    else:
        write_jsonl(p, rows)
    
    results = list(stream_jsonl(p))
    assert results == rows

def test_skip_empty_line(tmp_path: Path):
    p = tmp_path / "data.jsonl"
    # Manually write with empty lines
    p.write_text('{"a": 1}\n\n{"b": 2}\n', encoding="utf-8")

    results = list(stream_jsonl(p))
    assert results == [{"a": 1}, {"b": 2}]