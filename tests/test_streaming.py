"""
Unit and Integration Tests for FormatFlick Chunked Streaming Engine (Milestone 2).
Tests:
- Adaptive threshold routing (< 50MB vs >= 50MB vs force_streaming)
- All 20 pairwise format conversions in streaming mode (FILE_MODE and NFILE_MODE)
- SanitizingStream boundary handling for numeric XML tags
- Constant bounded memory during large streaming conversions (<50MB RSS delta)
"""
import os
import io
import json
import tempfile
import psutil
import pytest
import pandas as pd
import lxml.etree as ET

from formatflick.formatflick import Formatflick, formatflick, STREAMING_THRESHOLD_BYTES
from formatflick.global_var import CSV, TSV, XLSX, JSON, XML
from formatflick.engine.streaming import (
    SanitizingStream,
    stream_convert,
    stream_read_memory_efficient,
    get_stream_reader,
    get_stream_writer
)

ALL_FORMATS = [CSV, TSV, XLSX, JSON, XML]


@pytest.fixture
def sample_data_sources(tmp_path):
    """Creates small sample fixtures for all 5 formats."""
    src_csv = str(tmp_path / "sample.csv")
    pd.DataFrame({"id": [1, 2, 3], "val": ["x", "y", "z"]}).to_csv(src_csv, index=False)

    src_tsv = str(tmp_path / "sample.tsv")
    pd.DataFrame({"id": [1, 2, 3], "val": ["x", "y", "z"]}).to_csv(src_tsv, sep="\t", index=False)

    src_xlsx = str(tmp_path / "sample.xlsx")
    pd.DataFrame({"id": [1, 2, 3], "val": ["x", "y", "z"]}).to_excel(src_xlsx, index=False)

    src_json = str(tmp_path / "sample.json")
    with open(src_json, "w") as f:
        json.dump([{"id": 1, "val": "x"}, {"id": 2, "val": "y"}, {"id": 3, "val": "z"}], f)

    src_xml = str(tmp_path / "sample.xml")
    with open(src_xml, "w") as f:
        f.write("<Result><1><id>1</id><val>x</val></1><2><id>2</id><val>y</val></2><3><id>3</id><val>z</val></3></Result>")

    return {
        CSV: src_csv,
        TSV: src_tsv,
        XLSX: src_xlsx,
        JSON: src_json,
        XML: src_xml
    }


def test_adaptive_threshold_routing(sample_data_sources, tmp_path):
    """Verifies that should_stream() routes adaptively based on file size and threshold."""
    src = sample_data_sources[CSV]
    dst = str(tmp_path / "out.tsv")

    # Default: small file, force_streaming=False -> should_stream is False
    ff_small = Formatflick(source=src, destination=dst)
    assert not ff_small.should_stream()

    # Explicit low threshold: should_stream is True
    ff_low_thresh = Formatflick(source=src, destination=dst, streaming_threshold_bytes=10)
    assert ff_low_thresh.should_stream()

    # Explicit force_streaming: should_stream is True
    ff_forced = Formatflick(source=src, destination=dst, force_streaming=True)
    assert ff_forced.should_stream()


@pytest.mark.parametrize("src_ext", ALL_FORMATS)
@pytest.mark.parametrize("dst_ext", ALL_FORMATS)
def test_all_20_pairwise_streaming_file_mode(sample_data_sources, tmp_path, src_ext, dst_ext):
    """Verifies all 20 pairwise streaming conversions in FILE_MODE."""
    if src_ext == dst_ext:
        pytest.skip("Identical formats not part of pairwise map")

    src = sample_data_sources[src_ext]
    dst = str(tmp_path / f"out_{src_ext[1:]}_to_{dst_ext[1:]}{dst_ext}")

    converter = Formatflick(source=src, destination=dst, force_streaming=True, chunk_size=2)
    res = converter.convert()

    assert os.path.exists(dst), f"Destination file not created: {dst}"
    assert os.path.getsize(dst) > 0, f"Destination file is empty: {dst}"

    # Verify readable according to destination type
    if dst_ext == CSV:
        df = pd.read_csv(dst)
        assert len(df) == 3
    elif dst_ext == TSV:
        df = pd.read_csv(dst, sep="\t")
        assert len(df) == 3
    elif dst_ext == XLSX:
        df = pd.read_excel(dst)
        assert len(df) == 3
    elif dst_ext == JSON:
        with open(dst, "r") as f:
            data = json.load(f)
        assert isinstance(data, list)
        assert len(data) == 3
    elif dst_ext == XML:
        with open(dst, "rb") as f:
            content = f.read()
        assert b"<Result" in content
        assert b"</Result>" in content


@pytest.mark.parametrize("src_ext", ALL_FORMATS)
@pytest.mark.parametrize("dst_ext", ALL_FORMATS)
def test_all_20_pairwise_streaming_nfile_mode(sample_data_sources, src_ext, dst_ext):
    """Verifies all 20 pairwise streaming conversions in NFILE_MODE."""
    if src_ext == dst_ext:
        pytest.skip("Identical formats not part of pairwise map")

    src = sample_data_sources[src_ext]

    converter = formatflick(source=src, destination_extension=dst_ext, mode="nfile", force_streaming=True, chunk_size=2)
    res = converter.convert()

    assert res is not None
    if dst_ext in [CSV, TSV, XLSX]:
        assert isinstance(res, pd.DataFrame)
        assert len(res) == 3
    elif dst_ext == JSON:
        assert isinstance(res, (dict, list))
        assert len(res) == 3
    elif dst_ext == XML:
        assert isinstance(res, (bytes, bytearray, str))
        assert b"<Result" in res


def test_sanitizing_stream_boundary_handling():
    """Verifies SanitizingStream normalizes numeric tags even when tags are split across chunk reads."""
    xml_data = b"<Result><1><val>first</val></1><2><val>second</val></2></Result>"
    raw_stream = io.BytesIO(xml_data)

    # Use very small chunk_size to force tag boundary splits
    sanitized_stream = SanitizingStream(raw_stream, chunk_size=7)
    chunks = []
    while True:
        chunk = sanitized_stream.read(7)
        if not chunk:
            break
        chunks.append(chunk)

    full_output = b"".join(chunks)
    assert b"__ff_num__1" in full_output
    assert b"__ff_num__2" in full_output
    assert b"<1>" not in full_output


def test_streaming_bounded_memory_rss():
    """Verifies RSS delta remains strictly bounded (<50MB) during large streaming conversion."""
    p = psutil.Process()
    base_rss = p.memory_info().rss / (1024 * 1024)

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        f.write("id,account,amount\n")
        for i in range(100000):
            f.write(f"{i},acc_{i},{i*1.25}\n")
        src = f.name
    dst = tempfile.mktemp(suffix=".tsv")

    try:
        stream_convert(src, dst, CSV, TSV, chunk_size=10000)
        peak_rss = p.memory_info().rss / (1024 * 1024)
        delta_rss = peak_rss - base_rss
        assert delta_rss < 50, f"Streaming RSS delta {delta_rss:.2f} MB exceeded 50MB bound!"
        assert os.path.exists(dst)
        assert os.path.getsize(dst) > 0
    finally:
        if os.path.exists(src):
            os.remove(src)
        if os.path.exists(dst):
            os.remove(dst)
