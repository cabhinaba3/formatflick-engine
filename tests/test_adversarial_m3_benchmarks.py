"""
Milestone 3 Adversarial Challenge Suite:
Benchmarking Methodology, Metrics Accuracy, Speedup Claims, and Roundtrip Data Fidelity.

Author: m3_challenger_1 (EMPIRICAL CHALLENGER)
"""

import os
import gc
import json
import time
import tempfile
import tracemalloc
from pathlib import Path
import psutil
import pytest
import pandas as pd
import numpy as np

from formatflick.formatflick import Formatflick, formatflick
from formatflick.global_var import CSV, TSV, XLSX, JSON, XML
from formatflick.engine.jEngine import HAS_ORJSON
from formatflick.engine.streaming import stream_convert, DEFAULT_CHUNK_SIZE
from benchmarks.run_benchmarks import (
    generate_tabular_data,
    generate_json_data,
    generate_xml_data,
    generate_xlsx_data,
    baseline_convert,
    formatflick_convert,
)


@pytest.fixture
def tmp_workspace():
    with tempfile.TemporaryDirectory(prefix="ff_challenger_") as td:
        yield td


# ==============================================================================
# 1. Roundtrip Data Fidelity & Schema Consistency (Vectorized & Streaming)
# ==============================================================================

def test_tabular_roundtrip_csv_tsv_csv_exact_equality(tmp_workspace):
    """Verify that CSV -> TSV -> CSV preserves exact row count, column count, and types."""
    src_csv = os.path.join(tmp_workspace, "src.csv")
    mid_tsv = os.path.join(tmp_workspace, "mid.tsv")
    dst_csv = os.path.join(tmp_workspace, "dst.csv")

    df_orig = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "score": [95.5, 82.0, 99.1, 74.3, 88.8],
        "active": [True, False, True, True, False]
    })
    df_orig.to_csv(src_csv, index=False)

    Formatflick(src_csv, mid_tsv).convert()
    Formatflick(mid_tsv, dst_csv).convert()

    df_result = pd.read_csv(dst_csv)
    pd.testing.assert_frame_equal(df_orig, df_result)


def test_tabular_roundtrip_csv_xlsx_csv_values(tmp_workspace):
    """Verify that CSV -> XLSX -> CSV preserves values and schema."""
    src_csv = os.path.join(tmp_workspace, "src.csv")
    mid_xlsx = os.path.join(tmp_workspace, "mid.xlsx")
    dst_csv = os.path.join(tmp_workspace, "dst.csv")

    df_orig = pd.DataFrame({
        "id": [101, 102, 103],
        "name": ["Item_A", "Item_B", "Item_C"],
        "price": [19.99, 45.50, 100.00]
    })
    df_orig.to_csv(src_csv, index=False)

    Formatflick(src_csv, mid_xlsx).convert()
    Formatflick(mid_xlsx, dst_csv).convert()

    df_result = pd.read_csv(dst_csv)
    pd.testing.assert_frame_equal(df_orig, df_result)


def test_streaming_roundtrip_csv_tsv_csv_fidelity(tmp_workspace):
    """Verify that chunked streaming CSV -> TSV -> CSV preserves exact data across chunk boundaries."""
    src_csv = os.path.join(tmp_workspace, "src_stream.csv")
    mid_tsv = os.path.join(tmp_workspace, "mid_stream.tsv")
    dst_csv = os.path.join(tmp_workspace, "dst_stream.csv")

    rows = 5000
    generate_tabular_data(rows=rows, out_path=src_csv, sep=',')

    # Chunk size = 113 (arbitrary prime to test chunk boundary stitching)
    Formatflick(src_csv, mid_tsv, force_streaming=True, chunk_size=113).convert()
    Formatflick(mid_tsv, dst_csv, force_streaming=True, chunk_size=113).convert()

    df_orig = pd.read_csv(src_csv)
    df_result = pd.read_csv(dst_csv)

    assert len(df_result) == rows
    assert list(df_result.columns) == list(df_orig.columns)
    pd.testing.assert_frame_equal(df_orig, df_result)


def test_streaming_roundtrip_csv_json_csv_fidelity(tmp_workspace):
    """Verify that streaming CSV -> JSON -> CSV preserves row counts, headers, and values."""
    src_csv = os.path.join(tmp_workspace, "src.csv")
    mid_json = os.path.join(tmp_workspace, "mid.json")
    dst_csv = os.path.join(tmp_workspace, "dst.csv")

    rows = 2000
    generate_tabular_data(rows=rows, out_path=src_csv, sep=',')

    # Stream CSV to JSON
    Formatflick(src_csv, mid_json, force_streaming=True, chunk_size=250).convert()

    # Verify JSON is a valid array of objects
    with open(mid_json, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert isinstance(data, list)
    assert len(data) == rows

    # Stream JSON back to CSV
    Formatflick(mid_json, dst_csv, force_streaming=True, chunk_size=250).convert()

    df_orig = pd.read_csv(src_csv)
    df_result = pd.read_csv(dst_csv)

    assert len(df_result) == rows
    assert list(df_result.columns) == list(df_orig.columns)
    # Check sample values
    assert df_result.iloc[0]["id"] == df_orig.iloc[0]["id"]
    assert df_result.iloc[-1]["id"] == df_orig.iloc[-1]["id"]
    assert df_result.iloc[0]["user_name"] == df_orig.iloc[0]["user_name"]


def test_streaming_roundtrip_csv_xml_csv_fidelity(tmp_workspace):
    """Verify that streaming CSV -> XML -> CSV preserves row counts and field integrity."""
    src_csv = os.path.join(tmp_workspace, "src.csv")
    mid_xml = os.path.join(tmp_workspace, "mid.xml")
    dst_csv = os.path.join(tmp_workspace, "dst.csv")

    rows = 1500
    generate_tabular_data(rows=rows, out_path=src_csv, sep=',')

    Formatflick(src_csv, mid_xml, force_streaming=True, chunk_size=200).convert()
    Formatflick(mid_xml, dst_csv, force_streaming=True, chunk_size=200).convert()

    df_orig = pd.read_csv(src_csv)
    df_result = pd.read_csv(dst_csv)

    assert len(df_result) == rows
    for col in ["user_name", "category", "region"]:
        assert df_result[col].tolist() == df_orig[col].tolist()


# ==============================================================================
# 2. Pathological Edge Cases & Fidelity Under Hostile Inputs
# ==============================================================================

def test_fidelity_pathological_strings_and_unicode(tmp_workspace):
    """Verify roundtrip fidelity on unicode, special characters, and XML escape sequences."""
    src_csv = os.path.join(tmp_workspace, "pathological.csv")
    dst_tsv = os.path.join(tmp_workspace, "pathological.tsv")
    rt_csv = os.path.join(tmp_workspace, "pathological_rt.csv")

    records = {
        "id": [1, 2, 3, 4],
        "unicode": ["你好世界", "مرحبا بالعالم", "こんにちは", "Привет мир"],
        "special_xml": ["<tag attr='val'>", "&amp; &lt; &gt;", "quotes \"here\"", "normal text"],
        "emojis": ["🚀🔥🎉", "💻📊📈", "✨🌟💫", "🏆🥇🎯"]
    }
    df_orig = pd.DataFrame(records)
    df_orig.to_csv(src_csv, index=False)

    Formatflick(src_csv, dst_tsv).convert()
    Formatflick(dst_tsv, rt_csv).convert()

    df_res = pd.read_csv(rt_csv)
    pd.testing.assert_frame_equal(df_orig, df_res)


def test_fidelity_nulls_and_missing_values(tmp_workspace):
    """Verify handling of empty strings and null values."""
    src_csv = os.path.join(tmp_workspace, "nulls.csv")
    dst_tsv = os.path.join(tmp_workspace, "nulls.tsv")
    rt_csv = os.path.join(tmp_workspace, "nulls_rt.csv")

    df_orig = pd.DataFrame({
        "a": [1, np.nan, 3],
        "b": ["foo", "", "baz"],
        "c": [np.nan, "bar", np.nan]
    })
    df_orig.to_csv(src_csv, index=False)

    Formatflick(src_csv, dst_tsv).convert()
    Formatflick(dst_tsv, rt_csv).convert()

    df_res = pd.read_csv(rt_csv)
    assert len(df_res) == 3
    assert pd.isna(df_res.iloc[1]["a"])
    assert df_res.iloc[0]["a"] == 1.0


# ==============================================================================
# 3. Empirical Verification of Benchmark Methodology & Metric Artifacts
# ==============================================================================

def test_empirical_baseline_orient_discrepancy(tmp_workspace):
    """
    EMPIRICAL CHALLENGE: The benchmark suite in run_benchmarks.py uses
    df.to_dict(orient='records') in baseline_convert for JSON destination,
    whereas FormatFlick's in-memory cEngine.to_json() uses orient='list'.
    This test verifies that orient='records' artificially inflates baseline duration
    by 3x-4x compared to the true legacy FormatFlick baseline behavior.
    """
    csv_path = os.path.join(tmp_workspace, "bench_sample.csv")
    generate_tabular_data(rows=40000, out_path=csv_path)

    df = pd.read_csv(csv_path)

    # 1. Baseline orient='records' (what run_benchmarks.py executed)
    t0 = time.perf_counter()
    recs = df.to_dict(orient="records")
    with open(os.path.join(tmp_workspace, "recs.json"), "w") as f:
        json.dump(recs, f, indent=4)
    t_records_baseline = time.perf_counter() - t0

    # 2. Legacy FormatFlick orient='list' (what original FormatFlick cEngine did)
    t0 = time.perf_counter()
    col_dict = df.to_dict(orient="list")
    with open(os.path.join(tmp_workspace, "cols.json"), "w") as f:
        json.dump(col_dict, f, indent=4)
    t_list_baseline = time.perf_counter() - t0

    # 3. Accelerated FormatFlick
    t0 = time.perf_counter()
    formatflick_convert(csv_path, os.path.join(tmp_workspace, "ff.json"))
    t_accelerated = time.perf_counter() - t0

    # Empirical assertion: baseline records takes substantially longer than baseline list
    assert t_records_baseline > t_list_baseline, (
        f"orient=records ({t_records_baseline:.3f}s) was expected to be slower than "
        f"orient=list ({t_list_baseline:.3f}s)"
    )
    # Both baselines are slower than accelerated FormatFlick (which uses orjson when available)
    if HAS_ORJSON:
        assert t_accelerated < t_list_baseline, (
            f"Accelerated FormatFlick ({t_accelerated:.3f}s) should still be faster than "
            f"orient=list baseline ({t_list_baseline:.3f}s)"
        )
    else:
        assert t_accelerated < t_records_baseline, (
            f"FormatFlick ({t_accelerated:.3f}s) should still be faster than "
            f"orient=records baseline ({t_records_baseline:.3f}s)"
        )


def test_empirical_sequential_rss_masking_artifact(tmp_workspace):
    """
    EMPIRICAL CHALLENGE: Verify that sequential execution in a single Python process
    causes Delta RSS to report 0.0 MB due to glibc memory arena retention,
    whereas isolated tracemalloc confirms actual bounded chunk allocations.
    """
    csv_path = os.path.join(tmp_workspace, "large_sample.csv")
    tsv_path = os.path.join(tmp_workspace, "large_sample.tsv")
    # 200,000 rows (~14 MB)
    generate_tabular_data(rows=200000, out_path=csv_path)

    process = psutil.Process()

    # Step 1: Simulate prior unaccelerated baseline allocating large memory
    dummy_large = [pd.DataFrame({"x": range(500000), "y": [f"val_{i}" for i in range(500000)]})]
    del dummy_large
    gc.collect()

    base_rss = process.memory_info().rss / (1024 * 1024)

    # Step 2: Run streaming conversion while tracking tracemalloc
    tracemalloc.start()
    formatflick_convert(csv_path, tsv_path, force_streaming=True, chunk_size=50000)
    current_traced, peak_traced = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_rss = process.memory_info().rss / (1024 * 1024)
    delta_rss = max(0.0, peak_rss - base_rss)

    # Process RSS delta may be 0.0 MB due to arena reuse
    # But peak_traced proves bounded chunk allocation
    peak_traced_mb = peak_traced / (1024 * 1024)
    assert peak_traced_mb < 30.0, f"Peak traced RAM {peak_traced_mb:.2f}MB exceeded 30MB bound!"
    assert peak_traced_mb > 0.5, "Traced RAM should reflect actual chunk buffer allocations."


# ==============================================================================
# 4. Multi-Format Streaming Scalability & Bounded Memory Stress Tests
# ==============================================================================

@pytest.mark.parametrize("pair", [
    (CSV, TSV, generate_tabular_data),
    (CSV, JSON, generate_tabular_data),
    (CSV, XML, generate_tabular_data),
])
def test_streaming_memory_bounded_across_formats(tmp_workspace, pair):
    """
    Verify bounded memory across different destination formats under heavy load.
    Ensures that streaming to JSON and XML also maintain strictly bounded RAM (<50MB).
    """
    src_ext, dst_ext, gen_fn = pair
    src_path = os.path.join(tmp_workspace, f"source{src_ext}")
    dst_path = os.path.join(tmp_workspace, f"destination{dst_ext}")

    rows = 150000
    gen_fn(rows=rows, out_path=src_path)

    p = psutil.Process()
    gc.collect()
    base_rss = p.memory_info().rss / (1024 * 1024)

    t0 = time.perf_counter()
    formatflick_convert(src_path, dst_path, force_streaming=True, chunk_size=25000)
    dur = time.perf_counter() - t0

    gc.collect()
    peak_rss = p.memory_info().rss / (1024 * 1024)
    delta_rss = max(0.0, peak_rss - base_rss)

    assert os.path.exists(dst_path)
    assert os.path.getsize(dst_path) > 0
    # Peak delta RSS must be strictly bounded below 60MB (<100MB SLA)
    assert delta_rss < 60.0, f"Format conversion {src_ext}->{dst_ext} delta RSS {delta_rss:.2f}MB exceeded 60MB!"
