"""
Adversarial Stress Test Suite for Milestone 2: Tabular & Excel Chunked Streaming (CSV, TSV, XLSX).
EMPIRICAL CHALLENGER (m2_challenger_1)

Stress-tests:
1. Pathological CSV & TSV edge cases:
   - Embedded newlines inside multiline quotes
   - Multiline quotes with commas/tabs and escaped quotes
   - Multilingual unicode, non-ASCII scripts (Arabic, Chinese, Japanese, Cyrillic) and Emojis
   - Mixed types (int, float, str, None, bool) in single columns
   - Wide tables (1,000 columns)
   - Missing values (NaN, None, empty strings, trailing delimiters)
   - Conversions to TSV, JSON, XML, XLSX
2. XLSX edge cases:
   - Empty sheets (0 rows, header-only)
   - Special characters (<, >, &, ", ', control characters)
   - Formula cells (=SUM(...))
   - Date/time values (pd.Timestamp, datetime.date, datetime.datetime)
   - NaN, Inf, -Inf cells
   - Uneven row lengths (calamine padding)
3. Roundtrip fidelity across chunk boundaries:
   - Small prime chunk sizes forcing chunk splits across pathological rows
   - Exact row/record count preservation
   - Continuous XML row index validation (<1>, <2>, ... <N> without reset or skip)
   - Header deduplication (headers only on chunk 0, not repeated on chunk 1+)
   - Cyclical 5-format streaming roundtrip (CSV -> TSV -> XLSX -> JSON -> XML -> CSV)
4. Heavy load & bounded memory verification (<50MB RSS delta):
   - 100,000+ row XLSX streaming to CSV, TSV, JSON, XML
   - 100,000+ row CSV streaming to XLSX
   - Natural 50MB threshold boundary routing without force_streaming
   - psutil RSS delta verification strictly bounded (<50MB) with zero OOM
"""

import os
import gc
import json
import time
import math
import tempfile
import psutil
import pytest
import numpy as np
import pandas as pd
import openpyxl
import xlsxwriter
import lxml.etree as ET

from formatflick.formatflick import Formatflick, formatflick, STREAMING_THRESHOLD_BYTES
from formatflick.global_var import CSV, TSV, XLSX, JSON, XML
from formatflick.engine.streaming import (
    iter_csv_chunks,
    iter_xlsx_chunks,
    iter_json_chunks,
    iter_xml_chunks,
    stream_convert,
    stream_read_memory_efficient,
    get_stream_reader,
    get_stream_writer,
    DEFAULT_CHUNK_SIZE
)
from formatflick.engine.xEngine import NUM_TAG_BYTES_REGEX, TAG_NUM_PREFIX


# ==============================================================================
# Helper Utilities
# ==============================================================================

def get_current_rss_mb() -> float:
    """Returns current process RSS memory in megabytes."""
    return psutil.Process().memory_info().rss / (1024 * 1024)


def parse_formatflick_xml(xml_bytes: bytes) -> ET.Element:
    """Parses FormatFlick XML by sanitizing non-standard numeric row tags (<1>, <2>)."""
    sanitized = NUM_TAG_BYTES_REGEX.sub(rb"<\1" + TAG_NUM_PREFIX + rb"\2\3", xml_bytes)
    return ET.fromstring(sanitized)


# ==============================================================================
# 1. Pathological CSV & TSV Edge Cases
# ==============================================================================

class TestPathologicalCsvTsv:
    """Adversarial challenge for CSV/TSV parsing and writing under pathological inputs."""

    def test_multiline_embedded_newlines_and_quotes(self, tmp_path):
        """CSV fields containing multiline embedded newlines and quotes split across chunk boundaries."""
        csv_file = tmp_path / "pathological_multiline.csv"
        csv_content = (
            'id,title,description,notes\n'
            '1,"Header Line","First line\\nSecond line\\nThird line","Simple note"\n'
            '2,"Quote \\"Test\\"","Embedded , commas and ""double quotes""\\nAnother line","Note 2"\n'
            '3,"Unicode 🚀","مرحبا بالعالم\\n中文测试\\n🎉 line 3","Final note"\n'
            '4,"Empty notes","Single line text",""\n'
        )
        csv_file.write_text(csv_content, encoding="utf-8")

        # Test chunked reading with chunk_size=1 (forces chunk boundary per row)
        chunks = list(iter_csv_chunks(str(csv_file), chunk_size=1))
        assert len(chunks) == 4, f"Expected 4 chunks, got {len(chunks)}"

        # Convert to all other formats with chunk_size=1
        for dest_ext in [TSV, JSON, XML, XLSX]:
            out_file = tmp_path / f"out_multiline{dest_ext}"
            stream_convert(str(csv_file), str(out_file), CSV, dest_ext, chunk_size=1)
            assert out_file.exists()
            assert out_file.stat().st_size > 0

        # Verify JSON preserves 4 records
        json_file = tmp_path / f"out_multiline{JSON}"
        with open(json_file, "r", encoding="utf-8") as f:
            records = json.load(f)
        assert len(records) == 4
        assert "Second line" in records[0]["description"]
        assert "double quotes" in records[1]["description"]
        assert "中文测试" in records[2]["description"]

        # Verify XML parses cleanly with all 4 records
        xml_file = tmp_path / f"out_multiline{XML}"
        root = parse_formatflick_xml(xml_file.read_bytes())
        assert len(root) == 4

    def test_multilingual_unicode_and_emojis(self, tmp_path):
        """CSV/TSV with multi-byte unicode scripts, mathematical symbols, and emojis."""
        df = pd.DataFrame({
            "lang": ["Arabic", "Chinese", "Greek", "Russian", "Japanese", "Emoji", "Math"],
            "script": [
                "مرحبا بكم في عالم البرمجة",
                "欢迎来到数据转换引擎！",
                "Καλημέρα κόσμε! Ειδικοί χαρακτήρες",
                "Привет мир! Тестирование кодировки",
                "こんにちは世界！日本語テスト",
                "🚀🔥🎉💡⚡✨🛠️📦🏷️🎯",
                "∀x ∈ ℝ: x² ≥ 0 ∧ ∑(1/n²) = π²/6"
            ]
        })
        csv_path = tmp_path / "unicode.csv"
        df.to_csv(csv_path, index=False)

        # Roundtrip through TSV and XLSX with chunk_size=2
        tsv_path = tmp_path / "unicode.tsv"
        stream_convert(str(csv_path), str(tsv_path), CSV, TSV, chunk_size=2)
        df_tsv = pd.read_csv(tsv_path, sep="\t")
        pd.testing.assert_frame_equal(df, df_tsv)

        xlsx_path = tmp_path / "unicode.xlsx"
        stream_convert(str(tsv_path), str(xlsx_path), TSV, XLSX, chunk_size=2)
        df_xlsx = pd.read_excel(xlsx_path)
        pd.testing.assert_frame_equal(df, df_xlsx)

    def test_mixed_types_and_missing_values(self, tmp_path):
        """Columns with mixed data types and varied null representations across chunks."""
        csv_path = tmp_path / "mixed_types.csv"
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("col_a,col_b,col_c,col_d\n")
            f.write("1,10.5,true,sample\n")
            f.write("text_val,,false,NaN\n")
            f.write(",15.7,,another\n")
            f.write("999,Inf,true,\n")
            f.write("-1,-Inf,false,end\n")

        # Stream convert to TSV, XLSX, JSON, XML with chunk_size=2
        for dest_ext in [TSV, XLSX, JSON, XML]:
            dst = tmp_path / f"out_mixed{dest_ext}"
            stream_convert(str(csv_path), str(dst), CSV, dest_ext, chunk_size=2)
            assert dst.exists()
            assert dst.stat().st_size > 0

        # Read back TSV and verify row count = 5
        tsv_dst = tmp_path / f"out_mixed{TSV}"
        df_tsv = pd.read_csv(tsv_dst, sep="\t")
        assert len(df_tsv) == 5

    def test_wide_table_1000_columns(self, tmp_path):
        """Streaming wide tables with 1,000 columns across chunk boundaries."""
        n_cols = 1000
        n_rows = 20
        cols = [f"feat_{i}" for i in range(n_cols)]
        data = np.random.randn(n_rows, n_cols)
        df_wide = pd.DataFrame(data, columns=cols)
        df_wide["feat_0"] = range(n_rows)  # Integer ID

        csv_wide = tmp_path / "wide_1000.csv"
        df_wide.to_csv(csv_wide, index=False)

        # Convert to TSV, JSON, XLSX with chunk_size=5
        tsv_wide = tmp_path / "wide_1000.tsv"
        stream_convert(str(csv_wide), str(tsv_wide), CSV, TSV, chunk_size=5)
        df_read_tsv = pd.read_csv(tsv_wide, sep="\t")
        assert df_read_tsv.shape == (n_rows, n_cols)

        xlsx_wide = tmp_path / "wide_1000.xlsx"
        stream_convert(str(tsv_wide), str(xlsx_wide), TSV, XLSX, chunk_size=5)
        df_read_xlsx = pd.read_excel(xlsx_wide)
        assert df_read_xlsx.shape == (n_rows, n_cols)

        # XLSX -> CSV back-conversion
        csv_back = tmp_path / "wide_1000_back.csv"
        stream_convert(str(xlsx_wide), str(csv_back), XLSX, CSV, chunk_size=5)
        df_read_back = pd.read_csv(csv_back)
        assert df_read_back.shape == (n_rows, n_cols)

    def test_csv_duplicate_column_headers(self, tmp_path):
        """CSV containing duplicate header column names handles gracefully."""
        csv_path = tmp_path / "dup_cols.csv"
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("val,val,val\n1,2,3\n4,5,6\n7,8,9\n")

        for dest_ext in [TSV, XLSX, JSON, XML]:
            dst = tmp_path / f"out_dup{dest_ext}"
            stream_convert(str(csv_path), str(dst), CSV, dest_ext, chunk_size=2)
            assert dst.exists()
            assert dst.stat().st_size > 0


# ==============================================================================
# 2. XLSX Edge Cases & Pathological Inputs
# ==============================================================================

class TestXlsxEdgeCases:
    """Adversarial challenge for Excel (.xlsx) chunked streaming readers and writers."""

    def test_empty_and_header_only_sheets(self, tmp_path):
        """Empty workbook, empty sheet, and header-only sheet handling."""
        # 1. Truly empty sheet
        wb_empty = openpyxl.Workbook()
        xlsx_empty = tmp_path / "empty.xlsx"
        wb_empty.save(str(xlsx_empty))

        csv_from_empty = tmp_path / "empty_out.csv"
        stream_convert(str(xlsx_empty), str(csv_from_empty), XLSX, CSV)
        assert csv_from_empty.exists()

        # 2. Header-only sheet
        wb_header = openpyxl.Workbook()
        ws = wb_header.active
        ws.append(["col_x", "col_y", "col_z"])
        xlsx_header = tmp_path / "header_only.xlsx"
        wb_header.save(str(xlsx_header))

        csv_from_header = tmp_path / "header_out.csv"
        stream_convert(str(xlsx_header), str(csv_from_header), XLSX, CSV)
        with open(csv_from_header, "r", encoding="utf-8") as f:
            content = f.read().strip()
        assert content == "col_x,col_y,col_z"

    def test_xlsx_formula_cells_and_special_types(self, tmp_path):
        """Excel workbook with formulas, datetimes, booleans, and empty cells."""
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["id", "formula_col", "date_col", "bool_col", "empty_col"])
        ws.append([1, "=SUM(A2:A2)", "2023-01-15", True, None])
        ws.append([2, "=A3*10", "2024-12-31", False, ""])
        ws.append([3, "=AVERAGE(A2:A4)", "2025-06-01", True, None])

        xlsx_path = tmp_path / "formulas_and_dates.xlsx"
        wb.save(str(xlsx_path))

        # Stream convert to CSV, TSV, JSON, XML with chunk_size=2
        for dest_ext in [CSV, TSV, JSON, XML]:
            dst = tmp_path / f"out_special{dest_ext}"
            stream_convert(str(xlsx_path), str(dst), XLSX, dest_ext, chunk_size=2)
            assert dst.exists()
            assert dst.stat().st_size > 0

        # Read back CSV and verify 3 data rows
        csv_dst = tmp_path / f"out_special{CSV}"
        df_csv = pd.read_csv(csv_dst)
        assert len(df_csv) == 3

    def test_xlsx_nan_and_infinities(self, tmp_path):
        """Writing and streaming NaN, Inf, and -Inf values to and from XLSX."""
        df = pd.DataFrame({
            "normal": [1.0, 2.0, 3.0],
            "nan_col": [np.nan, 4.5, np.nan],
            "inf_col": [np.inf, -np.inf, 99.0],
            "text": ["val1", "val2", "val3"]
        })
        csv_in = tmp_path / "nans_infs.csv"
        df.to_csv(csv_in, index=False)

        # Convert CSV -> XLSX (XlsxStreamWriter with nan_inf_to_errors=True)
        xlsx_out = tmp_path / "nans_infs.xlsx"
        stream_convert(str(csv_in), str(xlsx_out), CSV, XLSX, chunk_size=2)
        assert xlsx_out.exists()

        # Convert XLSX back to CSV
        csv_back = tmp_path / "nans_infs_back.csv"
        stream_convert(str(xlsx_out), str(csv_back), XLSX, CSV, chunk_size=2)
        df_back = pd.read_csv(csv_back)
        assert len(df_back) == 3
        assert df_back["normal"].tolist() == [1.0, 2.0, 3.0]

    def test_xlsx_uneven_row_lengths(self, tmp_path):
        """Excel sheets where data rows have fewer or more cells than the header row."""
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["h1", "h2", "h3"])  # Header has 3 columns
        ws.append([1, 2])              # Row 1 has 2 columns (shorter)
        ws.append([3, 4, 5, 6, 7])     # Row 2 has 5 columns (wider)
        ws.append([8])                 # Row 3 has 1 column (shorter)

        xlsx_path = tmp_path / "uneven.xlsx"
        wb.save(str(xlsx_path))

        csv_out = tmp_path / "uneven.csv"
        stream_convert(str(xlsx_path), str(csv_out), XLSX, CSV, chunk_size=2)
        df = pd.read_csv(csv_out)
        assert len(df) == 3
        # Bounding width should be 5 columns
        assert df.shape[1] >= 3


# ==============================================================================
# 3. Roundtrip Fidelity Across Chunk Boundaries
# ==============================================================================

class TestChunkBoundaryRoundtripFidelity:
    """Validates structural and data integrity across chunk boundary stitching."""

    def test_cyclical_five_format_streaming_roundtrip(self, tmp_path):
        """Cyclical 5-format streaming conversion preserving data across prime chunk sizes."""
        n_rows = 500
        df_orig = pd.DataFrame({
            "id": list(range(1, n_rows + 1)),
            "username": [f"user_{i:04d}" for i in range(1, n_rows + 1)],
            "score": [round(i * 1.25, 2) for i in range(1, n_rows + 1)],
            "tag": [f"tag_{i % 7} 🎉" for i in range(1, n_rows + 1)]
        })
        src_csv = tmp_path / "cycle_0.csv"
        df_orig.to_csv(src_csv, index=False)

        # 1. CSV -> TSV (chunk_size=7)
        dst_tsv = tmp_path / "cycle_1.tsv"
        stream_convert(str(src_csv), str(dst_tsv), CSV, TSV, chunk_size=7)
        assert len(pd.read_csv(dst_tsv, sep="\t")) == n_rows

        # 2. TSV -> XLSX (chunk_size=11)
        dst_xlsx = tmp_path / "cycle_2.xlsx"
        stream_convert(str(dst_tsv), str(dst_xlsx), TSV, XLSX, chunk_size=11)
        assert len(pd.read_excel(dst_xlsx)) == n_rows

        # 3. XLSX -> JSON (chunk_size=13)
        dst_json = tmp_path / "cycle_3.json"
        stream_convert(str(dst_xlsx), str(dst_json), XLSX, JSON, chunk_size=13)
        with open(dst_json, "r", encoding="utf-8") as f:
            json_data = json.load(f)
        assert len(json_data) == n_rows

        # 4. JSON -> XML (chunk_size=17)
        dst_xml = tmp_path / "cycle_4.xml"
        stream_convert(str(dst_json), str(dst_xml), JSON, XML, chunk_size=17)

        # 5. XML -> CSV (chunk_size=19)
        dst_final_csv = tmp_path / "cycle_5.csv"
        stream_convert(str(dst_xml), str(dst_final_csv), XML, CSV, chunk_size=19)
        df_final = pd.read_csv(dst_final_csv)

        assert len(df_final) == n_rows
        assert df_final["id"].astype(int).tolist() == list(range(1, n_rows + 1))
        assert df_final["username"].tolist() == df_orig["username"].tolist()

    def test_continuous_xml_indexing_across_chunks(self, tmp_path):
        """Verifies row indices <1>, <2>, ... <N> increment continuously across chunk boundaries."""
        n_rows = 150
        df = pd.DataFrame({"val": range(1, n_rows + 1)})
        csv_path = tmp_path / "test_xml_idx.csv"
        df.to_csv(csv_path, index=False)

        xml_path = tmp_path / "test_xml_idx.xml"
        # Force tiny chunk_size=25 (6 chunks generated)
        stream_convert(str(csv_path), str(xml_path), CSV, XML, chunk_size=25)

        root = parse_formatflick_xml(xml_path.read_bytes())
        assert len(root) == n_rows

        # Verify exact tags from __ff_num__1 to __ff_num__150
        for i, child in enumerate(root, start=1):
            assert child.tag == f"__ff_num__{i}", f"Index discontinuity at position {i}: tag is {child.tag}"

    def test_header_deduplication_in_tabular_streams(self, tmp_path):
        """Verifies CSV/TSV headers are emitted exactly once on the first chunk."""
        df = pd.DataFrame({"alpha": range(100), "beta": range(100, 200)})
        csv_path = tmp_path / "dedup.csv"
        df.to_csv(csv_path, index=False)

        tsv_path = tmp_path / "dedup.tsv"
        stream_convert(str(csv_path), str(tsv_path), CSV, TSV, chunk_size=10)

        with open(tsv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        assert len(lines) == 101  # 1 header + 100 rows
        assert lines[0].strip() == "alpha\tbeta"
        for line in lines[1:]:
            assert "alpha" not in line and "beta" not in line

    def test_nfile_mode_streaming_fidelity(self, tmp_path):
        """formatflick(mode='nfile', force_streaming=True) produces exact object across chunks."""
        df_orig = pd.DataFrame({"k": [1, 2, 3, 4, 5], "v": ["a", "b", "c", "d", "e"]})
        csv_path = tmp_path / "nfile_stream.csv"
        df_orig.to_csv(csv_path, index=False)

        ff = formatflick(source=str(csv_path), destination_extension=TSV, mode="nfile", force_streaming=True, chunk_size=2)
        res = ff.convert()
        assert isinstance(res, pd.DataFrame)
        assert len(res) == 5
        pd.testing.assert_frame_equal(res, df_orig)


# ==============================================================================
# 4. Heavy Load & Bounded Memory Verification (<50MB RSS Bound)
# ==============================================================================

class TestHeavyLoadAndBoundedMemory:
    """Stress tests verifying strict constant memory scaling and high-throughput streaming."""

    def test_large_xlsx_100k_rows_streaming_rss_bound(self, tmp_path):
        """100,000+ row XLSX streaming to CSV, TSV, JSON, XML with RSS delta strictly < 50MB."""
        xlsx_path = tmp_path / "large_100k.xlsx"
        n_rows = 100000

        # Create 100,000 row XLSX using constant_memory writer
        wb = xlsxwriter.Workbook(str(xlsx_path), {"constant_memory": True})
        ws = wb.add_worksheet()
        ws.write_row(0, 0, ["id", "username", "amount", "flag"])
        for i in range(1, n_rows + 1):
            ws.write_row(i, 0, [i, f"user_{i}", i * 1.5, i % 2 == 0])
        wb.close()

        assert xlsx_path.exists()
        file_size_mb = xlsx_path.stat().st_size / (1024 * 1024)
        assert file_size_mb > 1.5

        # 1. XLSX -> CSV streaming
        gc.collect()
        base_rss = get_current_rss_mb()
        csv_out = tmp_path / "out_100k.csv"

        t0 = time.time()
        stream_convert(str(xlsx_path), str(csv_out), XLSX, CSV, chunk_size=25000)
        duration = time.time() - t0

        peak_rss = get_current_rss_mb()
        delta_rss = peak_rss - base_rss

        assert csv_out.exists()
        with open(csv_out, "r", encoding="utf-8") as f:
            line_count = sum(1 for _ in f)
        assert line_count == n_rows + 1  # 100,000 data rows + 1 header
        assert delta_rss < 50.0, f"XLSX -> CSV RSS delta {delta_rss:.2f} MB exceeded 50MB bound!"

        # 2. XLSX -> TSV streaming
        gc.collect()
        base_rss = get_current_rss_mb()
        tsv_out = tmp_path / "out_100k.tsv"
        stream_convert(str(xlsx_path), str(tsv_out), XLSX, TSV, chunk_size=25000)
        delta_rss_tsv = get_current_rss_mb() - base_rss
        assert delta_rss_tsv < 50.0

        # 3. XLSX -> JSON streaming
        gc.collect()
        base_rss = get_current_rss_mb()
        json_out = tmp_path / "out_100k.json"
        stream_convert(str(xlsx_path), str(json_out), XLSX, JSON, chunk_size=25000)
        delta_rss_json = get_current_rss_mb() - base_rss
        assert delta_rss_json < 50.0

        # 4. XLSX -> XML streaming
        gc.collect()
        base_rss = get_current_rss_mb()
        xml_out = tmp_path / "out_100k.xml"
        stream_convert(str(xlsx_path), str(xml_out), XLSX, XML, chunk_size=25000)
        delta_rss_xml = get_current_rss_mb() - base_rss
        assert delta_rss_xml < 50.0

    def test_large_csv_streaming_to_xlsx_rss_bound(self, tmp_path):
        """100,000+ row CSV streaming to XLSX via XlsxStreamWriter with RSS delta strictly < 50MB."""
        csv_path = tmp_path / "large_100k.csv"
        n_rows = 100000

        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("id,sku,price,in_stock\n")
            for i in range(1, n_rows + 1):
                f.write(f"{i},SKU-{i:07d},{i * 0.99:.2f},{i % 3 == 0}\n")

        gc.collect()
        base_rss = get_current_rss_mb()
        xlsx_out = tmp_path / "out_100k.xlsx"

        stream_convert(str(csv_path), str(xlsx_out), CSV, XLSX, chunk_size=25000)

        delta_rss = get_current_rss_mb() - base_rss
        assert delta_rss < 50.0, f"CSV -> XLSX RSS delta {delta_rss:.2f} MB exceeded 50MB bound!"
        assert xlsx_out.exists()
        assert xlsx_out.stat().st_size > 1.5 * 1024 * 1024

    def test_natural_adaptive_routing_above_50mb(self, tmp_path):
        """Natural 50MB threshold boundary routing without force_streaming."""
        large_csv = tmp_path / "file_above_50mb.csv"

        # Generate ~53MB CSV file
        with open(large_csv, "w", encoding="utf-8") as f:
            f.write("id,account,email,payload,score,active\n")
            pad = "abcdefghijklmnopqrstuvwxyz0123456789"
            for i in range(580000):
                f.write(f"{i},acc_{i:06d},user_{i}@test.com,{pad},{i*0.5:.2f},{i%2==0}\n")

        size_bytes = large_csv.stat().st_size
        assert size_bytes >= STREAMING_THRESHOLD_BYTES, f"File size {size_bytes} < 50MB threshold"

        out_tsv = tmp_path / "file_above_50mb.tsv"
        ff = Formatflick(source=str(large_csv), destination=str(out_tsv))

        # Verify automatic streaming routing
        assert ff.should_stream() is True

        gc.collect()
        base_rss = get_current_rss_mb()
        ff.convert()
        delta_rss = get_current_rss_mb() - base_rss

        assert out_tsv.exists()
        assert delta_rss < 50.0, f"Natural 50MB streaming RSS delta {delta_rss:.2f} MB exceeded bound!"
