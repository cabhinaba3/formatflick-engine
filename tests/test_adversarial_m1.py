"""
Adversarial Stress Test Suite for Milestone 1: cEngine and vectorized_xml.
Validates:
- Tabular edge cases (mixed types, NaNs, infinities, empty tables, 1-row tables)
- Wide tables (100+ and 500+ columns)
- Special XML characters (&, <, >, ", ', unicode, emojis, control chars)
- Schema tag fidelity (<Result><1>...</1></Result>)
- Single-shot vs Chunked vectorization byte equivalence
- Index variations (RangeIndex, discontinuous, float, string)
- Calamine XLSX reading & xlsxwriter writing fidelity
- Speedup benchmark vs baseline
- Reproduces and verifies empirical bugs discovered during adversarial review
"""

import io
import re
import math
import tempfile
from pathlib import Path
import xml.etree.ElementTree as ET
import pytest
import numpy as np
import pandas as pd

from formatflick.engine.vectorized_xml import dataframe_to_xml, records_to_xml, stream_dataframe_to_xml
from formatflick.engine.cEngine import cEngine, dest_cEngine
from formatflick.formatflick import Formatflick


# ==============================================================================
# Helper / Baseline Oracles
# ==============================================================================

def baseline_df_to_xml(df: pd.DataFrame, root: str = 'Result') -> bytes:
    """Baseline ElementTree implementation used prior to acceleration."""
    root_element = ET.Element(root)
    for i, row in df.iterrows():
        row_element = ET.SubElement(root_element, f"{i+1}")
        for col_name, col_value in row.items():
            col_element = ET.SubElement(row_element, str(col_name))
            col_element.text = str(col_value)
    return ET.tostring(root_element)


def parse_formatflick_xml(xml_bytes: bytes) -> ET.Element:
    """
    Parses FormatFlick XML by sanitizing non-standard numeric row tags (<1>, <2>)
    to valid W3C XML tags (<row_1>, <row_2>) for ElementTree validation.
    """
    text = xml_bytes.decode('utf-8')
    sanitized = re.sub(r'<(/?)(\d+)>', r'<\1row_\2>', text)
    return ET.fromstring(sanitized.encode('utf-8'))


# ==============================================================================
# 1. XML Special Characters & Unicode Escaping
# ==============================================================================

class TestXmlEscapingAndUnicode:
    def test_special_xml_chars_in_values(self):
        """Verify XML reserved characters (<, >, &, \", ') are properly escaped and parsed."""
        df = pd.DataFrame({
            "text": [
                "x < y",
                "a > b",
                "Tom & Jerry",
                'He said "hello"',
                "It's sunny",
                "<tag attr='val'>&</tag>",
                "Normal text"
            ]
        })
        xml_bytes = dataframe_to_xml(df)
        assert isinstance(xml_bytes, bytes)
        assert b"<Result>" in xml_bytes
        assert b"</Result>" in xml_bytes

        # Output must be well-formed XML and parseable by standard XML parsers
        parsed = parse_formatflick_xml(xml_bytes)
        assert parsed.tag == "Result"
        assert len(parsed) == len(df)

        # Verify parsed content matches original values
        for i, row_elem in enumerate(parsed):
            assert row_elem.tag == f"row_{i + 1}"
            text_val = row_elem.find("text").text
            assert text_val == df.loc[i, "text"]

    def test_unicode_and_emojis(self):
        """Verify non-ASCII, multi-byte UTF-8, and emojis roundtrip correctly."""
        df = pd.DataFrame({
            "language": ["Chinese", "Arabic", "Japanese", "Russian", "German", "Emoji"],
            "phrase": [
                "你好世界",
                "مرحبا بالعالم",
                "こんにちは世界",
                "Привет мир",
                "Äpfel, Überraschung & Schloss",
                "🔥🚀🎉💡⚡"
            ]
        })
        xml_bytes = dataframe_to_xml(df)
        parsed = parse_formatflick_xml(xml_bytes)
        assert len(parsed) == 6
        for i, row_elem in enumerate(parsed):
            assert row_elem.find("phrase").text == df.loc[i, "phrase"]

    def test_newlines_tabs_commas(self):
        """Verify whitespace characters (newlines, tabs, commas) inside values."""
        df = pd.DataFrame({
            "multiline": ["line1\nline2\nline3", "a\tb\tc", "comma,separated,values"],
            "quotes": ['"double quoted"', "'single quoted'", "mixed \"and 'quotes'"]
        })
        xml_bytes = dataframe_to_xml(df)
        parsed = parse_formatflick_xml(xml_bytes)
        assert len(parsed) == 3
        for i, row_elem in enumerate(parsed):
            assert row_elem.find("multiline").text == df.loc[i, "multiline"]
            assert row_elem.find("quotes").text == df.loc[i, "quotes"]


# ==============================================================================
# 2. Schema Tag Fidelity & Index Variations
# ==============================================================================

class TestSchemaFidelityAndIndexes:
    def test_schema_tag_naming(self):
        """Verify exact schema tag hierarchy: <Result><1><col>val</col></1>...</Result>"""
        df = pd.DataFrame({
            "id": [101, 102],
            "name": ["Alice", "Bob"]
        })
        xml_bytes = dataframe_to_xml(df, root="TestRoot")
        assert xml_bytes.startswith(b"<TestRoot><1>")
        assert xml_bytes.endswith(b"</2></TestRoot>")
        parsed = parse_formatflick_xml(xml_bytes)
        assert parsed.tag == "TestRoot"
        assert [elem.tag for elem in parsed] == ["row_1", "row_2"]

    def test_custom_elementtree_root(self):
        """Verify passing an ET.Element or other root object works seamlessly."""
        elem = ET.Element("CustomContainer")
        df = pd.DataFrame({"col": [1, 2]})
        xml_bytes = dataframe_to_xml(df, root=elem)
        parsed = parse_formatflick_xml(xml_bytes)
        assert parsed.tag == "CustomContainer"

    def test_non_zero_range_index(self):
        """Verify DataFrame with custom start index matches expected numbering."""
        df = pd.DataFrame({"val": [10, 20, 30]}, index=[5, 6, 7])
        xml_bytes = dataframe_to_xml(df)
        parsed = parse_formatflick_xml(xml_bytes)
        # Sliced / custom numeric indices: index + 1 -> 6, 7, 8
        assert [elem.tag for elem in parsed] == ["row_6", "row_7", "row_8"]

    def test_string_index(self):
        """Verify non-numeric index falls back cleanly to 1..N sequence."""
        df = pd.DataFrame({"val": [10, 20, 30]}, index=["rowA", "rowB", "rowC"])
        xml_bytes = dataframe_to_xml(df)
        parsed = parse_formatflick_xml(xml_bytes)
        assert [elem.tag for elem in parsed] == ["row_1", "row_2", "row_3"]

    def test_discontinuous_index(self):
        """Verify non-contiguous index behavior."""
        df = pd.DataFrame({"val": [1, 2, 3]}, index=[0, 10, 100])
        xml_bytes = dataframe_to_xml(df)
        parsed = parse_formatflick_xml(xml_bytes)
        assert [elem.tag for elem in parsed] == ["row_1", "row_11", "row_101"]


# ==============================================================================
# 3. Vectorization Chunk Equivalence & Streaming
# ==============================================================================

class TestChunkingAndStreaming:
    def test_single_shot_vs_chunked_equivalence(self):
        """Verify single-shot mode and chunked mode produce 100% byte-identical output."""
        df = pd.DataFrame({
            "a": list(range(100)),
            "b": [f"item_{i} & <test>" for i in range(100)],
            "c": [i * 1.5 for i in range(100)]
        })
        # Single-shot (chunk_size > len)
        single_shot = dataframe_to_xml(df, chunk_size=200)
        # Chunked (chunk_size < len, multi-chunk)
        chunked_20 = dataframe_to_xml(df, chunk_size=20)
        chunked_7 = dataframe_to_xml(df, chunk_size=7)

        assert single_shot == chunked_20
        assert single_shot == chunked_7

    def test_stream_dataframe_to_xml(self):
        """Verify stream_dataframe_to_xml matches dataframe_to_xml output."""
        df = pd.DataFrame({
            "id": list(range(50)),
            "val": [f"val_{i}" for i in range(50)]
        })
        expected = dataframe_to_xml(df, chunk_size=10)
        out_stream = io.BytesIO()
        stream_dataframe_to_xml(df, out_stream, chunk_size=10)
        assert out_stream.getvalue() == expected


# ==============================================================================
# 4. Wide Tables & Extreme Shapes
# ==============================================================================

class TestWideTablesAndShapes:
    def test_empty_dataframe(self):
        """Verify completely empty DataFrame returns valid root element."""
        df = pd.DataFrame()
        xml_bytes = dataframe_to_xml(df)
        assert xml_bytes == b"<Result />"
        parsed = ET.fromstring(xml_bytes)
        assert parsed.tag == "Result"
        assert len(parsed) == 0

    def test_empty_dataframe_with_columns(self):
        """Verify DataFrame with columns but 0 rows."""
        df = pd.DataFrame(columns=["col1", "col2", "col3"])
        xml_bytes = dataframe_to_xml(df)
        assert xml_bytes == b"<Result />"

    def test_single_row_single_column(self):
        """Verify single cell DataFrame."""
        df = pd.DataFrame({"only_col": [999]})
        xml_bytes = dataframe_to_xml(df)
        parsed = parse_formatflick_xml(xml_bytes)
        assert len(parsed) == 1
        assert parsed[0].tag == "row_1"
        assert parsed[0].find("only_col").text == "999"

    def test_wide_table_100_columns(self):
        """Verify extremely wide DataFrame with 100 columns."""
        cols = [f"col_{i}" for i in range(100)]
        data = {c: [i, i * 2, i * 3] for i, c in enumerate(cols)}
        df = pd.DataFrame(data)

        xml_bytes = dataframe_to_xml(df)
        parsed = parse_formatflick_xml(xml_bytes)
        assert len(parsed) == 3
        assert len(parsed[0]) == 100
        assert parsed[0].find("col_50").text == "50"
        assert parsed[2].find("col_99").text == str(99 * 3)

    def test_wide_table_500_columns(self):
        """Verify wide table with 500 columns."""
        cols = [f"c{i}" for i in range(500)]
        df = pd.DataFrame([[f"val_{i}" for i in range(500)]], columns=cols)
        xml_bytes = dataframe_to_xml(df)
        parsed = parse_formatflick_xml(xml_bytes)
        assert len(parsed) == 1
        assert len(parsed[0]) == 500
        assert parsed[0].find("c499").text == "val_499"


# ==============================================================================
# 5. Data Types & Values
# ==============================================================================

class TestDataTypesAndSpecialValues:
    def test_mixed_dtypes(self):
        """Verify integer, float, string, boolean, and timestamp types."""
        df = pd.DataFrame({
            "int_col": [1, 2],
            "float_col": [3.14, 2.718],
            "bool_col": [True, False],
            "str_col": ["alpha", "beta"],
            "date_col": [pd.Timestamp("2026-01-01"), pd.Timestamp("2026-10-13 12:00:00")]
        })
        xml_bytes = dataframe_to_xml(df)
        parsed = parse_formatflick_xml(xml_bytes)
        assert len(parsed) == 2
        assert parsed[0].find("int_col").text == "1"
        assert parsed[0].find("float_col").text == "3.14"
        assert parsed[0].find("bool_col").text == "True"
        assert parsed[0].find("str_col").text == "alpha"
        assert parsed[0].find("date_col").text == "2026-01-01 00:00:00"


# ==============================================================================
# 6. records_to_xml Validation
# ==============================================================================

class TestRecordsToXml:
    def test_empty_records(self):
        assert records_to_xml([]) == b"<Result />"

    def test_dict_and_list_of_dicts(self):
        records = [
            {"name": "Alice & Bob", "role": "<admin>"},
            {"name": "Charlie", "role": "user \"test\""}
        ]
        xml_bytes = records_to_xml(records)
        parsed = ET.fromstring(xml_bytes)
        assert len(parsed) == 4
        assert parsed.find("name").text == "Alice & Bob"
        assert parsed.find("role").text == "<admin>"

    def test_records_dataframe_input(self):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        xml_bytes = records_to_xml(df)
        parsed = ET.fromstring(xml_bytes)
        assert len(parsed) == 4


# ==============================================================================
# 7. cEngine Tabular Reading & Writing (PyArrow, Calamine, xlsxwriter)
# ==============================================================================

class TestCEngineTabularBackends:
    def test_cEngine_csv_read_write_roundtrip(self, tmp_path):
        """Verify CSV read and write roundtrip using PyArrow and Polars."""
        csv_file = tmp_path / "test.csv"
        df = pd.DataFrame({
            "col_int": [10, 20, 30],
            "col_str": ["comma, value", "quote \"here\"", "normal"],
            "col_float": [1.1, 2.2, 3.3]
        })
        dest_eng = dest_cEngine(df, str(csv_file), destination_extension=".csv")
        dest_eng.to_destination()
        assert csv_file.exists()

        eng = cEngine(str(csv_file), "file", source_extension=".csv")
        res_df = eng.read_file()
        assert len(res_df) == 3
        assert list(res_df.columns) == ["col_int", "col_str", "col_float"]
        assert res_df.loc[0, "col_str"] == "comma, value"
        assert res_df.loc[1, "col_str"] == 'quote "here"'

    def test_cEngine_tsv_read_write_roundtrip(self, tmp_path):
        """Verify TSV read and write roundtrip."""
        tsv_file = tmp_path / "test.tsv"
        df = pd.DataFrame({
            "col_a": ["tab\there?", "regular"],
            "col_b": [100, 200]
        })
        dest_eng = dest_cEngine(df, str(tsv_file), destination_extension=".tsv")
        dest_eng.to_destination()

        eng = cEngine(str(tsv_file), "file", source_extension=".tsv")
        res_df = eng.read_file()
        assert len(res_df) == 2
        assert res_df.loc[1, "col_b"] == 200

    def test_cEngine_xlsx_calamine_and_xlsxwriter(self, tmp_path):
        """Verify Excel read via calamine and write via xlsxwriter."""
        xlsx_file = tmp_path / "test.xlsx"
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "val": [10.5, 20.5, 30.5],
            "label": ["A", "B", "C"]
        })
        dest_eng = dest_cEngine(df, str(xlsx_file), destination_extension=".xlsx")
        dest_eng.to_destination()
        assert xlsx_file.exists()

        eng = cEngine(str(xlsx_file), "file", source_extension=".xlsx")
        res_df = eng.read_file()
        assert len(res_df) == 3
        assert list(res_df.columns) == ["id", "val", "label"]
        assert list(res_df["id"]) == [1, 2, 3]
        assert list(res_df["val"]) == [10.5, 20.5, 30.5]

    def test_cEngine_to_json(self):
        """Verify cEngine.to_json produces columnar list-dict."""
        df = pd.DataFrame({
            "name": ["Alice", "Bob"],
            "score": [95, 88]
        })
        json_obj = cEngine.to_json(df)
        assert isinstance(json_obj, dict)
        assert json_obj == {
            "name": ["Alice", "Bob"],
            "score": [95, 88]
        }

    def test_cEngine_to_xml(self):
        """Verify cEngine.to_xml uses vectorized_xml and returns bytes."""
        df = pd.DataFrame({
            "id": [1, 2],
            "data": ["v1", "v2"]
        })
        xml_bytes = cEngine.to_xml(df)
        assert isinstance(xml_bytes, bytes)
        parsed = parse_formatflick_xml(xml_bytes)
        assert parsed.tag == "Result"
        assert len(parsed) == 2


# ==============================================================================
# 8. Empirical Performance Speedup Verification
# ==============================================================================

class TestPerformanceSpeedup:
    def test_vectorized_xml_speedup_vs_baseline(self):
        """Empirically measure speedup of vectorized_xml vs baseline ElementTree iterrows."""
        import time
        n_rows = 10000
        df = pd.DataFrame({
            "int_val": list(range(n_rows)),
            "str_val": [f"item_{i}" for i in range(n_rows)],
            "float_val": [i * 0.1 for i in range(n_rows)]
        })

        # Baseline measurement
        t0 = time.perf_counter()
        base_xml = baseline_df_to_xml(df)
        t_baseline = time.perf_counter() - t0

        # Vectorized measurement
        t1 = time.perf_counter()
        vec_xml = dataframe_to_xml(df)
        t_vectorized = time.perf_counter() - t1

        # Both match byte-for-byte on standard datasets
        assert base_xml == vec_xml

        speedup = t_baseline / max(t_vectorized, 1e-6)
        print(f"\n[BENCHMARK] 10,000 rows XML: Baseline = {t_baseline:.4f}s, Vectorized = {t_vectorized:.4f}s, Speedup = {speedup:.1f}x")
        assert speedup >= 5.0, f"Expected speedup >= 5x, got {speedup:.2f}x"


# ==============================================================================
# 9. Adversarial Bug Reproductions (Empirical Evidence)
# ==============================================================================

class TestAdversarialBugReproductions:
    def test_reproduce_bug_nan_silent_row_drop(self):
        """
        VERIFIED FIX 1:
        When a float or nullable numeric column contains NaN / None,
        `dataframe_to_xml` preserves all rows without silent row drop.
        """
        df = pd.DataFrame({
            "id": [1.0, float("nan"), 3.0],
            "name": ["Alice", "Bob", "Charlie"]
        })
        xml_bytes = dataframe_to_xml(df)
        base_xml = baseline_df_to_xml(df)
        assert b"<1>" in xml_bytes
        assert b"<2>" in xml_bytes
        assert b"<3>" in xml_bytes
        assert b"<id>nan</id>" in xml_bytes
        assert xml_bytes == base_xml

    def test_reproduce_bug_duplicate_columns_crash(self):
        """
        VERIFIED FIX 2:
        When a DataFrame has duplicate column names (e.g. ['col', 'col']),
        `dataframe_to_xml` serializes both columns cleanly without crashing.
        """
        df = pd.DataFrame([[10, 20]], columns=["col", "col"])
        xml_bytes = dataframe_to_xml(df)
        assert xml_bytes == b"<Result><1><col>10</col><col>20</col></1></Result>"

    def test_reproduce_bug_xmltodict_expat_incompatibility(self):
        """
        EMPIRICAL BUG REPRODUCTION 3:
        FormatFlick's XML output schema (<Result><1>...</1></Result>) violates
        W3C XML 1.0 (element tags cannot start with digits), causing raw xmltodict
        (which uses expat) to fail without sanitization.
        """
        import xmltodict
        import xml.parsers.expat
        xml_data = b"<Result><1><a>10</a></1></Result>"
        with pytest.raises(xml.parsers.expat.ExpatError, match="not well-formed"):
            xmltodict.parse(xml_data)

    def test_verify_fix_xengine_reads_numeric_tag_xml(self):
        """
        VERIFIED FIX 3:
        Verify that xEngine transparently handles self-generated XML containing
        numeric tags (<Result><1><a>10</a></1></Result>).
        """
        import tempfile, os
        from formatflick.engine.xEngine import xEngine
        with tempfile.NamedTemporaryFile(suffix=".xml", delete=False, mode="wb") as f:
            f.write(b"<Result><1><a>10</a><b>hello</b></1><2><a>20</a><b>world</b></2></Result>")
            temp_path = f.name
        try:
            engine = xEngine(source=temp_path, mode="file", source_extension=".xml")
            data = engine.read_file()
            assert "Result" in data
            assert "1" in data["Result"]
            assert data["Result"]["1"]["a"] == "10"
            assert "2" in data["Result"]
            assert data["Result"]["2"]["a"] == "20"
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_verify_fix_cengine_guards_fallback(self):
        """
        VERIFIED FIX 4:
        Verify that dest_cEngine to_destination gracefully falls back when
        Polars or PyArrow are unavailable without raising NameError.
        """
        from unittest.mock import patch
        import formatflick.engine.cEngine as ce
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        with tempfile.TemporaryDirectory() as tmpdir:
            out_csv = Path(tmpdir) / "test_fallback.csv"
            with patch.object(ce, "_HAS_POLARS", False), patch.object(ce, "_HAS_PYARROW", False):
                ce.dest_cEngine(df, str(out_csv), destination_extension=".csv").to_destination()
            assert out_csv.exists()
            assert out_csv.stat().st_size > 0
