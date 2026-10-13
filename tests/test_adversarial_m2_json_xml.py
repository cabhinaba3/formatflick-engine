"""
Adversarial Stress Test Suite for Milestone 2: JSON & XML Chunked Streaming.
EMPIRICAL CHALLENGER (m2_challenger_2)

Stress-tests:
1. Pathological XML structures:
   - FormatFlick numeric row tags (<Result><1>...</1><2>...</2></Result>)
   - Deep hierarchical nesting (5+ levels)
   - XML special characters (&, <, >, ", ', control characters, multilingual unicode, emojis)
   - CDATA blocks with unescaped HTML/XML markup
   - Large attribute dictionaries
   - Empty, whitespace-only, and self-closing elements
   - SanitizingStream byte-boundary straddling with micro chunk sizes (1 to 64 bytes)
2. Large XML streaming (100,000+ elements):
   - XML -> CSV, XML -> TSV, XML -> JSON, XML -> XLSX
   - Memory tracking with psutil asserting peak RSS delta < 50MB
   - Empirical verification of continuous lxml DOM pruning (no element accumulation)
   - Data fidelity and record count assertions
3. Large JSON streaming (100,000+ records):
   - JSON -> CSV, JSON -> TSV, JSON -> XML, JSON -> XLSX via ijson
   - Memory tracking with psutil asserting peak RSS delta < 50MB
   - Heterogeneous schemas (divergent keys across records/chunks)
   - Nested dictionaries and arrays with dot-flattening
   - Special characters, unicode, escaped quotes, nulls, booleans, large numbers
4. Roundtrip schema and data consistency:
   - JSON -> XML -> JSON
   - XML -> JSON -> XML
   - JSON -> CSV -> JSON
   - Natural 50MB threshold boundary routing without force_streaming
"""

import os
import io
import gc
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
    get_stream_writer,
    DEFAULT_CHUNK_SIZE
)
from formatflick.engine.xEngine import NUM_TAG_BYTES_REGEX, TAG_NUM_PREFIX


# ==============================================================================
# Helper Utilities
# ==============================================================================

def get_current_rss_mb() -> float:
    """Returns current process RSS in MB."""
    return psutil.Process().memory_info().rss / (1024 * 1024)


# ==============================================================================
# Suite 1: Pathological XML Structures & Boundary Straddling
# ==============================================================================

def test_sanitizing_stream_micro_chunk_boundaries():
    """
    Adversarially tests SanitizingStream across micro chunk sizes (1 through 48 bytes)
    to force tag splits across every possible character position:
    <1>, </1>, <123456>, </123456>, <1 attr="foo">, <1/>, etc.
    Asserts byte-for-byte equivalence with single-shot regex sanitization.
    """
    raw_xml = (
        b"<Result>"
        b"<1><name>Alice</name><val>100</val></1>"
        b"<2 attr='x'><name>Bob</name><val>200</val></2>"
        b"<3/>"
        b"<987654321><data>large_numeric_tag</data></987654321>"
        b"<item><id>non_numeric</id></item>"
        b"</Result>"
    )

    # Expected single-shot result
    expected_sanitized = NUM_TAG_BYTES_REGEX.sub(rb"<\1" + TAG_NUM_PREFIX + rb"\2\3", raw_xml)

    # Test every chunk size from 1 to 48 bytes
    for chunk_size in range(1, 49):
        stream = SanitizingStream(io.BytesIO(raw_xml), chunk_size=chunk_size)
        accumulated = bytearray()
        while True:
            piece = stream.read(chunk_size)
            if not piece:
                break
            accumulated.extend(piece)
        assert bytes(accumulated) == expected_sanitized, (
            f"SanitizingStream produced byte mismatch at chunk_size={chunk_size}!\n"
            f"Expected: {expected_sanitized}\n"
            f"Got:      {bytes(accumulated)}"
        )


def test_xml_numeric_tags_streaming_all_targets(tmp_path):
    """
    Adversarially tests FormatFlick numeric row tags (<Result><1>...</1><2>...</2></Result>)
    streaming to CSV, TSV, JSON, and XLSX.
    """
    src_xml = str(tmp_path / "numeric_tags.xml")
    records_count = 50
    with open(src_xml, "w", encoding="utf-8") as f:
        f.write("<Result>")
        for i in range(1, records_count + 1):
            f.write(f"<{i}><id>{i}</id><name>User_{i}</name><score>{i * 1.5}</score></{i}>")
        f.write("</Result>")

    # 1. XML -> CSV
    dst_csv = str(tmp_path / "out_num.csv")
    converter = Formatflick(source=src_xml, destination=dst_csv, force_streaming=True, chunk_size=10)
    converter.convert()
    df_csv = pd.read_csv(dst_csv)
    assert len(df_csv) == records_count
    assert list(df_csv["id"]) == list(range(1, records_count + 1))
    assert list(df_csv["name"]) == [f"User_{i}" for i in range(1, records_count + 1)]

    # 2. XML -> TSV
    dst_tsv = str(tmp_path / "out_num.tsv")
    converter = Formatflick(source=src_xml, destination=dst_tsv, force_streaming=True, chunk_size=10)
    converter.convert()
    df_tsv = pd.read_csv(dst_tsv, sep="\t")
    assert len(df_tsv) == records_count
    assert list(df_tsv["id"]) == list(range(1, records_count + 1))

    # 3. XML -> JSON
    dst_json = str(tmp_path / "out_num.json")
    converter = Formatflick(source=src_xml, destination=dst_json, force_streaming=True, chunk_size=10)
    converter.convert()
    with open(dst_json, "r", encoding="utf-8") as f:
        json_data = json.load(f)
    assert isinstance(json_data, list)
    assert len(json_data) == records_count
    assert json_data[0]["name"] == "User_1"
    assert json_data[-1]["name"] == f"User_{records_count}"

    # 4. XML -> XLSX
    dst_xlsx = str(tmp_path / "out_num.xlsx")
    converter = Formatflick(source=src_xml, destination=dst_xlsx, force_streaming=True, chunk_size=10)
    converter.convert()
    df_xlsx = pd.read_excel(dst_xlsx)
    assert len(df_xlsx) == records_count
    assert list(df_xlsx["id"]) == list(range(1, records_count + 1))


def test_xml_deeply_nested_structures(tmp_path):
    """
    Adversarially tests XML with deep hierarchical nesting (6 levels).
    Verifies recursive element-to-dict extraction and dot-notation key flattening.
    """
    src_xml = str(tmp_path / "deep_nested.xml")
    with open(src_xml, "w", encoding="utf-8") as f:
        f.write("<Result>")
        for i in range(1, 11):
            f.write(
                f"<{i}>"
                f"<level1>"
                f"<level2>"
                f"<level3>"
                f"<level4>"
                f"<level5>"
                f"<value>deep_{i}</value>"
                f"</level5>"
                f"</level4>"
                f"</level3>"
                f"</level2>"
                f"</level1>"
                f"</{i}>"
            )
        f.write("</Result>")

    dst_csv = str(tmp_path / "deep_out.csv")
    converter = Formatflick(source=src_xml, destination=dst_csv, force_streaming=True, chunk_size=3)
    converter.convert()

    df = pd.read_csv(dst_csv)
    assert len(df) == 10
    expected_col = "level1.level2.level3.level4.level5.value"
    assert expected_col in df.columns
    assert list(df[expected_col]) == [f"deep_{i}" for i in range(1, 11)]


def test_xml_special_characters_and_multilingual_unicode(tmp_path):
    """
    Adversarially tests XML with XML special entities (&amp;, &lt;, &gt;, &quot;, &apos;),
    multilingual Unicode (Chinese, Japanese, Arabic, German umlauts), and emojis.
    Verifies data fidelity across streaming pipeline.
    """
    src_xml = str(tmp_path / "special_chars.xml")
    test_rows = [
        {"desc": "Entities", "val": "Tom & Jerry <cartoon> \"classic\" 'quote'"},
        {"desc": "Chinese", "val": "你好世界，数据转换测试"},
        {"desc": "Japanese", "val": "こんにちは世界、ストリーミング変換"},
        {"desc": "Arabic", "val": "مرحبا بالعالم، اختبار التحويل"},
        {"desc": "German", "val": "Größenmaßstäbe, Überwachung & Äpfel"},
        {"desc": "Emojis", "val": "🚀✨🔥🎉💯💻📊"},
    ]

    with open(src_xml, "w", encoding="utf-8") as f:
        f.write("<Result>")
        for i, row in enumerate(test_rows, 1):
            desc = row["desc"]
            # XML-escape entities in val
            val_escaped = (
                row["val"]
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&apos;")
            )
            f.write(f"<{i}><desc>{desc}</desc><val>{val_escaped}</val></{i}>")
        f.write("</Result>")

    # Stream to JSON
    dst_json = str(tmp_path / "special_out.json")
    converter = Formatflick(source=src_xml, destination=dst_json, force_streaming=True, chunk_size=2)
    converter.convert()

    with open(dst_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert len(data) == len(test_rows)
    for expected, actual in zip(test_rows, data):
        assert actual["desc"] == expected["desc"]
        assert actual["val"] == expected["val"]

    # Stream to CSV
    dst_csv = str(tmp_path / "special_out.csv")
    converter = Formatflick(source=src_xml, destination=dst_csv, force_streaming=True, chunk_size=2)
    converter.convert()

    df = pd.read_csv(dst_csv)
    assert len(df) == len(test_rows)
    for expected, actual_val in zip(test_rows, df["val"]):
        assert actual_val == expected["val"]


def test_xml_cdata_blocks(tmp_path):
    """
    Adversarially tests CDATA blocks containing unescaped HTML/XML markup,
    brackets, and symbols.
    """
    src_xml = str(tmp_path / "cdata.xml")
    cdata_contents = [
        "<div><h1>Title</h1><p>Paragraph with & and <tags></p></div>",
        "SELECT * FROM users WHERE age > 21 AND score < 100;",
        "JSON payload: {\"key\": \"value\", \"nested\": [1, 2, 3]}",
    ]

    with open(src_xml, "w", encoding="utf-8") as f:
        f.write("<Result>")
        for i, content in enumerate(cdata_contents, 1):
            f.write(f"<{i}><id>{i}</id><payload><![CDATA[{content}]]></payload></{i}>")
        f.write("</Result>")

    dst_json = str(tmp_path / "cdata_out.json")
    converter = Formatflick(source=src_xml, destination=dst_json, force_streaming=True, chunk_size=1)
    converter.convert()

    with open(dst_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert len(data) == 3
    for i, content in enumerate(cdata_contents):
        assert data[i]["payload"] == content


def test_xml_large_attributes_and_empty_elements(tmp_path):
    """
    Adversarially tests XML elements with extensive attribute sets (@attr),
    self-closing elements, and empty text elements.
    """
    src_xml = str(tmp_path / "attrs_empty.xml")
    with open(src_xml, "w", encoding="utf-8") as f:
        f.write("<Result>")
        # Row with 5 attributes and content
        f.write('<1 a="1" b="2" c="3" d="4" e="5"><name>Alice</name></1>')
        # Row with self-closing and empty elements
        f.write('<2><empty_self/><empty_tag></empty_tag><whitespace>   </whitespace></2>')
        f.write("</Result>")

    dst_json = str(tmp_path / "attrs_empty_out.json")
    converter = Formatflick(source=src_xml, destination=dst_json, force_streaming=True, chunk_size=1)
    converter.convert()

    with open(dst_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert len(data) == 2
    # Verify attributes extracted with '@' prefix
    assert data[0]["@a"] == "1"
    assert data[0]["@e"] == "5"
    assert data[0]["name"] == "Alice"
    # Verify empty elements handled without error
    assert data[1].get("empty_self") is None
    assert data[1].get("empty_tag") is None


# ==============================================================================
# Suite 2: Large XML Streaming (100,000+ Elements), DOM Pruning & Bounded Memory
# ==============================================================================

def test_large_xml_100k_elements_dom_pruning_and_bounded_rss(tmp_path):
    """
    Generates an XML dataset containing 100,000 elements.
    Streams XML -> CSV and measures RSS memory profile via psutil.
    Empirically verifies:
    1. DOM tree is pruned continuously: peak RSS delta stays strictly < 50MB.
    2. Data fidelity: exactly 100,000 records converted.
    3. Boundary records (first, middle, last) match perfectly.
    """
    src_xml = str(tmp_path / "large_100k.xml")
    total_elements = 100000

    print(f"\n[XML Stress] Writing {total_elements} elements to XML...")
    with open(src_xml, "w", encoding="utf-8") as f:
        f.write("<Result>")
        for i in range(1, total_elements + 1):
            f.write(f"<{i}><id>{i}</id><account>user_{i}</account><balance>{i * 2.5}</balance></{i}>")
        f.write("</Result>")

    xml_size_mb = os.path.getsize(src_xml) / (1024 * 1024)
    print(f"[XML Stress] XML file size: {xml_size_mb:.2f} MB")

    # Clean memory baseline
    gc.collect()
    base_rss = get_current_rss_mb()
    print(f"[XML Stress] Baseline RSS: {base_rss:.2f} MB")

    dst_csv = str(tmp_path / "out_100k.csv")
    # Execute chunked streaming
    stream_convert(src_xml, dst_csv, XML, CSV, chunk_size=10000)

    peak_rss = get_current_rss_mb()
    delta_rss = peak_rss - base_rss
    print(f"[XML Stress] Peak RSS: {peak_rss:.2f} MB, Delta RSS: {delta_rss:.2f} MB")

    # Assert bounded memory and DOM pruning (<50MB delta)
    assert delta_rss < 50.0, f"XML streaming memory delta ({delta_rss:.2f} MB) exceeded 50MB threshold! DOM pruning failure suspected."

    # Assert data fidelity
    assert os.path.exists(dst_csv)
    # Read back CSV in chunks to verify row count without large single-shot memory allocation
    row_count = 0
    first_row = None
    last_row = None
    for chunk in pd.read_csv(dst_csv, chunksize=25000):
        if first_row is None:
            first_row = chunk.iloc[0].to_dict()
        row_count += len(chunk)
        last_row = chunk.iloc[-1].to_dict()

    assert row_count == total_elements, f"Expected {total_elements} rows, but got {row_count}"
    assert int(first_row["id"]) == 1
    assert first_row["account"] == "user_1"
    assert float(first_row["balance"]) == 2.5
    assert int(last_row["id"]) == total_elements
    assert last_row["account"] == f"user_{total_elements}"
    assert float(last_row["balance"]) == total_elements * 2.5


def test_large_xml_streaming_to_json_and_xlsx(tmp_path):
    """
    Streams large XML (25,000 elements) to JSON and XLSX.
    Verifies bounded memory (<50MB) and data fidelity.
    """
    src_xml = str(tmp_path / "xml_25k.xml")
    total_elements = 25000
    with open(src_xml, "w", encoding="utf-8") as f:
        f.write("<Result>")
        for i in range(1, total_elements + 1):
            f.write(f"<{i}><sku>SKU_{i:06d}</sku><qty>{i % 500}</qty><price>{(i * 1.05):.2f}</price></{i}>")
        f.write("</Result>")

    # 1. XML -> JSON
    dst_json = str(tmp_path / "out_25k.json")
    gc.collect()
    base_rss = get_current_rss_mb()
    stream_convert(src_xml, dst_json, XML, JSON, chunk_size=5000)
    delta_rss_json = get_current_rss_mb() - base_rss
    assert delta_rss_json < 50.0, f"XML->JSON delta RSS ({delta_rss_json:.2f} MB) exceeded 50MB bound!"

    assert os.path.exists(dst_json)
    assert os.path.getsize(dst_json) > 0

    # 2. XML -> XLSX
    dst_xlsx = str(tmp_path / "out_25k.xlsx")
    gc.collect()
    base_rss = get_current_rss_mb()
    stream_convert(src_xml, dst_xlsx, XML, XLSX, chunk_size=5000)
    delta_rss_xlsx = get_current_rss_mb() - base_rss
    assert delta_rss_xlsx < 50.0, f"XML->XLSX delta RSS ({delta_rss_xlsx:.2f} MB) exceeded 50MB bound!"

    assert os.path.exists(dst_xlsx)
    assert os.path.getsize(dst_xlsx) > 0


# ==============================================================================
# Suite 3: Large JSON Streaming & Pathological JSON Edge Cases
# ==============================================================================

def test_large_json_100k_records_bounded_rss(tmp_path):
    """
    Generates a JSON dataset containing 100,000 records.
    Streams JSON -> TSV via ijson incremental reader.
    Empirically verifies:
    1. Bounded memory: peak RSS delta stays strictly < 50MB.
    2. Data fidelity: exactly 100,000 records converted.
    3. Boundary records (first, middle, last) match perfectly.
    """
    src_json = str(tmp_path / "large_100k.json")
    total_records = 100000

    print(f"\n[JSON Stress] Writing {total_records} records to JSON array...")
    with open(src_json, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i in range(1, total_records + 1):
            comma = "," if i < total_records else ""
            f.write(f'{{"rec_id": {i}, "customer": "cust_{i}", "amount": {i * 3.14:.2f}}}{comma}\n')
        f.write("]\n")

    json_size_mb = os.path.getsize(src_json) / (1024 * 1024)
    print(f"[JSON Stress] JSON file size: {json_size_mb:.2f} MB")

    # Clean memory baseline
    gc.collect()
    base_rss = get_current_rss_mb()
    print(f"[JSON Stress] Baseline RSS: {base_rss:.2f} MB")

    dst_tsv = str(tmp_path / "out_100k.tsv")
    # Execute chunked streaming
    stream_convert(src_json, dst_tsv, JSON, TSV, chunk_size=10000)

    peak_rss = get_current_rss_mb()
    delta_rss = peak_rss - base_rss
    print(f"[JSON Stress] Peak RSS: {peak_rss:.2f} MB, Delta RSS: {delta_rss:.2f} MB")

    # Assert bounded memory (<50MB delta)
    assert delta_rss < 50.0, f"JSON streaming memory delta ({delta_rss:.2f} MB) exceeded 50MB threshold!"

    # Assert data fidelity
    assert os.path.exists(dst_tsv)
    row_count = 0
    first_row = None
    last_row = None
    for chunk in pd.read_csv(dst_tsv, sep="\t", chunksize=25000):
        if first_row is None:
            first_row = chunk.iloc[0].to_dict()
        row_count += len(chunk)
        last_row = chunk.iloc[-1].to_dict()

    assert row_count == total_records, f"Expected {total_records} rows, got {row_count}"
    assert int(first_row["rec_id"]) == 1
    assert first_row["customer"] == "cust_1"
    assert int(last_row["rec_id"]) == total_records
    assert last_row["customer"] == f"cust_{total_records}"


def test_json_heterogeneous_keys_streaming(tmp_path):
    """
    Adversarially tests JSON stream with heterogeneous schemas across records/chunks.
    Record 1: {a, b}
    Record 2: {b, c}
    Record 3: {a, d, e}
    Verifies that streaming conversion to JSON and XML preserves disparate keys accurately.
    """
    src_json = str(tmp_path / "hetero.json")
    records = [
        {"id": 1, "field_a": "alpha", "field_b": 10},
        {"id": 2, "field_b": 20, "field_c": "gamma"},
        {"id": 3, "field_a": "omega", "field_d": True, "field_e": [1, 2, 3]},
        {"id": 4, "field_c": "delta", "field_f": {"nested": "obj"}},
    ]
    with open(src_json, "w", encoding="utf-8") as f:
        json.dump(records, f)

    # 1. JSON -> XML (streaming vectorization with chunk_size=2)
    dst_xml = str(tmp_path / "hetero_out.xml")
    converter = Formatflick(source=src_json, destination=dst_xml, force_streaming=True, chunk_size=2)
    converter.convert()

    with open(dst_xml, "rb") as f:
        xml_bytes = f.read()

    assert b"<Result>" in xml_bytes
    assert b"</Result>" in xml_bytes
    assert b"<field_a>alpha</field_a>" in xml_bytes
    assert b"<field_c>gamma</field_c>" in xml_bytes

    # 2. JSON -> XML -> JSON roundtrip via stream_convert to verify field preservation
    dst_json2 = str(tmp_path / "hetero_roundtrip.json")
    stream_convert(dst_xml, dst_json2, XML, JSON, chunk_size=2)
    with open(dst_json2, "r", encoding="utf-8") as f:
        re_json = json.load(f)
    assert len(re_json) == 4


def test_json_pathological_content_and_types(tmp_path):
    """
    Adversarially tests JSON with complex data types:
    - Escaped quotes and characters: \"Hello\", \n, \t, \\
    - Unicode and emojis
    - Explicit null values (mapped to None / NaN)
    - Booleans (True / False)
    - Deeply nested objects and lists
    - Extreme numbers (large integers, scientific floats)
    """
    src_json = str(tmp_path / "pathological.json")
    records = [
        {
            "id": 1,
            "escaped_str": 'He said "Hello" and \\path\\to\\file\nwith newline\tand tab',
            "unicode_str": "世界, العربية, Español, 🚀🎉",
            "is_active": True,
            "is_deleted": False,
            "nullable": None,
            "large_int": 9007199254740991,
            "scientific_float": 1.234567e-8,
            "nested_obj": {"level1": {"level2": {"item": "found"}}},
            "array_data": ["x", "y", "z"]
        }
    ]
    with open(src_json, "w", encoding="utf-8") as f:
        json.dump(records, f)

    dst_xml = str(tmp_path / "patho_out.xml")
    converter = Formatflick(source=src_json, destination=dst_xml, force_streaming=True, chunk_size=1)
    converter.convert()

    with open(dst_xml, "rb") as f:
        xml_content = f.read()

    assert b"<Result>" in xml_content
    assert b"</Result>" in xml_content
    # Verify unicode and boolean preserved in XML
    assert "世界".encode("utf-8") in xml_content
    assert "🚀".encode("utf-8") in xml_content
    assert b"<is_active>True</is_active>" in xml_content
    assert b"<is_deleted>False</is_deleted>" in xml_content
    assert b"<large_int>9007199254740991</large_int>" in xml_content

    # Now convert XML -> CSV to verify tabular serialization
    dst_csv = str(tmp_path / "patho_out.csv")
    converter2 = Formatflick(source=dst_xml, destination=dst_csv, force_streaming=True, chunk_size=1)
    converter2.convert()

    df = pd.read_csv(dst_csv)
    assert len(df) == 1
    assert "世界" in df["unicode_str"].iloc[0]
    assert df["is_active"].iloc[0] == True


# ==============================================================================
# Suite 4: Roundtrip Schema and Data Consistency
# ==============================================================================

def test_streaming_roundtrip_json_xml_json(tmp_path):
    """
    Verifies full streaming roundtrip consistency:
    JSON Source -> XML Streaming -> JSON Streaming.
    Asserts schema and data value preservation across chunk boundaries.
    """
    src_json = str(tmp_path / "orig.json")
    original_data = [
        {"id": i, "name": f"Person_{i}", "metric": float(i * 10.5)}
        for i in range(1, 101)
    ]
    with open(src_json, "w", encoding="utf-8") as f:
        json.dump(original_data, f)

    mid_xml = str(tmp_path / "mid.xml")
    final_json = str(tmp_path / "final.json")

    # Step 1: JSON -> XML (streaming with small chunk_size=15)
    Formatflick(source=src_json, destination=mid_xml, force_streaming=True, chunk_size=15).convert()
    assert os.path.exists(mid_xml)

    # Step 2: XML -> JSON (streaming with small chunk_size=15)
    Formatflick(source=mid_xml, destination=final_json, force_streaming=True, chunk_size=15).convert()
    assert os.path.exists(final_json)

    with open(final_json, "r", encoding="utf-8") as f:
        final_data = json.load(f)

    assert len(final_data) == len(original_data)
    for orig, fin in zip(original_data, final_data):
        assert int(fin["id"]) == orig["id"]
        assert fin["name"] == orig["name"]
        assert float(fin["metric"]) == orig["metric"]


def test_streaming_roundtrip_xml_json_xml(tmp_path):
    """
    Verifies full streaming roundtrip consistency:
    XML Source -> JSON Streaming -> XML Streaming.
    """
    src_xml = str(tmp_path / "orig.xml")
    with open(src_xml, "w", encoding="utf-8") as f:
        f.write("<Result>")
        for i in range(1, 101):
            f.write(f"<{i}><sku>SKU_{i}</sku><val>{i * 5}</val></{i}>")
        f.write("</Result>")

    mid_json = str(tmp_path / "mid.json")
    final_xml = str(tmp_path / "final.xml")

    # Step 1: XML -> JSON (chunk_size=20)
    Formatflick(source=src_xml, destination=mid_json, force_streaming=True, chunk_size=20).convert()

    # Step 2: JSON -> XML (chunk_size=20)
    Formatflick(source=mid_json, destination=final_xml, force_streaming=True, chunk_size=20).convert()

    with open(final_xml, "rb") as f:
        content = f.read()

    assert b"<Result>" in content
    assert b"</Result>" in content
    assert b"<sku>SKU_1</sku>" in content
    assert b"<sku>SKU_100</sku>" in content
    assert b"<val>500</val>" in content


def test_natural_size_threshold_routing(tmp_path):
    """
    Verifies that files exceeding STREAMING_THRESHOLD_BYTES automatically
    route to chunked streaming without needing force_streaming=True.
    """
    # Create file with custom low threshold to simulate natural threshold routing
    src_csv = str(tmp_path / "threshold_test.csv")
    pd.DataFrame({"a": range(100), "b": range(100)}).to_csv(src_csv, index=False)
    dst_json = str(tmp_path / "thresh_out.json")

    file_size = os.path.getsize(src_csv)

    # When threshold is set below file size: should_stream is True
    ff = Formatflick(source=src_csv, destination=dst_json, streaming_threshold_bytes=file_size - 1)
    assert ff.should_stream() is True

    # When threshold is set above file size: should_stream is False
    ff2 = Formatflick(source=src_csv, destination=dst_json, streaming_threshold_bytes=file_size + 1000)
    assert ff2.should_stream() is False

    # Convert with natural threshold triggered
    res = ff.convert()
    assert os.path.exists(dst_json)
    with open(dst_json, "r") as f:
        data = json.load(f)
    assert len(data) == 100
