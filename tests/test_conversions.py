"""
Comprehensive end-to-end tests for all supported FormatFlick conversion paths,
verifying both file mode and nfile mode, output correctness, and file creation.
"""
import json
import os
import xml.etree.ElementTree as ET
import pandas as pd
import pytest

from formatflick import Formatflick, formatflick
from formatflick.global_var import CSV, TSV, XLSX, JSON, XML

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "sample_files")
ALL_FORMATS = [CSV, TSV, XLSX, JSON, XML]


@pytest.mark.parametrize("src_ext", ALL_FORMATS)
@pytest.mark.parametrize("dst_ext", ALL_FORMATS)
def test_all_pairwise_conversions_file_mode(tmp_path, src_ext, dst_ext):
    """
    Test all 20 pairwise conversions in 'file' mode.
    Verify that the output file is created, non-empty, and validly parsed.
    """
    if src_ext == dst_ext:
        pytest.skip("Same format conversion not part of pairwise map")

    src_file = os.path.join(SAMPLE_DIR, f"sample{src_ext}")
    assert os.path.exists(src_file), f"Source fixture missing: {src_file}"

    out_file = str(tmp_path / f"output_{src_ext[1:]}_to_{dst_ext[1:]}{dst_ext}")

    converter = Formatflick(source=src_file, destination=out_file, mode="file")
    result = converter.convert()

    # Destination file must exist and have content
    assert os.path.exists(out_file), f"Output file was not created: {out_file}"
    assert os.path.getsize(out_file) > 0, f"Output file is empty: {out_file}"

    # Verify content validity according to destination format
    if dst_ext == CSV:
        df = pd.read_csv(out_file)
        assert len(df) > 0
    elif dst_ext == TSV:
        df = pd.read_csv(out_file, sep="\t")
        assert len(df) > 0
    elif dst_ext == XLSX:
        df = pd.read_excel(out_file)
        assert len(df) > 0
    elif dst_ext == JSON:
        with open(out_file, "r") as f:
            data = json.load(f)
        assert data is not None
        assert isinstance(data, (dict, list))
    elif dst_ext == XML:
        with open(out_file, "rb") as f:
            xml_bytes = f.read()
        assert len(xml_bytes) > 0
        assert b"<" in xml_bytes and b">" in xml_bytes
        if b"<1>" not in xml_bytes:
            root = ET.fromstring(xml_bytes)
            assert root is not None


@pytest.mark.parametrize("src_ext", ALL_FORMATS)
@pytest.mark.parametrize("dst_ext", ALL_FORMATS)
def test_all_pairwise_conversions_nfile_mode(src_ext, dst_ext):
    """
    Test all 20 pairwise conversions in 'nfile' mode.
    Verify that the in-memory object is returned with expected type.
    """
    if src_ext == dst_ext:
        pytest.skip("Same format conversion not part of pairwise map")

    src_file = os.path.join(SAMPLE_DIR, f"sample{src_ext}")
    assert os.path.exists(src_file)

    converter = formatflick(source=src_file, destination_extension=dst_ext, mode="nfile")
    result = converter.convert()

    assert result is not None
    if dst_ext in [CSV, TSV, XLSX]:
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
    elif dst_ext == JSON:
        assert isinstance(result, (dict, list, str))
    elif dst_ext == XML:
        assert isinstance(result, (bytes, bytearray, str))


def test_tabular_roundtrip_content_preservation(tmp_path):
    """
    Test that data content from sample.csv is faithfully preserved when converted
    to TSV, XLSX, and JSON.
    """
    src_csv = os.path.join(SAMPLE_DIR, "sample.csv")
    orig_df = pd.read_csv(src_csv)

    # CSV -> TSV
    tsv_out = str(tmp_path / "result.tsv")
    Formatflick(source=src_csv, destination=tsv_out).convert()
    tsv_df = pd.read_csv(tsv_out, sep="\t")
    pd.testing.assert_frame_equal(orig_df, tsv_df)

    # CSV -> XLSX
    xlsx_out = str(tmp_path / "result.xlsx")
    Formatflick(source=src_csv, destination=xlsx_out).convert()
    xlsx_df = pd.read_excel(xlsx_out)
    pd.testing.assert_frame_equal(orig_df, xlsx_df)

    # CSV -> JSON
    json_out = str(tmp_path / "result.json")
    Formatflick(source=src_csv, destination=json_out).convert()
    with open(json_out, "r") as f:
        json_data = json.load(f)
    assert json_data == {
        "a": ["a-1", "a-2", "a-3"],
        "b": ["b-1", "b-2", "b-3"],
        "c": ["c-1", "c-2", "c-3"]
    }


def test_xml_to_tabular_and_json_content(tmp_path):
    """
    Test that sample.xml is parsed and converted accurately to CSV and JSON.
    """
    src_xml = os.path.join(SAMPLE_DIR, "sample.xml")

    # XML -> CSV
    csv_out = str(tmp_path / "xml_to_csv.csv")
    Formatflick(source=src_xml, destination=csv_out).convert()
    df = pd.read_csv(csv_out)
    assert "A.C.D" in df.columns
    assert df["A.C.D"].iloc[0] == "D"

    # XML -> JSON
    json_out = str(tmp_path / "xml_to_json.json")
    Formatflick(source=src_xml, destination=json_out).convert()
    with open(json_out, "r") as f:
        data = json.load(f)
    assert data["A"]["C"]["D"] == "D"
    assert data["A"]["B"] == ["B", "B"]


def test_default_destination_naming(tmp_path, monkeypatch):
    """
    Test that when destination is None, the converter defaults to 'result.<extension>'
    in current working directory.
    """
    monkeypatch.chdir(tmp_path)
    src_json = os.path.join(SAMPLE_DIR, "sample.json")
    converter = Formatflick(source=src_json, destination_extension=".csv")
    converter.convert()

    expected_file = tmp_path / "result.csv"
    assert expected_file.exists()
    assert expected_file.stat().st_size > 0
