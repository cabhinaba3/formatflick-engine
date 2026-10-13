"""
Tests for FormatFlick public API styles, edge cases, error conditions, and logging.
"""
import os
import pytest

import formatflick as pkg
from formatflick import Formatflick, formatflick
from formatflick.formatflick import Formatflick as FF_from_module, formatflick as ff_from_module
from formatflick.global_var import VALID_EXTENSIONS

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "sample_files")
SAMPLE_CSV = os.path.join(SAMPLE_DIR, "sample.csv")


def test_api_import_consistency():
    """Verify that all documented and common import forms resolve cleanly."""
    assert pkg.Formatflick is pkg.formatflick
    assert Formatflick is formatflick
    assert FF_from_module is ff_from_module
    assert Formatflick is FF_from_module


def test_instantiation_via_all_aliases():
    """Verify all class aliases can instantiate a working converter."""
    c1 = pkg.Formatflick(SAMPLE_CSV, destination_extension=".json")
    c2 = pkg.formatflick(SAMPLE_CSV, destination_extension=".json")
    c3 = Formatflick(SAMPLE_CSV, destination_extension=".json")
    c4 = formatflick(SAMPLE_CSV, destination_extension=".json")
    c5 = FF_from_module(SAMPLE_CSV, destination_extension=".json")
    c6 = ff_from_module(SAMPLE_CSV, destination_extension=".json")

    for c in [c1, c2, c3, c4, c5, c6]:
        assert c.source == SAMPLE_CSV
        assert c.destination_extension == ".json"


def test_nfile_mode_missing_destination_extension():
    """nfile mode requires destination_extension."""
    with pytest.raises(Exception) as exc_info:
        Formatflick(SAMPLE_CSV, destination="out.json", destination_extension=None, mode="nfile")
    assert "destination_extension cannot be None for nfile mode" in str(exc_info.value)


def test_unsupported_destination_extension():
    """Extensions not in VALID_EXTENSIONS raise clear error."""
    with pytest.raises(Exception) as exc_info:
        Formatflick(SAMPLE_CSV, destination_extension=".unsupported")
    assert ".unsupported is not valid" in str(exc_info.value)


def test_disallowed_conversion():
    """Conversions not in conversion map (e.g. self-conversion) raise an error."""
    with pytest.raises(Exception) as exc_info:
        Formatflick(SAMPLE_CSV, destination_extension=".csv")
    assert "is not allowed" in str(exc_info.value)


def test_verbosity_and_logging(tmp_path):
    """Verify verbosity levels 0 to 4 can be set without crashing."""
    out_file = str(tmp_path / "out.json")
    for verb in [0, 1, 2, 3, 4]:
        conv = Formatflick(SAMPLE_CSV, destination=out_file, verbosity=verb)
        assert conv.log is not None
        res = conv.convert()
        assert res is not None

    # Invalid verbosity level should raise error from logger
    with pytest.raises(Exception) as exc:
        Formatflick(SAMPLE_CSV, destination=out_file, verbosity=99)
    assert "should be between 0 to 3" in str(exc.value) or "99" in str(exc.value)


def test_missing_source_file():
    """Non-existent source path raises expected exception."""
    with pytest.raises(Exception) as exc:
        Formatflick("/path/that/definitely/does/not/exist.csv", destination_extension=".json")
    assert "The source path dont exits" in str(exc.value)
