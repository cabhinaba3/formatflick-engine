# formatflick

A lightweight Python library to seamlessly convert between different structured and tabular data file formats.

## Supported Formats

FormatFlick supports bidirectional conversion across 5 common formats (all 20 pairwise conversion paths):
- **CSV** (`.csv`)
- **TSV** (`.tsv`)
- **Excel** (`.xlsx`)
- **JSON** (`.json`)
- **XML** (`.xml`)

## Installation

Install dependencies and the package:

```bash
pip install -e .
```

Core dependencies:
- `pandas` (tabular processing)
- `openpyxl` (Excel support)
- `xmltodict` & `lxml` (XML parsing and generation)

## Usage

Using `formatflick` is simple:

### Basic File Conversion (File Mode)

```python
from formatflick import Formatflick

# Convert CSV to JSON (defaults to 'result.json' in current directory)
converter = Formatflick("sample.csv", destination_extension=".json")
converter.convert()

# Convert with explicit destination path
converter = Formatflick("sample.json", destination="output.csv")
converter.convert()
```

### In-Memory Conversion (nfile Mode)

You can also convert in-memory without writing a destination file:

```python
from formatflick import Formatflick

converter = Formatflick("sample.csv", destination_extension=".json", mode="nfile")
data = converter.convert()  # returns dict for JSON, DataFrame for CSV/TSV/XLSX, bytes for XML
```

## Running Tests

Run the test suite with `pytest`:

```bash
pytest
```