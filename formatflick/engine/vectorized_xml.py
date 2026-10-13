"""
Vectorized and buffered XML serialization for FormatFlick.
Provides high-performance, SIMD-style vector concatenation for DataFrames
and fast buffered serialization for dictionary records, completely eliminating
the O(N) pd.DataFrame.iterrows() bottleneck.
"""
from typing import Any, Iterable, List, Union, IO
import io
import xml.sax.saxutils as saxutils
import pandas as pd


def _resolve_root_name(root: Any) -> str:
    if root is None:
        return 'Result'
    if isinstance(root, str):
        return root
    if hasattr(root, 'tag'):
        return root.tag
    return str(root)


def _format_chunk(chunk: pd.DataFrame, row_ids: pd.Series) -> str:
    """
    Format a DataFrame chunk into an XML body string.
    Safely handles:
    - Missing values (NaN, None, <NA>) without silent row deletion
    - Duplicate column names using positional iloc indexing
    - XML reserved character escaping for strings and objects
    """
    s_open = '<' + row_ids + '>'
    s_close = '</' + row_ids + '>'

    cols_xml = []
    n_cols = chunk.shape[1]
    for c_idx in range(n_cols):
        c = str(chunk.columns[c_idx])
        s = chunk.iloc[:, c_idx]
        if pd.api.types.is_integer_dtype(s) and not s.hasnans:
            val_str = s.astype(str)
        elif pd.api.types.is_float_dtype(s):
            val_str = s.astype(str).fillna('nan')
        else:
            val_str = s.astype(object).fillna('nan').map(lambda x: saxutils.escape(str(x)))
        cols_xml.append(f'<{c}>' + val_str + f'</{c}>')

    body = s_open
    for c_xml in cols_xml:
        body = body + c_xml
    body = body + s_close
    return body.str.cat(na_rep='')


def dataframe_to_xml(df: pd.DataFrame, root: Any = 'Result', chunk_size: int = 50000) -> bytes:
    """
    Vectorized XML generation for pandas DataFrames.
    Eliminates iterrows() bottleneck by operating on columnar Series buffers.
    Preserves exact schema: <Result><1><col>val</col>...</1><2>...</2></Result>.

    :param df: Input pandas DataFrame
    :param root: XML root element tag name or element (default: 'Result')
    :param chunk_size: Batch size for chunked memory buffering
    :return: XML output as bytes
    """
    root_name = _resolve_root_name(root)

    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)
    if df.empty:
        return f'<{root_name} />'.encode('utf-8')

    n_rows = len(df)

    # In-memory single-shot vectorization for datasets within chunk_size
    if n_rows <= chunk_size:
        is_numeric_idx = pd.api.types.is_numeric_dtype(df.index)
        if is_numeric_idx:
            row_ids = (df.index + 1).astype(str).fillna('nan')
        else:
            row_ids = pd.Series(range(1, n_rows + 1), index=df.index, dtype=str)

        body_str = _format_chunk(df, row_ids)
        return (f'<{root_name}>' + body_str + f'</{root_name}>').encode('utf-8')

    # Chunked buffer concatenation for larger datasets (bounded memory)
    buf = io.BytesIO()
    stream_dataframe_to_xml(df, buf, root=root_name, chunk_size=chunk_size)
    return buf.getvalue()


def records_to_xml(records: Any, root: Any = 'Result') -> bytes:
    """
    Fast buffered XML generation for list-of-dict or dict inputs.
    Preserves exact schema: <Result><key>val</key>...</Result>.

    :param records: List of dictionaries or dict or DataFrame
    :param root: XML root element tag name (default: 'Result')
    :return: XML output as bytes
    """
    root_name = _resolve_root_name(root)

    if isinstance(records, dict):
        records = [records]
    elif isinstance(records, pd.DataFrame):
        buf = io.StringIO()
        buf.write(f'<{root_name}>')
        write = buf.write
        escape = saxutils.escape
        n_rows = len(records)
        n_cols = records.shape[1]
        for r_idx in range(n_rows):
            for c_idx in range(n_cols):
                k = str(records.columns[c_idx])
                v = records.iat[r_idx, c_idx]
                write(f'<{k}>{escape(str(v))}</{k}>')
        buf.write(f'</{root_name}>')
        return buf.getvalue().encode('utf-8')
    elif not isinstance(records, (list, tuple)):
        records = [records]

    if not records:
        return f'<{root_name} />'.encode('utf-8')

    buf = io.StringIO()
    buf.write(f'<{root_name}>')
    write = buf.write
    escape = saxutils.escape
    for item in records:
        if isinstance(item, dict):
            for key, value in item.items():
                write(f'<{key}>{escape(str(value))}</{key}>')
        else:
            write(f'<item>{escape(str(item))}</item>')
    buf.write(f'</{root_name}>')
    return buf.getvalue().encode('utf-8')


def stream_dataframe_to_xml(df: pd.DataFrame, out_stream: IO[bytes], root: Any = 'Result', chunk_size: int = 50000) -> None:
    """
    Stream DataFrame directly to a binary output stream chunk-by-chunk with O(chunk_size) RAM.
    """
    root_name = _resolve_root_name(root)

    if df.empty:
        out_stream.write(f'<{root_name} />'.encode('utf-8'))
        return

    out_stream.write(f'<{root_name}>'.encode('utf-8'))
    n_rows = len(df)
    is_numeric_idx = pd.api.types.is_numeric_dtype(df.index)

    for start in range(0, n_rows, chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        if is_numeric_idx:
            row_ids = (chunk.index + 1).astype(str).fillna('nan')
        else:
            row_ids = pd.Series(range(start + 1, start + len(chunk) + 1), index=chunk.index, dtype=str)

        out_stream.write(_format_chunk(chunk, row_ids).encode('utf-8'))

    out_stream.write(f'</{root_name}>'.encode('utf-8'))
