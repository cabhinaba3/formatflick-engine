from typing import Any, Union
from pathlib import Path
import json
import pandas as pd

from .engine import engine, dest_engine
from .vectorized_xml import records_to_xml

try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


class jEngine(engine):
    def __init__(self,
                 source: Path,
                 mode: str,
                 *args, **kwargs
                 ):
        super().__init__(source, mode, *args, **kwargs)

    def read_file(self) -> Union[tuple[list[Any], list[dict[Any, Any]]], tuple[None, Any]]:
        """
        Reading the JSON file with SIMD acceleration (orjson) and stdlib fallback.
        Flattens nested JSON structures into dot-notation dictionaries.
        Returns a tuple of (headers, flatten_obj).
        """
        obj = None
        if HAS_ORJSON:
            try:
                with open(self.source, 'rb') as json_file:
                    raw_bytes = json_file.read()
                obj = orjson.loads(raw_bytes)
            except Exception:
                obj = None

        if obj is None:
            with open(self.source, 'r', encoding='utf-8') as json_file:
                obj = json.load(json_file)

        if not isinstance(obj, list):
            obj = [obj]

        flatten_obj = [self.flatten_json(item) for item in obj]

        headers_set = set()
        for entry in flatten_obj:
            if isinstance(entry, dict):
                headers_set.update(entry.keys())
        headers = list(headers_set)

        return headers, flatten_obj

    @staticmethod
    def flatten_json_util(json_obj, prefix='_', depth=0, out_dict=None):
        """
        Optimized key-path flattening for hierarchical JSON.
        Preserves dot-notation keys (e.g., 'c.d.0', 'c.e.f') and schema unions.
        Directly populates an accumulator dict to avoid redundant intermediate
        dictionary allocations and merge operations.
        """
        flat_dict = {} if out_dict is None else out_dict
        if isinstance(json_obj, dict):
            for key, value in json_obj.items():
                _key = f"{prefix}{key}" if depth != 0 else key
                if isinstance(value, (dict, list)):
                    jEngine.flatten_json_util(value, f"{_key}.", depth + 1, flat_dict)
                else:
                    flat_dict[_key] = value
        elif isinstance(json_obj, list):
            for i, item in enumerate(json_obj):
                _key = f"{prefix}{i}" if depth != 0 else i
                if isinstance(item, (dict, list)):
                    jEngine.flatten_json_util(item, f"{_key}.", depth + 1, flat_dict)
                else:
                    flat_dict[_key] = item
        else:
            flat_dict[prefix[:-1]] = json_obj
        return flat_dict

    def flatten_json(self, json_obj):
        """
        Flatten method for json objects with fast-path for flat dictionaries.
        """
        if isinstance(json_obj, dict):
            # Fast-path: if no nested containers exist, shallow copy is sufficient
            for v in json_obj.values():
                if isinstance(v, (dict, list)):
                    break
            else:
                return json_obj.copy()

        prefix = self.kwargs.get("prefix", '_')
        return self.flatten_json_util(json_obj, prefix, 0)

    @staticmethod
    def to_csv(obj):
        """
        Convert flattened JSON records into tabular DataFrame with Polars acceleration.
        """
        if HAS_POLARS:
            try:
                return pl.DataFrame(obj).to_pandas()
            except Exception:
                pass
        return pd.DataFrame(obj)

    @staticmethod
    def to_tsv(obj):
        """
        Convert flattened JSON records into tabular DataFrame for TSV serialization.
        """
        return jEngine.to_csv(obj)

    @staticmethod
    def to_xlsx(obj):
        """
        Convert flattened JSON records into tabular DataFrame for XLSX serialization.
        """
        return jEngine.to_csv(obj)

    @staticmethod
    def to_json(obj):
        """
        Returns JSON object representation (list of dicts).
        """
        return obj

    @staticmethod
    def to_xml(obj, root=None):
        """
        Convert a JSON list-of-dicts into XML byte output with fast buffer formatting.
        """
        return records_to_xml(obj, root=root)


class dest_jEngine(dest_engine):
    def to_destination(self):
        """
        Converts in-memory JSON data to destination file.
        Uses SIMD orjson for ultra-fast serialization with graceful fallback to json.dump.
        """
        data = self.obj
        if isinstance(data, (str, bytes, bytearray)):
            try:
                data = orjson.loads(data) if HAS_ORJSON else json.loads(data)
            except Exception:
                pass

        if HAS_ORJSON:
            try:
                if isinstance(data, str):
                    with open(self.destination, 'w', encoding='utf-8') as dest:
                        dest.write(data)
                elif isinstance(data, (bytes, bytearray)):
                    with open(self.destination, 'wb') as dest:
                        dest.write(data)
                else:
                    payload = orjson.dumps(
                        data,
                        option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY,
                        default=str
                    )
                    with open(self.destination, 'wb') as dest:
                        dest.write(payload)
                return
            except Exception:
                pass

        # Fallback stdlib json serializer
        with open(self.destination, 'w+', encoding='utf-8') as dest:
            if isinstance(data, str):
                dest.write(data)
            elif isinstance(data, (bytes, bytearray)):
                dest.write(data.decode('utf-8'))
            else:
                json.dump(data, dest, indent=4)
