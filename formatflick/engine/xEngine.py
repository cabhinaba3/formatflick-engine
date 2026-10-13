from typing import Any
from .engine import engine, dest_engine
from .jEngine import jEngine
from pathlib import Path
import os
import re
import xmltodict
import json
import pandas as pd

try:
    import lxml.etree as ET
    HAS_LXML = True
except ImportError:
    HAS_LXML = False

TAG_NUM_PREFIX = b"__ff_num__"
TAG_NUM_PREFIX_STR = "__ff_num__"
NUM_TAG_BYTES_REGEX = re.compile(rb"<(/)?([0-9][a-zA-Z0-9_\-\.]*)([\s>/])")
NUM_TAG_STR_REGEX = re.compile(r"<(/)?([0-9][a-zA-Z0-9_\-\.]*)([\s>/])")


def _desanitize_tag(tag: str) -> str:
    """Strips internal prefix applied to sanitize numeric tags (<1>, <2>)."""
    if tag.startswith(TAG_NUM_PREFIX_STR):
        return tag[len(TAG_NUM_PREFIX_STR):]
    return tag


def _element_to_dict(elem) -> Any:
    """
    High-performance C-accelerated element-to-dict converter.
    Produces identical schema to xmltodict.parse():
    - Attributes mapped to '@<attr>'
    - Repeating sibling elements mapped to list
    - Leaf text mapped to string (or None if empty)
    - Elements with attributes and text mapped to '#text'
    """
    has_attrib = bool(elem.attrib)
    text = elem.text
    num_children = len(elem)

    if num_children == 0:
        if not has_attrib:
            if text is not None:
                t = text.strip()
                return t if t != '' else None
            return None
        d = {'@' + k: v for k, v in elem.attrib.items()}
        if text is not None:
            t = text.strip()
            if t:
                d['#text'] = t
        return d

    d = {'@' + k: v for k, v in elem.attrib.items()} if has_attrib else {}
    child_dict = {}
    for child in elem:
        cd = _element_to_dict(child)
        qname = ET.QName(child)
        tag = f'{child.prefix}:{qname.localname}' if child.prefix else qname.localname
        tag = _desanitize_tag(tag)
        if tag in child_dict:
            existing = child_dict[tag]
            if isinstance(existing, list):
                existing.append(cd)
            else:
                child_dict[tag] = [existing, cd]
        else:
            child_dict[tag] = cd
    d.update(child_dict)
    if text is not None:
        t = text.strip()
        if t:
            d['#text'] = t
    return d


def lxml_parse_to_dict(source) -> dict:
    """
    Parse XML using libxml2 (via lxml.etree) in C and convert to standard dict schema.
    Transparently supports FormatFlick self-generated XML containing numeric element tags.
    """
    parser = ET.XMLParser(remove_comments=True)
    root = None
    if isinstance(source, (str, Path, os.PathLike)) and os.path.exists(source):
        try:
            root = ET.parse(str(source), parser=parser).getroot()
        except ET.XMLSyntaxError:
            with open(source, "rb") as f:
                content = f.read()
    else:
        content = source.encode('utf-8') if isinstance(source, str) else source

    if root is None:
        try:
            root = ET.fromstring(content, parser=parser)
        except ET.XMLSyntaxError:
            sanitized = NUM_TAG_BYTES_REGEX.sub(rb"<\1" + TAG_NUM_PREFIX + rb"\2\3", content)
            root = ET.fromstring(sanitized, parser=parser)

    qname = ET.QName(root)
    root_tag = f'{root.prefix}:{qname.localname}' if root.prefix else qname.localname
    return {_desanitize_tag(root_tag): _element_to_dict(root)}


class xEngine(engine):
    def __init__(self,
                 source: Path,
                 mode: str,
                 *args, **kwargs
                 ):
        super().__init__(source, mode, *args, **kwargs)

    def read_file(self) -> dict:
        """
        Read XML source file using C-accelerated lxml parsing,
        with transparent fallback to xmltodict for maximum resilience.
        Seamlessly handles self-generated XML files containing numeric tags.
        """
        if HAS_LXML:
            try:
                return lxml_parse_to_dict(self.source)
            except Exception:
                pass

        with open(self.source, "r", encoding="utf-8") as xml_file:
            xml_data = xml_file.read()
            try:
                return xmltodict.parse(xml_data)
            except Exception:
                sanitized = NUM_TAG_STR_REGEX.sub(rf"<\1{TAG_NUM_PREFIX_STR}\2\3", xml_data)

                def postprocessor(path, key, value):
                    if key and key.startswith(TAG_NUM_PREFIX_STR):
                        return key[len(TAG_NUM_PREFIX_STR):], value
                    return key, value

                return xmltodict.parse(sanitized, postprocessor=postprocessor)

    @staticmethod
    def to_json(obj):
        return json.dumps(obj, indent=4)

    @staticmethod
    def to_csv(obj):
        if isinstance(obj, dict) and len(obj) == 1:
            root_val = next(iter(obj.values()))
            if isinstance(root_val, dict) and len(root_val) >= 1:
                first_k = next(iter(root_val.keys()))
                first_v = next(iter(root_val.values()))
                if isinstance(first_v, dict) and (first_k.isdigit() or (len(root_val) > 1 and all(isinstance(v, dict) for v in root_val.values()))):
                    flat_rows = [jEngine.flatten_json_util(v) if isinstance(v, (dict, list)) else v for v in root_val.values()]
                    return pd.DataFrame(flat_rows)
            elif isinstance(root_val, list) and all(isinstance(v, dict) for v in root_val):
                flat_rows = [jEngine.flatten_json_util(v) if isinstance(v, (dict, list)) else v for v in root_val]
                return pd.DataFrame(flat_rows)
        flat_obj = jEngine.flatten_json_util(obj, '_', 0)
        df = pd.DataFrame([flat_obj])
        return df

    @staticmethod
    def to_tsv(obj):
        return xEngine.to_csv(obj)

    @staticmethod
    def to_xlsx(obj):
        return xEngine.to_csv(obj)

    @staticmethod
    def to_xml(obj, root=None):
        if isinstance(obj, (bytes, bytearray)):
            return obj
        if isinstance(obj, str):
            return obj
        if isinstance(obj, dict):
            return xmltodict.unparse(obj, pretty=True)
        return str(obj)


class dest_xEngine(dest_engine):
    def to_destination(self):
        data = self.obj
        if isinstance(data, (bytes, bytearray)):
            with open(self.destination, "wb") as dest:
                dest.write(data)
        elif isinstance(data, str):
            with open(self.destination, "w", encoding="utf-8") as dest:
                dest.write(data)
        elif isinstance(data, dict):
            with open(self.destination, "w", encoding="utf-8") as dest:
                dest.write(xmltodict.unparse(data, pretty=True))
        else:
            with open(self.destination, "w", encoding="utf-8") as dest:
                dest.write(str(data))
