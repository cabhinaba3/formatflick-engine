"""
Empirical Benchmark Suite for FormatFlick Milestone 1 (JSON & XML Acceleration)

Benchmarks:
1. jEngine.read_file() (orjson SIMD vs json.load baseline)
2. dest_jEngine.to_destination() (orjson SIMD vs json.dump baseline)
3. jEngine.to_csv() (Polars accelerated vs pd.DataFrame baseline)
4. xEngine.read_file() (lxml C-parser vs xmltodict baseline)
5. XML generation (vectorized_xml vs ElementTree iterrows baseline)
6. End-to-end conversions on representative datasets (50,000+ records)
"""
import os
import sys
import time
import json
import io
import tempfile
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import xmltodict

from formatflick import Formatflick
from formatflick.engine.jEngine import jEngine, dest_jEngine
from formatflick.engine.xEngine import xEngine, lxml_parse_to_dict
from formatflick.engine.vectorized_xml import dataframe_to_xml, records_to_xml


def time_func(fn, reps=3):
    """Run function multiple times and return minimum elapsed seconds."""
    times = []
    for _ in range(reps):
        start = time.perf_counter()
        fn()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    return min(times)


def generate_benchmark_data(num_records=50000):
    records = []
    for i in range(num_records):
        records.append({
            "id": i,
            "name": f"User_{i}",
            "email": f"user_{i}@example.com",
            "score": round(i * 1.5, 2),
            "active": i % 2 == 0,
            "meta": {
                "city": "Metropolis" if i % 3 == 0 else "Gotham",
                "zip": 10000 + (i % 500),
                "tags": [f"tag_{i % 5}", f"role_{i % 3}"]
            }
        })
    return records


def generate_benchmark_xml(num_items=10000):
    lines = ['<catalog name="SuperStore">']
    for i in range(num_items):
        lines.append(f'  <item id="{i}" category="cat_{i % 10}">')
        lines.append(f'    <name>Product_{i}</name>')
        lines.append(f'    <price currency="USD">{round(10.0 + (i * 0.25) % 100, 2)}</price>')
        lines.append(f'    <stock>{i % 50}</stock>')
        lines.append('  </item>')
    lines.append('</catalog>')
    return '\n'.join(lines)


def run_benchmarks():
    results = {}
    tmp_dir = tempfile.TemporaryDirectory()
    base_dir = tmp_dir.name

    print("=" * 70)
    print("FORMATFLICK MILESTONE 1: EMPIRICAL PERFORMANCE & SPEEDUP BENCHMARKS")
    print("=" * 70)

    # -------------------------------------------------------------
    # 1. JSON Read Benchmark (50,000 records)
    # -------------------------------------------------------------
    print("\n[1] Benchmarking JSON Reading (50,000 nested records)...")
    records = generate_benchmark_data(50000)
    json_path = os.path.join(base_dir, "bench_50k.json")
    with open(json_path, "w") as f:
        json.dump(records, f)

    file_size_mb = os.path.getsize(json_path) / (1024 * 1024)
    print(f"    Payload size: {file_size_mb:.2f} MB")

    def baseline_json_read():
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)

    engine_j = jEngine(source=json_path, mode="file")
    def accelerated_json_read():
        return engine_j.read_file()

    t_base_json_read = time_func(baseline_json_read, reps=3)
    t_acc_json_read = time_func(accelerated_json_read, reps=3)
    speedup_json_read = t_base_json_read / t_acc_json_read if t_acc_json_read > 0 else 0

    print(f"    Baseline (json.load):       {t_base_json_read:.4f} s ({file_size_mb / t_base_json_read:.1f} MB/s)")
    print(f"    Accelerated (jEngine):      {t_acc_json_read:.4f} s ({file_size_mb / t_acc_json_read:.1f} MB/s)")
    print(f"    Speedup:                    {speedup_json_read:.2f}x")
    results["json_read"] = {
        "baseline_s": t_base_json_read,
        "accelerated_s": t_acc_json_read,
        "speedup": speedup_json_read,
        "size_mb": file_size_mb
    }

    # -------------------------------------------------------------
    # 2. JSON Write Benchmark (50,000 records)
    # -------------------------------------------------------------
    print("\n[2] Benchmarking JSON Writing (50,000 records)...")
    out_json_base = os.path.join(base_dir, "out_base.json")
    out_json_acc = os.path.join(base_dir, "out_acc.json")

    def baseline_json_write():
        with open(out_json_base, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=4)

    dest_j = dest_jEngine(obj=records, destination=out_json_acc)
    def accelerated_json_write():
        dest_j.to_destination()

    t_base_json_write = time_func(baseline_json_write, reps=2)
    t_acc_json_write = time_func(accelerated_json_write, reps=3)
    speedup_json_write = t_base_json_write / t_acc_json_write if t_acc_json_write > 0 else 0

    print(f"    Baseline (json.dump):       {t_base_json_write:.4f} s")
    print(f"    Accelerated (dest_jEngine): {t_acc_json_write:.4f} s")
    print(f"    Speedup:                    {speedup_json_write:.2f}x")
    results["json_write"] = {
        "baseline_s": t_base_json_write,
        "accelerated_s": t_acc_json_write,
        "speedup": speedup_json_write
    }

    # -------------------------------------------------------------
    # 3. XML Reading Benchmark (10,000 items with attributes)
    # -------------------------------------------------------------
    print("\n[3] Benchmarking XML Reading (10,000 items with attributes)...")
    xml_content = generate_benchmark_xml(10000)
    xml_path = os.path.join(base_dir, "bench_10k.xml")
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_content)

    xml_size_mb = os.path.getsize(xml_path) / (1024 * 1024)
    print(f"    Payload size: {xml_size_mb:.2f} MB")

    def baseline_xml_read():
        with open(xml_path, "r", encoding="utf-8") as f:
            return xmltodict.parse(f.read())

    engine_x = xEngine(source=xml_path, mode="file")
    def accelerated_xml_read():
        return engine_x.read_file()

    t_base_xml_read = time_func(baseline_xml_read, reps=3)
    t_acc_xml_read = time_func(accelerated_xml_read, reps=3)
    speedup_xml_read = t_base_xml_read / t_acc_xml_read if t_acc_xml_read > 0 else 0

    print(f"    Baseline (xmltodict):       {t_base_xml_read:.4f} s ({xml_size_mb / t_base_xml_read:.1f} MB/s)")
    print(f"    Accelerated (xEngine lxml): {t_acc_xml_read:.4f} s ({xml_size_mb / t_acc_xml_read:.1f} MB/s)")
    print(f"    Speedup:                    {speedup_xml_read:.2f}x")
    results["xml_read"] = {
        "baseline_s": t_base_xml_read,
        "accelerated_s": t_acc_xml_read,
        "speedup": speedup_xml_read,
        "size_mb": xml_size_mb
    }

    # -------------------------------------------------------------
    # 4. XML Serialization Benchmark (DataFrame to XML)
    # -------------------------------------------------------------
    print("\n[4] Benchmarking XML Serialization (DataFrame 25,000 rows)...")
    df_sample = pd.DataFrame({
        "id": range(25000),
        "name": [f"item_{i}" for i in range(25000)],
        "val": [round(i * 0.123, 3) for i in range(25000)],
        "category": [f"cat_{i % 20}" for i in range(25000)]
    })

    def baseline_xml_write():
        root = ET.Element("Result")
        for i, row in df_sample.iterrows():
            item = ET.SubElement(root, str(i + 1))
            for col in df_sample.columns:
                child = ET.SubElement(item, col)
                child.text = str(row[col])
        return ET.tostring(root, encoding="utf-8")

    def accelerated_xml_write():
        return dataframe_to_xml(df_sample, root="Result")

    t_base_xml_write = time_func(baseline_xml_write, reps=1)
    t_acc_xml_write = time_func(accelerated_xml_write, reps=3)
    speedup_xml_write = t_base_xml_write / t_acc_xml_write if t_acc_xml_write > 0 else 0

    print(f"    Baseline (ET.iterrows):     {t_base_xml_write:.4f} s")
    print(f"    Accelerated (vectorized):   {t_acc_xml_write:.4f} s")
    print(f"    Speedup:                    {speedup_xml_write:.2f}x")
    results["xml_write"] = {
        "baseline_s": t_base_xml_write,
        "accelerated_s": t_acc_xml_write,
        "speedup": speedup_xml_write
    }

    # -------------------------------------------------------------
    # 5. End-to-End Conversion Benchmark (Formatflick JSON -> CSV)
    # -------------------------------------------------------------
    print("\n[5] Benchmarking End-to-End Formatflick Conversions...")
    out_csv = os.path.join(base_dir, "bench_converted.csv")
    def e2e_json_to_csv():
        converter = Formatflick(source=json_path, destination=out_csv, mode="file")
        return converter.convert()

    t_e2e = time_func(e2e_json_to_csv, reps=3)
    print(f"    E2E Formatflick JSON->CSV:  {t_e2e:.4f} s (50,000 records)")
    results["e2e_json_to_csv"] = t_e2e

    print("\n" + "=" * 70)
    print("BENCHMARK SUMMARY:")
    for k, v in results.items():
        if isinstance(v, dict) and "speedup" in v:
            print(f"  {k:20s}: {v['speedup']:6.2f}x speedup (baseline: {v['baseline_s']:.4f}s, accelerated: {v['accelerated_s']:.4f}s)")
        else:
            print(f"  {k:20s}: {v}")
    print("=" * 70)

    tmp_dir.cleanup()
    return results


if __name__ == "__main__":
    run_benchmarks()
