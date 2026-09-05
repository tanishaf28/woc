#!/usr/bin/env python3
"""Roll up merge_eval.py's per-case merged_woc_{clients,servers}_*.csv files
into two flat CSVs: one row per experiment with throughput + latency, for
clients and for servers separately.

Usage:
    python3 extract_metrics.py <run_dir> [--size N] [--system woc]

<run_dir> is searched recursively for merged/merged_woc_clients_*.csv and
merged/merged_woc_servers_*.csv (both are written by every driver's
merge_case_results, since merge_eval.py is always called once in client mode
and once in --servers mode). The case label is the parent of the 'merged'
directory's name (e.g. for
results/plainmsg_evals/<ts>/n7/eval1_indep_90.0/merged/merged_woc_clients_*.csv
the label is 'eval1_indep_90.0' and size is parsed from the 'n7' segment).
Rows are keyed by (size, label) together, so the same experiment label
recurring under multiple n<N>/ sizes produces one row per size instead of
collapsing into a single row.
If no 'n<N>' segment is present in the path, --size must be supplied (used by
the fixed-size-5 scripts).

Writes <run_dir>/extracted_metrics.csv (clients) and
<run_dir>/extracted_metrics_servers.csv (servers), same columns:
system,size,experiment,filename,throughput,avg_latency,p50,p95,p99,conflict,fallbacks
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import sys
from typing import Optional


METRIC_FIELDS = [
    "TOTAL_THROUGHPUT",
    "AVG_LATENCY",
    "P50_LATENCY",
    "P95_LATENCY",
    "P99_LATENCY",
    "TOTAL_CONFLICT_COMMITS",
    "TOTAL_FAST_PATH_FALLBACKS",
]


def _parse_value(raw: str) -> float:
    match = re.search(r"-?\d+(?:\.\d+)?", raw.replace(",", ""))
    return float(match.group(0)) if match else 0.0


def _parse_case_csv(path: str) -> dict[str, float]:
    values: dict[str, float] = {field: 0.0 for field in METRIC_FIELDS}
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # header: metric,value,notes
        for row in reader:
            if not row:
                continue
            metric = row[0].strip()
            if metric in values and len(row) > 1:
                values[metric] = _parse_value(row[1])
    return values


def _pick_latest(csvs: list[str]) -> str:
    return max(csvs, key=os.path.basename)


def _size_from_path(path: str, default_size: Optional[int]) -> int:
    match = re.search(r"[\\/]n(\d+)[\\/]", path)
    if match:
        return int(match.group(1))
    if default_size is not None:
        return default_size
    raise ValueError(f"Could not infer size from path {path!r}; pass --size")


def extract(run_dir: str, system: str, default_size: Optional[int], glob_name: str, output_name: str) -> str:
    pattern = os.path.join(run_dir, "**", "merged", glob_name)
    case_csvs: dict[tuple[int, str], list[str]] = {}
    for csv_path in glob.glob(pattern, recursive=True):
        merged_dir = os.path.dirname(csv_path)
        case_dir = os.path.dirname(merged_dir)
        label = os.path.basename(case_dir)
        size = _size_from_path(csv_path, default_size)
        case_csvs.setdefault((size, label), []).append(csv_path)

    if not case_csvs:
        print(f"WARNING: no {glob_name} found under {run_dir}", file=sys.stderr)
        return ""

    output_path = os.path.join(run_dir, output_name)
    write_header = not os.path.exists(output_path)

    with open(output_path, "a", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        if write_header:
            writer.writerow(
                ["system", "size", "experiment", "filename", "throughput",
                 "avg_latency", "p50", "p95", "p99", "conflict", "fallbacks"]
            )
        for size, label in sorted(case_csvs):
            latest = _pick_latest(case_csvs[(size, label)])
            metrics = _parse_case_csv(latest)
            writer.writerow([
                system,
                size,
                label,
                os.path.basename(latest),
                metrics["TOTAL_THROUGHPUT"],
                metrics["AVG_LATENCY"],
                metrics["P50_LATENCY"],
                metrics["P95_LATENCY"],
                metrics["P99_LATENCY"],
                metrics["TOTAL_CONFLICT_COMMITS"],
                metrics["TOTAL_FAST_PATH_FALLBACKS"],
            ])

    print(f"Wrote {len(case_csvs)} row(s) to {output_path}")
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", help="Run directory to scan for merged/merged_woc_{clients,servers}_*.csv")
    parser.add_argument("--size", type=int, default=None, help="Cluster size to record when not inferable from an n<N>/ path segment")
    parser.add_argument("--system", default="woc", help="System name column (default: woc)")
    args = parser.parse_args()

    try:
        extract(args.run_dir, args.system, args.size, "merged_woc_clients_*.csv", "extracted_metrics.csv")
        extract(args.run_dir, args.system, args.size, "merged_woc_servers_*.csv", "extracted_metrics_servers.csv")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
