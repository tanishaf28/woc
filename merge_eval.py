#!/usr/bin/env python3


from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import sys
from datetime import datetime
from typing import Optional


def _cell(row: list[str], idx: int) -> str:
    return row[idx].strip() if idx < len(row) else ""


def _parse_float(text: str) -> Optional[float]:
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    return float(match.group(0))


def _parse_int(text: str) -> Optional[int]:
    match = re.search(r"-?\d+", text.replace(",", ""))
    if not match:
        return None
    return int(match.group(0))


def _extract_run_timestamp(path: str) -> Optional[str]:
    # Expected filenames include a run timestamp suffix:
    # s0_n5_f2_b1_woc_YYYYMMDD_HHMMSS.csv or client5_woc_YYYYMMDD_HHMMSS.csv
    match = re.search(r"_(\d{8}_\d{6})\.csv$", os.path.basename(path))
    if not match:
        return None
    return match.group(1)


def _pick_latest_csv(csvs: list[str]) -> str:
    # Prefer filename timestamp to avoid SCP copy-order mtime skew.
    with_ts = [(path, _extract_run_timestamp(path)) for path in csvs]
    with_ts = [(path, ts) for path, ts in with_ts if ts is not None]
    if with_ts:
        return max(with_ts, key=lambda item: item[1])[0]
    return max(csvs, key=os.path.getmtime)


def _default_output(eval_dir: str, suffix: str = "clients") -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(eval_dir, "merged", f"merged_woc_{suffix}_{timestamp}.csv")


def _resolve_output(eval_dir: str, output: Optional[str], suffix: str = "clients") -> str:
    if not output:
        return _default_output(eval_dir, suffix)

    # If output is an existing directory or ends with a path separator,
    # write a timestamped file inside that directory.
    if os.path.isdir(output) or output.endswith(os.sep) or output.endswith("/"):
        return os.path.join(output, os.path.basename(_default_output(eval_dir, suffix)))

    return output


def _parse_id_filter(ids_arg: Optional[str]) -> Optional[set[int]]:
    if not ids_arg:
        return None

    ids: set[int] = set()
    for token in ids_arg.split(","):
        token = token.strip()
        if not token:
            continue

        if "-" in token:
            start_str, end_str = token.split("-", 1)
            start = int(start_str)
            end = int(end_str)
            if start > end:
                start, end = end, start
            ids.update(range(start, end + 1))
        else:
            ids.add(int(token))

    if not ids:
        raise ValueError("--ids produced an empty set")
    return ids


def _extract_dir_id(path: str, prefix: str) -> Optional[int]:
    name = os.path.basename(os.path.normpath(path))
    match = re.fullmatch(rf"{prefix}(\d+)", name)
    if not match:
        return None
    return int(match.group(1))


def _filter_dirs_by_ids(dirs: list[str], prefix: str, allowed_ids: Optional[set[int]]) -> list[str]:
    if allowed_ids is None:
        return dirs

    filtered: list[str] = []
    for d in dirs:
        dir_id = _extract_dir_id(d, prefix)
        if dir_id is not None and dir_id in allowed_ids:
            filtered.append(d)
    return filtered


def merge_client_csvs(
    eval_dir: str = "./eval",
    output: Optional[str] = None,
    allowed_client_ids: Optional[set[int]] = None,
) -> str:
    """Merge per-client WOC evaluation CSV files into one summary CSV.
    
    WOC client CSV format (6-column header):
    pclock,latency (ms) per batch,throughput (Tx/sec),fast path ops,slow path ops,conflict ops
    
    Summary rows to extract:
    - AVERAGE
    - THROUGHPUT
    - P50_LATENCY, P95_LATENCY, P99_LATENCY
    - AVG_LATENCY_PER_TX
    - AVG_FAST_PATH_LATENCY, AVG_SLOW_PATH_LATENCY
    - GLOBAL_TOTALS (contains fast/slow/conflict ops in columns 3-5)
    - TOTAL_FAST_PATH_FALLBACKS
    - TOTAL_FAST_COMMITS, TOTAL_SLOW_COMMITS, TOTAL_CONFLICT_COMMITS
    """
    all_latencies: list[float] = []
    total_fast = 0
    total_slow = 0
    total_conflict = 0
    all_throughputs: list[float] = []
    fast_latencies: list[float] = []
    slow_latencies: list[float] = []
    total_fallbacks = 0

    client_dirs_all = [
        d
        for d in glob.glob(os.path.join(eval_dir, "client*/"))
        if os.path.isdir(d)
    ]
    client_dirs = _filter_dirs_by_ids(client_dirs_all, "client", allowed_client_ids)
    if not client_dirs:
        if allowed_client_ids is None:
            raise RuntimeError(f"No client directories found in {eval_dir}")
        raise RuntimeError(
            f"No matching client directories found in {eval_dir} for ids: {sorted(allowed_client_ids)}"
        )

    if allowed_client_ids is None:
        print(f"Found {len(client_dirs)} client directories")
    else:
        print(
            f"Found {len(client_dirs)} client directories matching ids {sorted(allowed_client_ids)}"
        )

    merged_clients = 0
    for client_dir in sorted(client_dirs):
        csvs = glob.glob(os.path.join(client_dir, "*.csv"))
        if not csvs:
            print(f"  WARNING: No CSV in {client_dir}")
            continue

        # Take the latest run by filename timestamp if present.
        latest = _pick_latest_csv(csvs)
        merged_clients += 1
        print(f"  Reading: {latest}")

        with open(latest, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue

                label = _cell(row, 0)
                if label in ("pclock", "NO_DATA", ""):
                    continue

                # Per-batch rows have numeric pclock in column 0.
                try:
                    int(label)
                    lat = _parse_float(_cell(row, 1))
                    if lat is not None and lat > 0:
                        all_latencies.append(lat)
                    continue
                except ValueError:
                    pass

                # Summary rows.
                if label == "THROUGHPUT":
                    tpt = _parse_float(_cell(row, 2))
                    if tpt is not None:
                        all_throughputs.append(tpt)
                elif label == "AVG_FAST_PATH_LATENCY":
                    lat = _parse_float(_cell(row, 1))
                    count = _parse_int(_cell(row, 3))
                    if lat is not None:
                        fast_latencies.extend([lat] * (count if count and count > 0 else 1))
                elif label == "AVG_SLOW_PATH_LATENCY":
                    lat = _parse_float(_cell(row, 1))
                    count = _parse_int(_cell(row, 4))
                    if lat is not None:
                        slow_latencies.extend([lat] * (count if count and count > 0 else 1))
                elif label == "GLOBAL_TOTALS":
                    # WOC format: "GLOBAL_TOTALS", "", "", "fast ops", "slow ops", "conflict ops"
                    fast_str = _cell(row, 3)
                    slow_str = _cell(row, 4)
                    conflict_str = _cell(row, 5)
                    
                    fast_val = _parse_int(fast_str)
                    slow_val = _parse_int(slow_str)
                    conflict_val = _parse_int(conflict_str)
                    
                    if fast_val is not None:
                        total_fast += fast_val
                    if slow_val is not None:
                        total_slow += slow_val
                    if conflict_val is not None:
                        total_conflict += conflict_val
                elif label == "TOTAL_FAST_PATH_FALLBACKS":
                    val = _parse_int(_cell(row, 1))
                    if val is not None:
                        total_fallbacks += val

    if not all_latencies:
        raise RuntimeError("No latency data found in client CSV files")

    output_path = _resolve_output(eval_dir, output)
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    all_latencies.sort()
    n = len(all_latencies)
    p50 = all_latencies[n * 50 // 100]
    p95 = all_latencies[n * 95 // 100]
    p99 = all_latencies[n * 99 // 100]
    avg_lat = sum(all_latencies) / n

    # Sum per-client wall-clock throughput values into aggregate throughput.
    total_tpt = sum(all_throughputs)

    avg_fast_lat = sum(fast_latencies) / len(fast_latencies) if fast_latencies else 0.0
    avg_slow_lat = sum(slow_latencies) / len(slow_latencies) if slow_latencies else 0.0

    total_ops = total_fast + total_slow + total_conflict
    fast_ratio = (total_fast / total_ops) if total_ops > 0 else 0.0
    slow_ratio = (total_slow / total_ops) if total_ops > 0 else 0.0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value", "notes"])
        writer.writerow(["NUM_CLIENT_DIRS", len(client_dirs), "client* folders under eval"])
        writer.writerow(["NUM_CLIENTS_MERGED", merged_clients, "clients with CSV data"])
        writer.writerow(["TOTAL_THROUGHPUT", f"{total_tpt:.1f} Tx/sec", "sum of all clients"])
        writer.writerow(["AVG_LATENCY", f"{avg_lat:.3f} ms", "across all batches all clients"])
        writer.writerow(["P50_LATENCY", f"{p50:.1f} ms", ""])
        writer.writerow(["P95_LATENCY", f"{p95:.1f} ms", ""])
        writer.writerow(["P99_LATENCY", f"{p99:.1f} ms", ""])
        writer.writerow(["AVG_FAST_PATH_LATENCY", f"{avg_fast_lat:.3f} ms", ""])
        writer.writerow(["AVG_SLOW_PATH_LATENCY", f"{avg_slow_lat:.3f} ms", ""])
        writer.writerow(["TOTAL_FAST_COMMITS", total_fast, f"{fast_ratio * 100:.1f}%"])
        writer.writerow(["TOTAL_SLOW_COMMITS", total_slow, f"{slow_ratio * 100:.1f}%"])
        writer.writerow(["TOTAL_CONFLICT_COMMITS", total_conflict, ""])
        writer.writerow(["TOTAL_FAST_PATH_FALLBACKS", total_fallbacks, ""])

    print(f"\nMerged CSV written to: {output_path}")
    print(f"  Clients merged: {merged_clients}/{len(client_dirs)}")
    print(f"  Total throughput: {total_tpt:.1f} Tx/sec")
    print(f"  Avg latency: {avg_lat:.3f} ms")
    print(f"  P99 latency: {p99:.1f} ms")
    print(f"  Fast/Slow ratio: {fast_ratio * 100:.1f}% / {slow_ratio * 100:.1f}%")

    return output_path


def merge_server_csvs(
    eval_dir: str = "./eval",
    output: Optional[str] = None,
    allowed_server_ids: Optional[set[int]] = None,
) -> str:
    """Merge per-server WOC evaluation CSV files into one summary CSV.
    
    Similar to merge_client_csvs but for server-side metrics from eval/server*/ directories.
    """
    all_latencies: list[float] = []
    total_fast = 0
    total_slow = 0
    total_conflict = 0
    all_throughputs: list[float] = []
    fast_latencies: list[float] = []
    slow_latencies: list[float] = []
    total_fallbacks = 0
    summary_avg_latencies: list[float] = []
    summary_p50_latencies: list[float] = []
    summary_p95_latencies: list[float] = []
    summary_p99_latencies: list[float] = []

    server_dirs_all = [
        d
        for d in glob.glob(os.path.join(eval_dir, "server*/"))
        if os.path.isdir(d)
    ]
    server_dirs = _filter_dirs_by_ids(server_dirs_all, "server", allowed_server_ids)
    if not server_dirs:
        if allowed_server_ids is None:
            raise RuntimeError(f"No server directories found in {eval_dir}")
        raise RuntimeError(
            f"No matching server directories found in {eval_dir} for ids: {sorted(allowed_server_ids)}"
        )

    if allowed_server_ids is None:
        print(f"Found {len(server_dirs)} server directories")
    else:
        print(
            f"Found {len(server_dirs)} server directories matching ids {sorted(allowed_server_ids)}"
        )

    merged_servers = 0
    for server_dir in sorted(server_dirs):
        csvs = glob.glob(os.path.join(server_dir, "*.csv"))
        if not csvs:
            print(f"  WARNING: No CSV in {server_dir}")
            continue

        # Take the latest run by filename timestamp if present.
        latest = _pick_latest_csv(csvs)
        merged_servers += 1
        print(f"  Reading: {latest}")

        with open(latest, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue

                label = _cell(row, 0)
                if label in ("pclock", "NO_DATA", ""):
                    continue

                # Per-batch rows have numeric pclock in column 0.
                try:
                    int(label)
                    lat = _parse_float(_cell(row, 1))
                    if lat is not None and lat > 0:
                        all_latencies.append(lat)
                    continue
                except ValueError:
                    pass

                # Summary rows - extract metrics from server CSV format
                if label == "AVERAGE":
                    avg_lat = _parse_float(_cell(row, 1))
                    if avg_lat is not None:
                        summary_avg_latencies.append(avg_lat)
                elif label == "THROUGHPUT":
                    tpt = _parse_float(_cell(row, 2))
                    if tpt is not None:
                        all_throughputs.append(tpt)
                elif label == "P50_LATENCY":
                    lat = _parse_float(_cell(row, 1))
                    if lat is not None:
                        summary_p50_latencies.append(lat)
                elif label == "P95_LATENCY":
                    lat = _parse_float(_cell(row, 1))
                    if lat is not None:
                        summary_p95_latencies.append(lat)
                elif label == "P99_LATENCY":
                    lat = _parse_float(_cell(row, 1))
                    if lat is not None:
                        summary_p99_latencies.append(lat)
                elif label == "AVG_FAST_PATH_LATENCY":
                    lat = _parse_float(_cell(row, 1))
                    count = _parse_int(_cell(row, 3))
                    if lat is not None:
                        fast_latencies.extend([lat] * (count if count and count > 0 else 1))
                elif label == "AVG_SLOW_PATH_LATENCY":
                    lat = _parse_float(_cell(row, 1))
                    count = _parse_int(_cell(row, 4))
                    if lat is not None:
                        slow_latencies.extend([lat] * (count if count and count > 0 else 1))
                elif label == "GLOBAL_TOTALS":
                    # Server format: "GLOBAL_TOTALS", "", "", "fast ops", "slow ops", "conflict ops"
                    fast_str = _cell(row, 3)
                    slow_str = _cell(row, 4)
                    conflict_str = _cell(row, 5)
                    
                    fast_val = _parse_int(fast_str)
                    slow_val = _parse_int(slow_str)
                    conflict_val = _parse_int(conflict_str)
                    
                    if fast_val is not None:
                        total_fast += fast_val
                    if slow_val is not None:
                        total_slow += slow_val
                    if conflict_val is not None:
                        total_conflict += conflict_val
                elif label == "TOTAL_FAST_PATH_FALLBACKS":
                    val = _parse_int(_cell(row, 1))
                    if val is not None:
                        total_fallbacks += val

    output_path = _resolve_output(eval_dir, output, "servers")
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    if all_latencies:
        all_latencies.sort()
        n = len(all_latencies)
        p50 = all_latencies[n * 50 // 100]
        p95 = all_latencies[n * 95 // 100]
        p99 = all_latencies[n * 99 // 100]
        avg_lat = sum(all_latencies) / n
    elif summary_avg_latencies:
        avg_lat = sum(summary_avg_latencies) / len(summary_avg_latencies)
        p50 = sum(summary_p50_latencies) / len(summary_p50_latencies) if summary_p50_latencies else avg_lat
        p95 = sum(summary_p95_latencies) / len(summary_p95_latencies) if summary_p95_latencies else p50
        p99 = sum(summary_p99_latencies) / len(summary_p99_latencies) if summary_p99_latencies else p95
        print("  WARNING: No per-batch latency rows found; using summary latency rows from server CSVs")
    else:
        raise RuntimeError("No latency data found in server CSV files")

    # Sum per-server wall-clock throughput values into aggregate throughput.
    total_tpt = sum(all_throughputs)

    avg_fast_lat = sum(fast_latencies) / len(fast_latencies) if fast_latencies else 0.0
    avg_slow_lat = sum(slow_latencies) / len(slow_latencies) if slow_latencies else 0.0

    total_ops = total_fast + total_slow + total_conflict
    fast_ratio = (total_fast / total_ops) if total_ops > 0 else 0.0
    slow_ratio = (total_slow / total_ops) if total_ops > 0 else 0.0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value", "notes"])
        writer.writerow(["NUM_SERVER_DIRS", len(server_dirs), "server* folders under eval"])
        writer.writerow(["NUM_SERVERS_MERGED", merged_servers, "servers with CSV data"])
        writer.writerow(["TOTAL_THROUGHPUT", f"{total_tpt:.1f} Tx/sec", "sum of all servers"])
        writer.writerow(["AVG_LATENCY", f"{avg_lat:.3f} ms", "across all batches all servers"])
        writer.writerow(["P50_LATENCY", f"{p50:.1f} ms", ""])
        writer.writerow(["P95_LATENCY", f"{p95:.1f} ms", ""])
        writer.writerow(["P99_LATENCY", f"{p99:.1f} ms", ""])
        writer.writerow(["AVG_FAST_PATH_LATENCY", f"{avg_fast_lat:.3f} ms", ""])
        writer.writerow(["AVG_SLOW_PATH_LATENCY", f"{avg_slow_lat:.3f} ms", ""])
        writer.writerow(["TOTAL_FAST_COMMITS", total_fast, f"{fast_ratio * 100:.1f}%"])
        writer.writerow(["TOTAL_SLOW_COMMITS", total_slow, f"{slow_ratio * 100:.1f}%"])
        writer.writerow(["TOTAL_CONFLICT_COMMITS", total_conflict, ""])
        writer.writerow(["TOTAL_FAST_PATH_FALLBACKS", total_fallbacks, ""])

    print(f"\nMerged server CSV written to: {output_path}")
    print(f"  Servers merged: {merged_servers}/{len(server_dirs)}")
    print(f"  Total throughput: {total_tpt:.1f} Tx/sec")
    print(f"  Avg latency: {avg_lat:.3f} ms")
    print(f"  P99 latency: {p99:.1f} ms")
    print(f"  Fast/Slow ratio: {fast_ratio * 100:.1f}% / {slow_ratio * 100:.1f}%")

    return output_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Merge all latest per-client or per-server WOC evaluation CSV files into a summary CSV"
        )
    )
    parser.add_argument(
        "eval_dir",
        nargs="?",
        default="./eval",
        help="Path to eval directory containing client*/ or server*/ folders (default: ./eval)",
    )
    parser.add_argument(
        "output",
        nargs="?",
        default=None,
        help=(
            "Output CSV file path or output directory. "
            "Default: ./eval/merged/merged_woc_<type>_<timestamp>.csv"
        ),
    )
    parser.add_argument(
        "--servers",
        action="store_true",
        help="Merge server CSVs instead of client CSVs",
    )
    parser.add_argument(
        "--ids",
        default=None,
        help=(
            "Optional comma/range filter for directory IDs to merge. "
            "Examples: '0,1,2' or '0-10'. "
            "For client mode this targets client<ID>; for --servers it targets server<ID>."
        ),
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    try:
        allowed_ids = _parse_id_filter(args.ids)
        if args.servers:
            merge_server_csvs(
                eval_dir=args.eval_dir,
                output=args.output,
                allowed_server_ids=allowed_ids,
            )
        else:
            merge_client_csvs(
                eval_dir=args.eval_dir,
                output=args.output,
                allowed_client_ids=allowed_ids,
            )
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
