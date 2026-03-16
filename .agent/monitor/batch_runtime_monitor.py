#!/usr/bin/env python3
"""Minute-level runtime monitor for the live batch workload."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import statistics
import subprocess
import time
from collections import Counter, defaultdict
from pathlib import Path

import httpx


KEYWORD_PATTERNS = {
    "proxy_429": re.compile(r"CONNECT tunnel failed, response 429"),
    "blank_page": re.compile(r"\[页面为空\]"),
    "blocked": re.compile(r"被封 HTTP|Session 被封锁|代理被封"),
    "request_timeout": re.compile(r"请求超时"),
    "task_failed": re.compile(r"采集失败"),
    "session_init_failed": re.compile(r"Session 初始化失败"),
    "traceback": re.compile(r"Traceback|Exception"),
}

NOTABLE_TOKENS = (
    "ERROR",
    "WARNING",
    "Traceback",
    "Exception",
    "CONNECT tunnel failed",
    "Session 被封锁",
    "代理被封",
    "采集失败",
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def http_get_json(client: httpx.Client, url: str):
    resp = client.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def read_log_delta(path: Path, offset: int) -> tuple[str, int]:
    if not path.exists():
        return "", offset
    size = path.stat().st_size
    if size < offset:
        offset = 0
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        fh.seek(offset)
        text = fh.read()
        return text, fh.tell()


def analyze_log_chunk(text: str) -> tuple[dict[str, int], list[str]]:
    counts = {name: len(pattern.findall(text)) for name, pattern in KEYWORD_PATTERNS.items()}
    notable = []
    for line in text.splitlines():
        if any(token in line for token in NOTABLE_TOKENS):
            notable.append(line[-500:])
    return counts, notable[:50]


def get_process_snapshot(server_url: str) -> list[dict[str, str]]:
    cmd = (
        "ps -Ao pid,ppid,pgid,%cpu,%mem,etime,command | "
        f"rg 'run_server.py|run_worker.py --server {re.escape(server_url)}|worker/screenshot.py {re.escape(server_url)}'"
    )
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False)
    rows: list[dict[str, str]] = []
    for line in proc.stdout.splitlines():
        parts = line.split(None, 6)
        if len(parts) != 7:
            continue
        pid, ppid, pgid, cpu, mem, etime, command = parts
        rows.append(
            {
                "pid": pid,
                "ppid": ppid,
                "pgid": pgid,
                "cpu": cpu,
                "mem": mem,
                "etime": etime,
                "command": command,
            }
        )
    return rows


def summarize_workers(workers: list[dict]) -> dict:
    online = [w for w in workers if w.get("online")]
    return {
        "online_count": len(online),
        "worker_ids": [w.get("worker_id") for w in workers],
        "avg_success_rate": round(
            statistics.mean([w["success_rate"] for w in online if isinstance(w.get("success_rate"), (int, float))]),
            4,
        )
        if any(isinstance(w.get("success_rate"), (int, float)) for w in online)
        else None,
        "avg_block_rate": round(
            statistics.mean([w["block_rate"] for w in online if isinstance(w.get("block_rate"), (int, float))]),
            4,
        )
        if any(isinstance(w.get("block_rate"), (int, float)) for w in online)
        else None,
    }


def build_report(
    batch: dict,
    snapshots: list[dict],
    cumulative_log_counts: dict[str, int],
    notable_events: list[dict],
    report_file: Path,
    state: dict,
) -> None:
    batch_name = batch["name"]
    started_at = snapshots[0]["ts"] if snapshots else utc_now()
    ended_at = snapshots[-1]["ts"] if snapshots else utc_now()

    deltas = []
    prev = None
    for snap in snapshots:
        done = snap["batch"]["completed"]
        if prev is not None:
            deltas.append(max(done - prev, 0))
        prev = done

    avg_per_min = round(statistics.mean(deltas), 2) if deltas else 0
    peak_per_min = max(deltas) if deltas else 0
    avg_workers = round(statistics.mean([snap["workers_summary"]["online_count"] for snap in snapshots]), 2) if snapshots else 0
    peak_worker_cpu = max(
        [float(p["cpu"]) for snap in snapshots for p in snap["processes"] if "run_worker.py" in p["command"]],
        default=0.0,
    )
    peak_server_cpu = max(
        [float(p["cpu"]) for snap in snapshots for p in snap["processes"] if "run_server.py" in p["command"]],
        default=0.0,
    )

    top_events = Counter()
    for item in notable_events:
        top_events[item["source"]] += 1

    lines = [
        "# F-005 Runtime Observation Report",
        "",
        "## Target",
        "",
        f"- Batch: `{batch_name}`",
        f"- Batch id: `{batch['id']}`",
        f"- Total tasks: `{batch['total_tasks']}`",
        f"- Monitor started: `{started_at}`",
        f"- Monitor ended: `{ended_at}`",
        f"- Snapshot count: `{len(snapshots)}`",
        "",
        "## Outcome",
        "",
        f"- Final progress: `completed={batch['completed']} failed={batch['failed']} pending={batch['pending']} processing={batch['processing']}`",
        f"- Average completed per minute: `{avg_per_min}`",
        f"- Peak completed per minute: `{peak_per_min}`",
        f"- Average online workers observed: `{avg_workers}`",
        f"- Peak worker CPU (single process): `{peak_worker_cpu}%`",
        f"- Peak server CPU: `{peak_server_cpu}%`",
        "",
        "## Stability Summary",
        "",
        f"- Cumulative `429` proxy errors: `{cumulative_log_counts.get('proxy_429', 0)}`",
        f"- Cumulative blank-page retries: `{cumulative_log_counts.get('blank_page', 0)}`",
        f"- Cumulative block detections: `{cumulative_log_counts.get('blocked', 0)}`",
        f"- Cumulative request timeouts: `{cumulative_log_counts.get('request_timeout', 0)}`",
        f"- Cumulative terminal task failures: `{cumulative_log_counts.get('task_failed', 0)}`",
        f"- Cumulative session init failures: `{cumulative_log_counts.get('session_init_failed', 0)}`",
        "",
        "## Suspected Issues",
        "",
    ]

    issues = []
    if cumulative_log_counts.get("proxy_429", 0):
        issues.append("Proxy-side `429` bursts were sustained and were the most common source of startup instability and retries.")
    if cumulative_log_counts.get("blank_page", 0):
        issues.append("Workers continued to encounter `[页面为空]` retries under load, which suggests either upstream throttling or HTML readiness problems still surface during high concurrency.")
    if cumulative_log_counts.get("blocked", 0):
        issues.append("Block detection and session rotation remained active during the run, so throughput depends on how quickly the proxy/session pool recovers after a bad burst.")
    if state.get("worker_registry_mismatch"):
        issues.append("`/api/workers` reported `enable_screenshot=true` for workers launched with `--no-screenshot`, so the worker registry is likely misreporting runtime configuration.")
    if not issues:
        issues.append("No new systemic instability signatures were detected beyond normal retry noise.")

    for item in issues:
        lines.append(f"- {item}")

    lines.extend(
        [
            "",
            "## Notable Events",
            "",
        ]
    )

    for item in notable_events[-25:]:
        lines.append(f"- `{item['ts']}` `{item['source']}`: {item['line']}")

    if len(notable_events) == 0:
        lines.append("- none captured")

    lines.extend(
        [
            "",
            "## Artifacts",
            "",
            f"- Snapshot log: `{state['snapshot_file']}`",
            f"- State file: `{state['state_file']}`",
            f"- Runtime logs: `{state['log_dir']}`",
            "",
        ]
    )

    report_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-url", required=True)
    parser.add_argument("--batch-id", type=int, required=True)
    parser.add_argument("--batch-name", required=True)
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--log-dir", required=True)
    parser.add_argument("--snapshot-file", required=True)
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--report-file", required=True)
    parser.add_argument("--expected-workers", default="")
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    snapshot_file = Path(args.snapshot_file)
    state_file = Path(args.state_file)
    report_file = Path(args.report_file)
    expected_workers = [x for x in args.expected_workers.split(",") if x]

    state = load_json(
        state_file,
        {
            "started_at": utc_now(),
            "offsets": {},
            "cumulative_log_counts": defaultdict(int),
            "worker_registry_mismatch": False,
            "snapshot_file": str(snapshot_file),
            "state_file": str(state_file),
            "log_dir": str(log_dir),
        },
    )
    offsets = {k: int(v) for k, v in state.get("offsets", {}).items()}
    cumulative_log_counts = Counter(state.get("cumulative_log_counts", {}))
    snapshot_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.parent.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    with httpx.Client() as client:
        while True:
            ts = utc_now()
            try:
                batches = http_get_json(client, f"{args.server_url}/api/batches")["batches"]
                workers = http_get_json(client, f"{args.server_url}/api/workers")["workers"]
                progress = http_get_json(client, f"{args.server_url}/api/progress")
            except Exception as exc:
                error_snapshot = {"ts": ts, "kind": "monitor_error", "error": repr(exc)}
                with snapshot_file.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(error_snapshot, ensure_ascii=False) + "\n")
                time.sleep(args.interval)
                continue

            batch = next((b for b in batches if b["id"] == args.batch_id or b["name"] == args.batch_name), None)
            if batch is None:
                error_snapshot = {"ts": ts, "kind": "missing_batch", "batch_id": args.batch_id, "batch_name": args.batch_name}
                with snapshot_file.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(error_snapshot, ensure_ascii=False) + "\n")
                time.sleep(args.interval)
                continue

            log_counts_this_round = Counter()
            notable_events = []
            for log_path in sorted(log_dir.glob("worker_stage*.log")):
                text, new_offset = read_log_delta(log_path, offsets.get(log_path.name, 0))
                offsets[log_path.name] = new_offset
                if not text:
                    continue
                counts, notable = analyze_log_chunk(text)
                log_counts_this_round.update(counts)
                cumulative_log_counts.update(counts)
                for line in notable:
                    notable_events.append({"ts": ts, "source": log_path.name, "line": line})

            worker_map = {w["worker_id"]: w for w in workers}
            for wid in expected_workers:
                worker = worker_map.get(wid)
                if worker and wid != "loadtest-stage1" and worker.get("enable_screenshot") is True:
                    state["worker_registry_mismatch"] = True

            processes = get_process_snapshot(args.server_url)
            snapshot = {
                "ts": ts,
                "batch": batch,
                "overall_progress": progress,
                "workers": workers,
                "workers_summary": summarize_workers(workers),
                "processes": processes,
                "log_counts_this_round": dict(log_counts_this_round),
                "notable_events": notable_events,
            }
            with snapshot_file.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(snapshot, ensure_ascii=False) + "\n")

            state["offsets"] = offsets
            state["cumulative_log_counts"] = dict(cumulative_log_counts)
            state["last_snapshot_at"] = ts
            state["last_completed"] = batch["completed"]
            state["last_pending"] = batch["pending"]
            state["last_processing"] = batch["processing"]
            save_json(state_file, state)

            if batch["pending"] == 0 and batch["processing"] == 0:
                with snapshot_file.open("r", encoding="utf-8") as fh:
                    snapshots = [json.loads(line) for line in fh if line.strip() and json.loads(line).get("kind") is None]
                all_notable = []
                for snap in snapshots:
                    all_notable.extend(snap.get("notable_events", []))
                build_report(batch, snapshots, dict(cumulative_log_counts), all_notable, report_file, state)
                break

            time.sleep(args.interval)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
