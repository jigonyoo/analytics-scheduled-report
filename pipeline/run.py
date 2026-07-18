"""CLI orchestrator: run a daily or weekly scheduled report from an events
file and write the Markdown report + alerts CSV to an output directory.

Usage:
    python3 -m pipeline.run --events data/events.csv --period day \
        --date 2024-01-15 --out sample_output
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Tuple

from .aggregate import aggregate_events
from .alerts import check_threshold_rules, check_trailing_outliers, default_rules
from .deltas import compute_deltas
from .ingest import read_events
from .report import build_markdown_report, write_alerts_csv
from .schedule import ScheduleWindow, daily_window, weekly_window
from .util import format_date, parse_date


def run_report(
    events: List[Dict],
    window: ScheduleWindow,
    dimension: str = "category",
    title: str = "Scheduled Analytics Report",
) -> Tuple[str, List[Dict]]:
    """Build a report for a single schedule window.

    Aggregates ALL events at the window's period_type/dimension (so deltas
    and trailing-outlier checks have visibility into prior periods), then
    reports only on the period(s) that fall inside the window. Pure
    function of (events, window): calling it twice with the same arguments
    returns identical results (idempotent, no double counting).
    """
    history_agg = aggregate_events(events, period_type=window.period_type, dimension=dimension)
    if window.period_type == "day":
        window_periods = {window.label}
    else:
        window_periods = {format_date(window.start)}

    delta_rows_all = compute_deltas(history_agg)
    delta_rows_window = [r for r in delta_rows_all if r["period"] in window_periods]

    rules = default_rules(window.period_type)
    threshold_alerts = check_threshold_rules(delta_rows_window, rules)

    outlier_alerts = []
    if window.period_type == "day":
        for metric in ("count", "avg_amount"):
            all_outliers = check_trailing_outliers(
                history_agg, metric=metric, group="purchase", window=7, min_periods=5, z_threshold=2.5
            )
            outlier_alerts.extend(a for a in all_outliers if a["period"] in window_periods)

    alerts = threshold_alerts + outlier_alerts
    md = build_markdown_report(title, window.label, delta_rows_window, alerts)
    return md, alerts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Run a scheduled analytics report.")
    parser.add_argument("--events", required=True, help="path to events CSV/JSON")
    parser.add_argument("--period", choices=["day", "week"], required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD date inside the target window")
    parser.add_argument("--dimension", default="category")
    parser.add_argument("--out", required=True, help="output directory")
    args = parser.parse_args(argv)

    events = read_events(args.events)
    target_date = parse_date(args.date)
    window = daily_window(target_date) if args.period == "day" else weekly_window(target_date)

    title = f"{'Daily' if args.period == 'day' else 'Weekly'} Analytics Report"
    md, alerts = run_report(events, window, dimension=args.dimension, title=title)

    os.makedirs(args.out, exist_ok=True)
    report_name = "daily_report.md" if args.period == "day" else "weekly_report.md"
    with open(os.path.join(args.out, report_name), "w", encoding="utf-8") as fh:
        fh.write(md)
    write_alerts_csv(alerts, os.path.join(args.out, "alerts.csv"))
    print(f"wrote {report_name} with {len(alerts)} alert(s) to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
