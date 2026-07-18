#!/usr/bin/env python3
"""Run a daily and a weekly scheduled analytics report over the synthetic
demo dataset and write the results to sample_output/.

No network access, no API keys, and no wall-clock timestamps in the
output: running this script twice produces byte-identical files in
sample_output/ (see tests/test_report.py for an md5-based check).

This script only ever creates directories (os.makedirs(..., exist_ok=True))
and overwrites files via open(path, "w") — it never deletes the output
directory or its contents, so it works on filesystems that allow writes
but block deletion.
"""
from __future__ import annotations

import os

from data.generate_events import DRIFT_END, DRIFT_START, DROP_DATE, SPIKE_DATE, write_events_csv
from pipeline.aggregate import aggregate_events
from pipeline.alerts import check_threshold_rules, check_trailing_outliers, default_rules
from pipeline.deltas import compute_deltas
from pipeline.ingest import read_events_csv
from pipeline.report import build_markdown_report, write_alerts_csv
from pipeline.schedule import ScheduleWindow, daily_window, weekly_window
from pipeline.util import format_date

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "data")
EVENTS_CSV = os.path.join(DATA_DIR, "events.csv")
SAMPLE_OUTPUT_DIR = os.path.join(ROOT_DIR, "sample_output")

# Chosen so the demo actually exercises alerting: the spike day (and the
# week it falls in) reliably crosses the illustrative thresholds/outlier
# rules defined in pipeline/alerts.py.
DAILY_REPORT_DATE = SPIKE_DATE
WEEKLY_REPORT_DATE = SPIKE_DATE


def build_report(events, window: ScheduleWindow, dimension: str = "category"):
    """Build (markdown, alerts) for a single schedule window. Pure function
    of (events, window) -> re-running with the same arguments returns an
    identical result (idempotent, no double counting)."""
    history_agg = aggregate_events(events, period_type=window.period_type, dimension=dimension)
    window_periods = {window.label} if window.period_type == "day" else {format_date(window.start)}

    delta_rows_all = compute_deltas(history_agg)
    delta_rows_window = [r for r in delta_rows_all if r["period"] in window_periods]

    rules = default_rules(window.period_type)
    threshold_alerts = check_threshold_rules(delta_rows_window, rules)

    outlier_alerts = []
    if window.period_type == "day":
        # Daily cadence has enough history (42 days) for a 7-day trailing
        # window; the 6-point weekly series is too short for a meaningful
        # trailing mean/stdev, so weekly outlier checks are skipped here
        # (see README Limitations).
        for metric in ("count", "avg_amount"):
            all_outliers = check_trailing_outliers(
                history_agg, metric=metric, group="purchase", window=7, min_periods=5, z_threshold=2.5
            )
            outlier_alerts.extend(a for a in all_outliers if a["period"] in window_periods)

    alerts = threshold_alerts + outlier_alerts
    title = "Daily Analytics Report" if window.period_type == "day" else "Weekly Analytics Report"
    md = build_markdown_report(title, window.label, delta_rows_window, alerts)
    return md, alerts


def main():
    n_events = write_events_csv(EVENTS_CSV)
    events = read_events_csv(EVENTS_CSV)

    os.makedirs(SAMPLE_OUTPUT_DIR, exist_ok=True)

    daily_win = daily_window(DAILY_REPORT_DATE)
    weekly_win = weekly_window(WEEKLY_REPORT_DATE)

    daily_md, daily_alerts = build_report(events, daily_win)
    weekly_md, weekly_alerts = build_report(events, weekly_win)

    with open(os.path.join(SAMPLE_OUTPUT_DIR, "daily_report.md"), "w", encoding="utf-8") as fh:
        fh.write(daily_md)
    with open(os.path.join(SAMPLE_OUTPUT_DIR, "weekly_report.md"), "w", encoding="utf-8") as fh:
        fh.write(weekly_md)

    all_alerts = daily_alerts + weekly_alerts
    write_alerts_csv(all_alerts, os.path.join(SAMPLE_OUTPUT_DIR, "alerts.csv"))

    dates = sorted({e["date"] for e in events})
    summary_lines = [
        "Scheduled Analytics Report - run summary",
        "=========================================",
        f"Events ingested: {n_events}",
        f"Date range covered: {dates[0]} to {dates[-1]} ({len(dates)} distinct days)",
        f"Daily report window: {daily_win.label}",
        f"Weekly report window: {weekly_win.label}",
        f"Alerts fired (daily report): {len(daily_alerts)}",
        f"Alerts fired (weekly report): {len(weekly_alerts)}",
        f"Alerts fired (total): {len(all_alerts)}",
        "",
        "Planted anomalies in the synthetic dataset (see data/generate_events.py):",
        f"  - spike day: {SPIKE_DATE}",
        f"  - drop day: {DROP_DATE}",
        f"  - slow drift window: {DRIFT_START} to {DRIFT_END}",
        "",
        "Output files:",
        "  - daily_report.md",
        "  - weekly_report.md",
        "  - alerts.csv",
        "  - run_summary.txt (this file)",
        "",
    ]
    with open(os.path.join(SAMPLE_OUTPUT_DIR, "run_summary.txt"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(summary_lines))

    print(f"Wrote sample_output/ with {len(all_alerts)} total alert(s).")


if __name__ == "__main__":
    main()
