"""Assemble the Markdown report body and the alerts CSV.

No timestamps, run IDs, or other non-deterministic content are embedded
anywhere in the output: every value in the report is derived from the
input event data itself, so the same input always produces a
byte-identical report.
"""
from __future__ import annotations

import csv
from typing import Dict, List

ALERT_FIELDS = [
    "rule", "kind", "metric", "group", "period", "measured_value",
    "threshold_value", "comparator", "trailing_mean", "trailing_stdev",
    "z_score", "message",
]


def _fmt(value):
    return "n/a" if value is None else value


def build_markdown_report(
    title: str,
    window_label: str,
    delta_rows: List[Dict],
    alerts: List[Dict],
) -> str:
    lines = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append(f"Window: `{window_label}`")
    lines.append("")
    lines.append(
        "This report is generated from event/transaction data only. Figures "
        "below are measured directly from the input log; anything under "
        "'Alerts' is a rule- or statistics-based flag for a human to review, "
        "not a diagnosis of cause."
    )
    lines.append("")
    lines.append("## Metrics by group")
    lines.append("")
    lines.append(
        "| period | group | count | sum_amount | avg_amount | distinct_users | "
        "count_delta | count_pct_change | count_direction |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for row in sorted(delta_rows, key=lambda r: (r["period"], r["group"])):
        lines.append(
            "| {period} | {group} | {count} | {sum_amount} | {avg_amount} | "
            "{distinct_users} | {count_delta} | {count_pct_change} | {count_direction} |".format(
                period=row["period"],
                group=row["group"],
                count=row["count"],
                sum_amount=row["sum_amount"],
                avg_amount=row["avg_amount"],
                distinct_users=row["distinct_users"],
                count_delta=_fmt(row.get("count_delta")),
                count_pct_change=_fmt(row.get("count_pct_change")),
                count_direction=row.get("count_direction", "n/a"),
            )
        )
    lines.append("")
    lines.append("## Alerts")
    lines.append("")
    if not alerts:
        lines.append("No threshold or statistical-outlier rules fired for this window.")
    else:
        lines.append("| rule | kind | metric | group | period | measured_value | detail |")
        lines.append("|---|---|---|---|---|---|---|")
        for a in sorted(alerts, key=lambda x: (x["period"], x["group"], x["rule"])):
            detail_bits = []
            if a.get("threshold_value") is not None:
                detail_bits.append(f"threshold={a['threshold_value']} ({a['comparator']})")
            if a.get("z_score") is not None:
                detail_bits.append(
                    f"trailing_mean={a['trailing_mean']} trailing_stdev={a['trailing_stdev']} z={a['z_score']}"
                )
            detail = "; ".join(detail_bits)
            lines.append(
                f"| {a['rule']} | {a['kind']} | {a['metric']} | {a['group']} | "
                f"{a['period']} | {a['measured_value']} | {detail} |"
            )
    lines.append("")
    lines.append(
        "_Alerts are threshold/statistical flags for human review, not root-cause "
        "diagnoses. Outlier detection is a trailing-window mean/stdev heuristic, "
        "not a trained model._"
    )
    lines.append("")
    return "\n".join(lines)


def write_alerts_csv(alerts: List[Dict], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=ALERT_FIELDS)
        writer.writeheader()
        for a in sorted(alerts, key=lambda x: (x["period"], x["group"], x["rule"])):
            row = {field: a.get(field, "") for field in ALERT_FIELDS}
            writer.writerow(row)
