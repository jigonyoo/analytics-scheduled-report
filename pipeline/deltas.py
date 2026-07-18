"""Period-over-period deltas.

For each (group), periods are sorted chronologically and each period gets
the absolute change, percent change, and a direction label ("up"/"down"/
"flat") relative to the immediately preceding period for that group. The
first period seen for a group has no predecessor, so its delta fields are
None / "n/a" rather than a fabricated number.
"""
from __future__ import annotations

from typing import Dict, List

from .util import round2, safe_div

METRIC_FIELDS = ["count", "sum_amount", "avg_amount", "distinct_users"]


def compute_deltas(agg_rows: List[Dict]) -> List[Dict]:
    by_group: Dict[str, List[Dict]] = {}
    for row in agg_rows:
        by_group.setdefault(row["group"], []).append(row)

    out = []
    for group, rows in by_group.items():
        rows_sorted = sorted(rows, key=lambda r: r["period"])
        prev = None
        for row in rows_sorted:
            delta_row = dict(row)
            for metric in METRIC_FIELDS:
                if prev is None:
                    delta_row[f"{metric}_delta"] = None
                    delta_row[f"{metric}_pct_change"] = None
                    delta_row[f"{metric}_direction"] = "n/a"
                else:
                    cur_val = row[metric]
                    prev_val = prev[metric]
                    delta = cur_val - prev_val
                    pct = safe_div(delta, prev_val)
                    delta_row[f"{metric}_delta"] = round2(delta)
                    delta_row[f"{metric}_pct_change"] = round2(pct * 100) if pct is not None else None
                    if delta > 0:
                        direction = "up"
                    elif delta < 0:
                        direction = "down"
                    else:
                        direction = "flat"
                    delta_row[f"{metric}_direction"] = direction
            out.append(delta_row)
            prev = row
    out.sort(key=lambda r: (r["period"], r["group"]))
    return out
