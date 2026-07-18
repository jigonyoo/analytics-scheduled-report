"""Group events by period (day/week) and a dimension, computing count, sum,
average, and distinct-user counts. Pure functions over in-memory lists; no
I/O and no hidden state.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from .util import PipelineError, parse_date, period_key, round2


def aggregate_events(
    events: List[Dict],
    period_type: str = "day",
    dimension: str = "category",
) -> List[Dict]:
    """Aggregate events into (period, group) rows.

    Returns a list of dicts sorted by (period, group) with keys:
      period, period_type, group, count, sum_amount, avg_amount, distinct_users
    """
    if period_type not in ("day", "week"):
        raise PipelineError(f"unknown period_type: {period_type!r}")

    buckets: Dict[tuple, Dict] = {}
    for ev in events:
        if dimension not in ev:
            raise PipelineError(f"dimension {dimension!r} not present on event {ev.get('event_id')}")
        d = parse_date(ev["date"])
        pkey = period_key(d, period_type)
        gkey = ev[dimension]
        key = (pkey, gkey)
        bucket = buckets.setdefault(key, {"count": 0, "sum_amount": 0.0, "users": set()})
        bucket["count"] += 1
        bucket["sum_amount"] += float(ev["amount"])
        bucket["users"].add(ev["user_id"])

    rows = []
    for (pkey, gkey), b in buckets.items():
        count = b["count"]
        rows.append(
            {
                "period": pkey,
                "period_type": period_type,
                "group": gkey,
                "count": count,
                "sum_amount": round2(b["sum_amount"]),
                "avg_amount": round2(b["sum_amount"] / count) if count else 0.0,
                "distinct_users": len(b["users"]),
            }
        )
    rows.sort(key=lambda r: (r["period"], r["group"]))
    return rows


def filter_rows(rows: List[Dict], group: Optional[str] = None) -> List[Dict]:
    if group is None:
        return rows
    return [r for r in rows if r["group"] == group]
