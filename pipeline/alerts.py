"""Threshold and simple statistical-outlier alert rules.

Every alert dict always carries: rule (name), kind, metric, group, period,
and measured_value, plus a human-readable message describing what was
measured and which rule fired. Alerts are flags for a human to confirm —
they say what changed and which rule it crossed, not why it changed.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from .util import PipelineError, mean, pstdev, round2


def check_threshold_rules(agg_rows: List[Dict], rules: List[Dict]) -> List[Dict]:
    """rules: list of dicts with keys: name, metric, group (optional filter),
    comparator ('lt' | 'gt'), value.
    """
    alerts = []
    for rule in rules:
        metric = rule["metric"]
        comparator = rule["comparator"]
        threshold = rule["value"]
        target_group = rule.get("group")
        for row in agg_rows:
            if target_group is not None and row["group"] != target_group:
                continue
            measured = row.get(metric)
            if measured is None:
                continue
            fired = (measured < threshold) if comparator == "lt" else (measured > threshold)
            if fired:
                alerts.append(
                    {
                        "rule": rule["name"],
                        "kind": "threshold",
                        "metric": metric,
                        "group": row["group"],
                        "period": row["period"],
                        "measured_value": measured,
                        "threshold_value": threshold,
                        "comparator": comparator,
                        "message": (
                            f"{rule['name']}: {metric}={measured} for group={row['group']} "
                            f"period={row['period']} {comparator} threshold {threshold}"
                        ),
                    }
                )
    alerts.sort(key=lambda a: (a["period"], a["group"], a["rule"]))
    return alerts


def check_trailing_outliers(
    agg_rows: List[Dict],
    metric: str,
    group: Optional[str] = None,
    window: int = 7,
    min_periods: int = 5,
    z_threshold: float = 2.5,
) -> List[Dict]:
    """Flag a period as a statistical outlier if its value is more than
    z_threshold trailing standard deviations from the trailing mean of the
    preceding `window` periods (same group, chronological order, current
    period excluded from its own baseline).

    Requires at least min_periods of trailing history with non-zero spread;
    periods without enough history are skipped (not flagged) rather than
    guessed at. This is a simple trailing-window heuristic, not a trained
    model, and it does not model seasonality beyond what the window size
    happens to capture.
    """
    rows = [r for r in agg_rows if group is None or r["group"] == group]
    by_group: Dict[str, List[Dict]] = {}
    for row in rows:
        by_group.setdefault(row["group"], []).append(row)

    alerts = []
    for g, grows in by_group.items():
        grows_sorted = sorted(grows, key=lambda r: r["period"])
        history: List[float] = []
        for row in grows_sorted:
            value = row[metric]
            trailing = history[-window:]
            if len(trailing) >= min_periods:
                m = mean(trailing)
                sd = pstdev(trailing)
                if sd > 0:
                    z = (value - m) / sd
                    if abs(z) >= z_threshold:
                        alerts.append(
                            {
                                "rule": f"trailing_outlier_{metric}",
                                "kind": "statistical_outlier",
                                "metric": metric,
                                "group": g,
                                "period": row["period"],
                                "measured_value": value,
                                "trailing_mean": round2(m),
                                "trailing_stdev": round2(sd),
                                "z_score": round2(z),
                                "message": (
                                    f"trailing_outlier_{metric}: {metric}={value} for group={g} "
                                    f"period={row['period']} vs trailing mean={round2(m)} "
                                    f"stdev={round2(sd)} (z={round2(z)}, threshold={z_threshold})"
                                ),
                            }
                        )
            history.append(value)
    alerts.sort(key=lambda a: (a["period"], a["group"], a["rule"]))
    return alerts


def default_rules(period_type: str) -> List[Dict]:
    """Illustrative threshold constants tuned to the scale of this sample's
    synthetic dataset. A real deployment would derive these from actual
    business context/SLAs, not hardcode them.
    """
    if period_type == "day":
        return [
            {
                "name": "low_daily_purchase_volume",
                "metric": "count",
                "group": "purchase",
                "comparator": "lt",
                "value": 120,
            },
            {
                "name": "high_daily_purchase_volume",
                "metric": "count",
                "group": "purchase",
                "comparator": "gt",
                "value": 700,
            },
        ]
    if period_type == "week":
        return [
            {
                "name": "low_weekly_purchase_volume",
                "metric": "count",
                "group": "purchase",
                "comparator": "lt",
                "value": 1400,
            },
            {
                "name": "high_weekly_purchase_volume",
                "metric": "count",
                "group": "purchase",
                "comparator": "gt",
                "value": 2200,
            },
        ]
    raise PipelineError(f"no default rules for period_type: {period_type!r}")
