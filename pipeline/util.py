"""Shared helpers: date parsing, period-key computation, and small statistics
utilities used across the pipeline. Stdlib only, no I/O, no network.
"""
from __future__ import annotations

import datetime as _dt
import statistics as _statistics
from typing import Iterable, List, Optional

DATE_FMT = "%Y-%m-%d"


class PipelineError(Exception):
    """Base class for pipeline-raised errors (ingest/validation/etc.)."""


def parse_date(value: str) -> _dt.date:
    """Parse a 'YYYY-MM-DD' string into a date. Raises PipelineError on bad input."""
    try:
        return _dt.datetime.strptime(value.strip(), DATE_FMT).date()
    except (ValueError, AttributeError) as exc:
        raise PipelineError(f"invalid date value: {value!r}") from exc


def format_date(d: _dt.date) -> str:
    return d.strftime(DATE_FMT)


def week_start(d: _dt.date) -> _dt.date:
    """Return the Monday that starts the ISO week containing d."""
    return d - _dt.timedelta(days=d.weekday())


def period_key(d: _dt.date, period_type: str) -> str:
    """Return the string key identifying the period a date belongs to."""
    if period_type == "day":
        return format_date(d)
    if period_type == "week":
        return format_date(week_start(d))
    raise PipelineError(f"unknown period_type: {period_type!r}")


def daterange(start: _dt.date, end: _dt.date) -> Iterable[_dt.date]:
    """Yield each date from start to end, inclusive."""
    days = (end - start).days
    for i in range(days + 1):
        yield start + _dt.timedelta(days=i)


def mean(values: List[float]) -> float:
    return _statistics.mean(values) if values else 0.0


def pstdev(values: List[float]) -> float:
    return _statistics.pstdev(values) if len(values) > 1 else 0.0


def safe_div(numerator: float, denominator: float) -> Optional[float]:
    if denominator == 0:
        return None
    return numerator / denominator


def round2(value: float) -> float:
    """Deterministic 2-decimal rounding; normalizes -0.0 to 0.0."""
    r = round(value, 2)
    if r == 0:
        r = 0.0
    return r
