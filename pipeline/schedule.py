"""Schedule-window abstraction.

A ScheduleWindow defines the date range a given daily/weekly report run
covers. Filtering events into a window is a pure function of
(events, window): re-running the same window over the same input always
returns an equal subset, so downstream aggregation is idempotent — running
a scheduled report twice for the same window neither drops nor
double-counts events.

This module does not persist any "already ran this window" state; see
README Limitations for what a production scheduler would still need.
"""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Dict, List

from .util import format_date, parse_date, week_start


@dataclass(frozen=True)
class ScheduleWindow:
    period_type: str  # "day" or "week"
    start: _dt.date  # inclusive
    end: _dt.date  # inclusive
    label: str

    def contains(self, d: _dt.date) -> bool:
        return self.start <= d <= self.end


def daily_window(target_date: _dt.date) -> ScheduleWindow:
    return ScheduleWindow(
        period_type="day",
        start=target_date,
        end=target_date,
        label=format_date(target_date),
    )


def weekly_window(any_date_in_week: _dt.date) -> ScheduleWindow:
    start = week_start(any_date_in_week)
    end = start + _dt.timedelta(days=6)
    return ScheduleWindow(
        period_type="week",
        start=start,
        end=end,
        label=f"{format_date(start)}_to_{format_date(end)}",
    )


def events_in_window(events: List[Dict], window: ScheduleWindow) -> List[Dict]:
    """Return only the events whose date falls inside window (inclusive on
    both ends). Pure/idempotent: calling this twice with the same inputs
    returns an equal list every time; calling it on non-overlapping windows
    never returns the same event twice.
    """
    out = []
    for ev in events:
        d = parse_date(ev["date"])
        if window.contains(d):
            out.append(ev)
    return out
