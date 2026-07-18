"""Read and validate the raw event/transaction log (CSV or JSON array).

Validates presence and type of required fields before anything downstream
touches the data. No network access; reads only the local file given.
"""
from __future__ import annotations

import csv
import json
import os
from typing import Dict, List

from .util import PipelineError, parse_date

REQUIRED_FIELDS = ["event_id", "timestamp", "date", "category", "channel", "user_id", "amount"]
VALID_CATEGORIES = {"purchase", "refund", "signup", "support_ticket"}


def _coerce_row(raw: Dict[str, str], line_no: int) -> Dict:
    missing = [f for f in REQUIRED_FIELDS if f not in raw or raw[f] in (None, "")]
    if missing:
        raise PipelineError(f"row {line_no}: missing required field(s) {missing}")

    row = dict(raw)
    parse_date(row["date"])  # validates format, raises PipelineError if bad
    if row["category"] not in VALID_CATEGORIES:
        raise PipelineError(
            f"row {line_no}: unknown category {row['category']!r}; "
            f"expected one of {sorted(VALID_CATEGORIES)}"
        )
    try:
        row["amount"] = float(row["amount"])
    except (TypeError, ValueError) as exc:
        raise PipelineError(f"row {line_no}: amount {row['amount']!r} is not numeric") from exc
    return row


def read_events_csv(path: str) -> List[Dict]:
    if not os.path.exists(path):
        raise PipelineError(f"events file not found: {path}")
    events = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for i, raw in enumerate(reader, start=2):  # header occupies line 1
            events.append(_coerce_row(raw, i))
    return events


def read_events_json(path: str) -> List[Dict]:
    if not os.path.exists(path):
        raise PipelineError(f"events file not found: {path}")
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise PipelineError("JSON events file must contain a top-level array")
    return [_coerce_row(raw, i) for i, raw in enumerate(data, start=1)]


def read_events(path: str) -> List[Dict]:
    """Dispatch on file extension (.csv or .json)."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return read_events_csv(path)
    if ext == ".json":
        return read_events_json(path)
    raise PipelineError(f"unsupported events file extension: {ext!r}")
