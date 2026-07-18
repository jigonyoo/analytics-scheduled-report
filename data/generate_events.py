"""Deterministically generate a synthetic multi-week event/transaction log
for the scheduled-report demo, with a few planted anomalies so the alerting
pipeline has real signal to catch:

  * a spike day  (2024-01-15, a Monday): purchase volume ~3x normal,
    e.g. a promo or marketing push.
  * a drop day   (2024-01-29, a Monday): purchase volume ~0.2x normal,
    e.g. an outage or payment-provider incident.
  * a slow drift (2024-02-02 .. 2024-02-11): average purchase amount
    creeps up ~3%/day, cumulative. This is the kind of change a single-day
    threshold or a short trailing window will NOT reliably catch as a
    one-day outlier, but it shows up clearly as a run of consecutive
    "up" deltas.

Everything here is seeded (random.Random(SEED)), and the code path taken
for a given day/event never depends on wall-clock time, so the output is
byte-identical across runs and machines.
"""
from __future__ import annotations

import csv
import datetime as _dt
import os
import random
from typing import List

SEED = 20240101
START_DATE = _dt.date(2024, 1, 1)  # a Monday
NUM_DAYS = 42  # 6 full ISO weeks: 2024-01-01 .. 2024-02-11

SPIKE_DATE = _dt.date(2024, 1, 15)
DROP_DATE = _dt.date(2024, 1, 29)
DRIFT_START = _dt.date(2024, 2, 2)
DRIFT_END = _dt.date(2024, 2, 11)
DRIFT_DAILY_GROWTH = 0.03

CATEGORY_WEIGHTS = [
    ("purchase", 0.50),
    ("signup", 0.20),
    ("support_ticket", 0.25),
    ("refund", 0.05),
]
CHANNELS = ["web", "mobile", "api", "partner"]
NUM_USERS = 500

FIELDNAMES = ["event_id", "timestamp", "date", "category", "channel", "user_id", "amount"]


def _weighted_category(rng: random.Random) -> str:
    r = rng.random()
    cum = 0.0
    for cat, w in CATEGORY_WEIGHTS:
        cum += w
        if r <= cum:
            return cat
    return CATEGORY_WEIGHTS[-1][0]


def _daily_base_count(d: _dt.date) -> int:
    return 600 if d.weekday() < 5 else 350  # Mon-Fri busier than weekends


def _drift_multiplier(d: _dt.date) -> float:
    if d < DRIFT_START:
        return 1.0
    day_index = (min(d, DRIFT_END) - DRIFT_START).days
    return (1.0 + DRIFT_DAILY_GROWTH) ** day_index


def generate_events() -> List[dict]:
    """Return the full synthetic event list, deterministic for a fixed SEED."""
    rng = random.Random(SEED)
    events = []
    event_seq = 0
    for offset in range(NUM_DAYS):
        d = START_DATE + _dt.timedelta(days=offset)
        base = _daily_base_count(d)
        jitter = rng.uniform(0.92, 1.08)
        count = int(round(base * jitter))
        if d == SPIKE_DATE:
            count = int(round(count * 3.0))
        elif d == DROP_DATE:
            count = int(round(count * 0.2))

        drift_mult = _drift_multiplier(d)

        for _ in range(count):
            event_seq += 1
            category = _weighted_category(rng)
            channel = rng.choice(CHANNELS)
            user_id = f"u{rng.randint(1, NUM_USERS):04d}"
            hour = rng.randint(0, 23)
            minute = rng.randint(0, 59)
            second = rng.randint(0, 59)
            ts = _dt.datetime.combine(d, _dt.time(hour, minute, second))

            if category == "purchase":
                amount = rng.uniform(15.0, 90.0) * drift_mult
            elif category == "refund":
                amount = -rng.uniform(10.0, 60.0)
            else:
                amount = 0.0

            events.append(
                {
                    "event_id": f"e{event_seq:07d}",
                    "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S"),
                    "date": d.strftime("%Y-%m-%d"),
                    "category": category,
                    "channel": channel,
                    "user_id": user_id,
                    "amount": round(amount, 2),
                }
            )
    events.sort(key=lambda e: (e["date"], e["timestamp"], e["event_id"]))
    return events


def write_events_csv(path: str) -> int:
    """Generate events and write them to a CSV file at `path`. Returns the
    number of events written. Overwrites any existing file at `path`
    (never deletes the directory)."""
    events = generate_events()
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        for ev in events:
            writer.writerow(ev)
    return len(events)


if __name__ == "__main__":
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "events.csv")
    n = write_events_csv(out_path)
    print(f"wrote {n} events to {out_path}")
