# Scheduled Analytics Reporting (sample #2)

A code sample for a "Data Analytics" freelance offer. Sample #1 is a
one-shot analytics pipeline; this one adds the **scheduling + alerting +
delta** dimension: it turns a raw event/transaction log into a periodic
(daily/weekly) report with metric aggregations, period-over-period deltas,
threshold/statistical alert flags, and idempotent re-runs.

This is a positioning sample from a data/automation engineer, not a claim
of a production-grade analytics platform. Everything it reports is
measured directly from the input log; anything framed as a judgment
("this looks anomalous") is explicitly hedged as a rule- or
statistics-based flag for a human to confirm.

## What it does

1. **`pipeline/ingest.py`** reads a CSV or JSON event log and validates
   required fields/types before anything downstream touches it.
2. **`pipeline/aggregate.py`** groups events by period (day/week) and a
   dimension (category or channel), computing count, sum, average, and
   distinct user count.
3. **`pipeline/deltas.py`** computes period-over-period change (absolute,
   percent, direction) per group.
4. **`pipeline/alerts.py`** checks two independent kinds of rules:
   - **threshold rules** (e.g. daily purchase count below/above a fixed
     number), and
   - **statistical outlier flags** (current value vs. a trailing-window
     mean/stdev, z-score based).

   Every alert carries the rule name, the metric, the group/period it
   applies to, and the measured value — never a bare "something's wrong".
5. **`pipeline/report.py`** assembles a Markdown report and an alerts CSV.
   No wall-clock timestamps or run IDs are embedded anywhere; the report
   is a pure function of the input data.
6. **`pipeline/schedule.py`** defines daily/weekly report windows and
   filters events into them. Filtering is a pure function of
   `(events, window)`, so re-running the same window against the same
   input file always produces the same subset of events and therefore the
   same report — no double counting from re-running a schedule.
7. **`pipeline/run.py`** is a small CLI orchestrator tying the above
   together for a single window.

## Demo dataset

`data/generate_events.py` deterministically generates six weeks
(2024-01-01 .. 2024-02-11) of synthetic purchase/refund/signup/support
events (seeded `random.Random`, no wall-clock dependency), with three
planted anomalies so the alerting logic has real signal to catch:

- **Spike day** (2024-01-15): purchase volume ~3x normal.
- **Drop day** (2024-01-29): purchase volume ~0.2x normal.
- **Slow drift** (2024-02-02 .. 2024-02-11): average purchase amount
  creeps up ~3%/day, cumulative — a change visible in the deltas as a run
  of consecutive "up" days, but not reliably caught by a single-day
  threshold or the short trailing-window outlier check.

## Running it

```bash
cd analytics-scheduled-report
python3 run_demo.py
```

This generates `data/events.csv`, runs a daily report (for the spike day)
and a weekly report (for the week containing it), and writes
`sample_output/daily_report.md`, `sample_output/weekly_report.md`,
`sample_output/alerts.csv`, and `sample_output/run_summary.txt`.

Running it twice produces byte-identical files in `sample_output/`
(verified by a dedicated md5 test).

To run a report for an arbitrary date via the CLI:

```bash
python3 -m pipeline.run --events data/events.csv --period day --date 2024-01-29 --out /tmp/out
python3 -m pipeline.run --events data/events.csv --period week --date 2024-01-29 --out /tmp/out
```

## Tests

```bash
python3 -m unittest discover -s tests -v
```

20+ tests covering aggregation correctness, delta math, threshold and
statistical-outlier alerts firing on the planted anomalies (and *not*
firing on an ordinary day), idempotent re-runs, schedule-window
boundaries, ingest validation, and output determinism (md5-stable).

## Docker

```bash
docker compose up --build
```

The `analysis` service runs with `network_mode: none` — the demo needs no
network access, which the compose file enforces rather than just asserts.

## No network, no API keys

The core pipeline, the demo, and the tests are pure Python 3 standard
library (`csv`, `json`, `datetime`, `statistics`, `argparse`,
`dataclasses`, `unittest`, `hashlib`). There is no optional LLM/OpenAI
mode in this sample — it isn't needed for what this pipeline does
(aggregation, deltas, threshold/statistical checks are deterministic
arithmetic, not something an LLM call would improve), so none was added
just to have one.

## Limitations

This sample is intentionally scoped. It does **not**:

- **Diagnose root causes.** Alerts are threshold/statistical flags for a
  human to confirm and investigate — a "high_daily_purchase_volume" alert
  says the count crossed a number, not why.
- **Use a trained anomaly-detection model.** The outlier check is a
  trailing mean/standard-deviation heuristic over the preceding N periods
  (default: 7 days, minimum 5 periods of history required before a period
  is even checked). It is not seasonality-aware beyond whatever the window
  size happens to capture, and it will not catch anomalies smaller than
  the trailing noise floor.
- **Guarantee weekly alerts catch daily anomalies.** A single very bad day
  inside an otherwise-normal week can leave the *weekly* total inside
  normal bounds even though the *daily* total clearly is not — this
  sample's own test suite demonstrates this with the planted drop day
  (see `test_weekly_aggregation_can_dilute_a_single_bad_day`). Rolling up
  to a coarser period is a real tradeoff, not a bug, but it means weekly
  reports alone are not a substitute for daily monitoring.
- **Persist run/lock state.** `schedule.py`'s idempotency guarantee is
  about pure re-computation: the same `(events file, window)` always
  yields the same report. It does not implement a run-log, lock, or
  "already ran this window" datastore — a production scheduler (cron,
  Airflow, etc.) would still need one to avoid launching two overlapping
  runs concurrently.
- **Deduplicate or reconcile events.** `event_id` uniqueness, duplicate
  submissions, and out-of-order/late-arriving events are not detected or
  corrected. `ingest.py` validates required fields and types only.
- **Handle timezones.** All dates/timestamps are treated as naive local
  values; no timezone conversion or DST handling is performed.
- **Validate at production scale.** The pipeline holds all events for a
  window in memory (Python lists/dicts) — fine for this sample's ~40k
  synthetic events, not sized for a real high-volume event stream without
  further work (chunking, a real datastore, etc.).
- **Calibrate thresholds from real business data.** The threshold
  constants in `pipeline/alerts.py::default_rules` are illustrative,
  tuned to this sample's synthetic dataset's scale — not derived from any
  real SLA or business target.

## Repo layout

```
pipeline/       ingest, aggregate, deltas, alerts, report, schedule, run, util
data/           generate_events.py (synthetic data), events.csv (generated)
tests/          test_report.py
sample_output/  generated by run_demo.py
run_demo.py     end-to-end demo entrypoint
SCHEMA.md        event log + intermediate row schemas
```
