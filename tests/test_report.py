"""Test suite for the scheduled-analytics-report pipeline.

Covers: aggregation correctness, delta math, threshold + statistical
outlier alert firing (and non-firing) on the synthetic dataset's planted
anomalies, idempotent re-runs, schedule window boundaries, output
determinism (md5-stable), and the invariant that every alert carries its
triggering rule and measured value.

Run from the project root:
    python3 -m unittest discover -s tests -v
"""
from __future__ import annotations

import csv
import datetime as _dt
import hashlib
import os
import shutil
import tempfile
import unittest

import run_demo
from data.generate_events import (
    DRIFT_START,
    DROP_DATE,
    SPIKE_DATE,
    generate_events,
)
from pipeline.aggregate import aggregate_events
from pipeline.alerts import check_threshold_rules, check_trailing_outliers, default_rules
from pipeline.deltas import compute_deltas
from pipeline.ingest import PipelineError, read_events_csv
from pipeline.report import build_markdown_report, write_alerts_csv
from pipeline.schedule import daily_window, events_in_window, weekly_window
from pipeline.util import format_date, parse_date, period_key, week_start

# A day far from the start-of-series ramp-up and from any planted anomaly:
# a full 7-day trailing window is available and clean.
NORMAL_DAY = "2024-01-11"

FIXTURE_EVENTS = [
    {"event_id": "e1", "timestamp": "2024-01-01T10:00:00", "date": "2024-01-01",
     "category": "purchase", "channel": "web", "user_id": "u1", "amount": 10.0},
    {"event_id": "e2", "timestamp": "2024-01-01T11:00:00", "date": "2024-01-01",
     "category": "purchase", "channel": "web", "user_id": "u2", "amount": 30.0},
    {"event_id": "e3", "timestamp": "2024-01-01T12:00:00", "date": "2024-01-01",
     "category": "signup", "channel": "mobile", "user_id": "u3", "amount": 0.0},
    {"event_id": "e4", "timestamp": "2024-01-02T09:00:00", "date": "2024-01-02",
     "category": "purchase", "channel": "web", "user_id": "u1", "amount": 50.0},
    {"event_id": "e5", "timestamp": "2024-01-08T09:00:00", "date": "2024-01-08",
     "category": "purchase", "channel": "web", "user_id": "u4", "amount": 20.0},
]


class AggregationTests(unittest.TestCase):
    def test_counts_sums_averages_distinct(self):
        rows = aggregate_events(FIXTURE_EVENTS, period_type="day", dimension="category")
        row = next(r for r in rows if r["period"] == "2024-01-01" and r["group"] == "purchase")
        self.assertEqual(row["count"], 2)
        self.assertEqual(row["sum_amount"], 40.0)
        self.assertEqual(row["avg_amount"], 20.0)
        self.assertEqual(row["distinct_users"], 2)

    def test_distinct_users_counts_unique_only(self):
        rows = aggregate_events(FIXTURE_EVENTS, period_type="week", dimension="category")
        # u1 buys on both 2024-01-01 and 2024-01-02, same ISO week -> distinct count 2 (u1, u2)
        row = next(r for r in rows if r["group"] == "purchase" and r["period"] == "2024-01-01")
        self.assertEqual(row["distinct_users"], 2)

    def test_week_grouping_buckets_by_monday_start(self):
        rows = aggregate_events(FIXTURE_EVENTS, period_type="week", dimension="category")
        periods = sorted({r["period"] for r in rows})
        # 2024-01-01..02 fall in the week starting 2024-01-01 (a Monday);
        # 2024-01-08 starts a new week.
        self.assertIn("2024-01-01", periods)
        self.assertIn("2024-01-08", periods)

    def test_aggregate_by_alternate_dimension(self):
        rows = aggregate_events(FIXTURE_EVENTS, period_type="day", dimension="channel")
        row = next(r for r in rows if r["period"] == "2024-01-01" and r["group"] == "web")
        self.assertEqual(row["count"], 2)

    def test_unknown_period_type_raises(self):
        with self.assertRaises(PipelineError):
            aggregate_events(FIXTURE_EVENTS, period_type="month")


class DeltaTests(unittest.TestCase):
    def test_first_period_has_no_previous(self):
        rows = aggregate_events(FIXTURE_EVENTS, period_type="day", dimension="category")
        deltas = compute_deltas(rows)
        first = next(r for r in deltas if r["group"] == "purchase" and r["period"] == "2024-01-01")
        self.assertIsNone(first["count_delta"])
        self.assertIsNone(first["count_pct_change"])
        self.assertEqual(first["count_direction"], "n/a")

    def test_delta_math_is_correct(self):
        rows = aggregate_events(FIXTURE_EVENTS, period_type="day", dimension="category")
        deltas = compute_deltas(rows)
        # purchase: day1 count=2 sum=40.0, day2 count=1 sum=50.0
        day2 = next(r for r in deltas if r["group"] == "purchase" and r["period"] == "2024-01-02")
        self.assertEqual(day2["count_delta"], -1)
        self.assertAlmostEqual(day2["count_pct_change"], -50.0)
        self.assertEqual(day2["count_direction"], "down")
        self.assertEqual(day2["sum_amount_delta"], 10.0)
        self.assertEqual(day2["sum_amount_direction"], "up")

    def test_delta_direction_flat_when_equal(self):
        rows = [
            {"period": "2024-01-01", "period_type": "day", "group": "x", "count": 5,
             "sum_amount": 10.0, "avg_amount": 2.0, "distinct_users": 3},
            {"period": "2024-01-02", "period_type": "day", "group": "x", "count": 5,
             "sum_amount": 10.0, "avg_amount": 2.0, "distinct_users": 3},
        ]
        deltas = compute_deltas(rows)
        second = deltas[1]
        self.assertEqual(second["count_delta"], 0)
        self.assertEqual(second["count_direction"], "flat")

    def test_delta_handles_zero_previous_value_without_crashing(self):
        rows = [
            {"period": "2024-01-01", "period_type": "day", "group": "x", "count": 0,
             "sum_amount": 0.0, "avg_amount": 0.0, "distinct_users": 0},
            {"period": "2024-01-02", "period_type": "day", "group": "x", "count": 5,
             "sum_amount": 10.0, "avg_amount": 2.0, "distinct_users": 3},
        ]
        deltas = compute_deltas(rows)
        second = deltas[1]
        self.assertEqual(second["count_delta"], 5)
        self.assertIsNone(second["count_pct_change"])  # division by zero -> None, not a crash
        self.assertEqual(second["count_direction"], "up")


class ThresholdAlertTests(unittest.TestCase):
    def test_low_threshold_fires(self):
        rows = [{"period": "2024-01-01", "group": "purchase", "count": 50}]
        rules = [{"name": "low_test", "metric": "count", "group": "purchase", "comparator": "lt", "value": 100}]
        alerts = check_threshold_rules(rows, rules)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["rule"], "low_test")
        self.assertEqual(alerts[0]["measured_value"], 50)

    def test_high_threshold_fires(self):
        rows = [{"period": "2024-01-01", "group": "purchase", "count": 900}]
        rules = [{"name": "high_test", "metric": "count", "group": "purchase", "comparator": "gt", "value": 700}]
        alerts = check_threshold_rules(rows, rules)
        self.assertEqual(len(alerts), 1)

    def test_threshold_does_not_fire_within_bounds(self):
        rows = [{"period": "2024-01-01", "group": "purchase", "count": 300}]
        rules = default_rules("day")
        alerts = check_threshold_rules(rows, rules)
        self.assertEqual(alerts, [])

    def test_threshold_respects_group_filter(self):
        rows = [{"period": "2024-01-01", "group": "signup", "count": 5}]
        rules = [{"name": "low_test", "metric": "count", "group": "purchase", "comparator": "lt", "value": 100}]
        alerts = check_threshold_rules(rows, rules)
        self.assertEqual(alerts, [])  # rule only targets the "purchase" group


class SyntheticDatasetAlertTests(unittest.TestCase):
    """Uses the real generated dataset to check that alerts fire on planted
    anomalies and stay silent on an ordinary day."""

    @classmethod
    def setUpClass(cls):
        cls.events = generate_events()
        cls.daily_agg = aggregate_events(cls.events, period_type="day", dimension="category")
        cls.weekly_agg = aggregate_events(cls.events, period_type="week", dimension="category")
        cls.daily_deltas = compute_deltas(cls.daily_agg)

    def test_spike_day_purchase_count_far_above_baseline(self):
        spike_row = next(r for r in self.daily_agg if r["period"] == format_date(SPIKE_DATE) and r["group"] == "purchase")
        normal_row = next(r for r in self.daily_agg if r["period"] == NORMAL_DAY and r["group"] == "purchase")
        self.assertGreater(spike_row["count"], normal_row["count"] * 2)

    def test_drop_day_purchase_count_far_below_baseline(self):
        drop_row = next(r for r in self.daily_agg if r["period"] == format_date(DROP_DATE) and r["group"] == "purchase")
        normal_row = next(r for r in self.daily_agg if r["period"] == NORMAL_DAY and r["group"] == "purchase")
        self.assertLess(drop_row["count"], normal_row["count"] * 0.5)

    def test_threshold_alert_fires_on_spike_day(self):
        alerts = check_threshold_rules(self.daily_deltas, default_rules("day"))
        spike_alerts = [a for a in alerts if a["period"] == format_date(SPIKE_DATE)]
        self.assertTrue(any(a["rule"] == "high_daily_purchase_volume" for a in spike_alerts))

    def test_threshold_alert_fires_on_drop_day(self):
        alerts = check_threshold_rules(self.daily_deltas, default_rules("day"))
        drop_alerts = [a for a in alerts if a["period"] == format_date(DROP_DATE)]
        self.assertTrue(any(a["rule"] == "low_daily_purchase_volume" for a in drop_alerts))

    def test_threshold_alert_silent_on_normal_day(self):
        alerts = check_threshold_rules(self.daily_deltas, default_rules("day"))
        normal_alerts = [a for a in alerts if a["period"] == NORMAL_DAY]
        self.assertEqual(normal_alerts, [])

    def test_outlier_alert_fires_on_spike_day(self):
        outliers = check_trailing_outliers(self.daily_agg, metric="count", group="purchase")
        self.assertTrue(any(o["period"] == format_date(SPIKE_DATE) for o in outliers))

    def test_outlier_alert_fires_on_drop_day(self):
        outliers = check_trailing_outliers(self.daily_agg, metric="count", group="purchase")
        self.assertTrue(any(o["period"] == format_date(DROP_DATE) for o in outliers))

    def test_outlier_alert_silent_on_normal_day(self):
        outliers_count = check_trailing_outliers(self.daily_agg, metric="count", group="purchase")
        outliers_amount = check_trailing_outliers(self.daily_agg, metric="avg_amount", group="purchase")
        self.assertFalse(any(o["period"] == NORMAL_DAY for o in outliers_count))
        self.assertFalse(any(o["period"] == NORMAL_DAY for o in outliers_amount))

    def test_weekly_threshold_fires_for_spike_week(self):
        weekly_deltas = compute_deltas(self.weekly_agg)
        alerts = check_threshold_rules(weekly_deltas, default_rules("week"))
        spike_week_label = format_date(week_start(SPIKE_DATE))
        self.assertTrue(any(
            a["period"] == spike_week_label and a["rule"] == "high_weekly_purchase_volume" for a in alerts
        ))

    def test_weekly_aggregation_can_dilute_a_single_bad_day(self):
        # Honest limitation: the drop day is a single bad day inside a week
        # of otherwise-normal days, so the *weekly* total does not
        # necessarily cross the low-volume threshold even though the
        # *daily* one clearly does. This documents that weekly rollups can
        # mask a single-day anomaly.
        weekly_deltas = compute_deltas(self.weekly_agg)
        alerts = check_threshold_rules(weekly_deltas, default_rules("week"))
        drop_week_label = format_date(week_start(DROP_DATE))
        low_fired = any(
            a["period"] == drop_week_label and a["rule"] == "low_weekly_purchase_volume" for a in alerts
        )
        self.assertFalse(low_fired)

    def test_slow_drift_shows_up_as_consecutive_up_deltas(self):
        drift_rows = [
            r for r in self.daily_deltas
            if r["group"] == "purchase" and parse_date(r["period"]) >= DRIFT_START
        ]
        drift_rows.sort(key=lambda r: r["period"])
        # Not every single day of a noisy drift need be "up", but the
        # majority should be, which is what a human scanning deltas would
        # notice even if no single-day alert fires.
        up_days = sum(1 for r in drift_rows if r["avg_amount_direction"] == "up")
        self.assertGreaterEqual(up_days, len(drift_rows) // 2)


class AlertShapeTests(unittest.TestCase):
    def test_threshold_alerts_always_carry_rule_and_measured_value(self):
        rows = [{"period": "2024-01-01", "group": "purchase", "count": 50}]
        rules = [{"name": "low_test", "metric": "count", "group": "purchase", "comparator": "lt", "value": 100}]
        alerts = check_threshold_rules(rows, rules)
        for a in alerts:
            self.assertIn("rule", a)
            self.assertIn("measured_value", a)
            self.assertIsNotNone(a["rule"])
            self.assertIsNotNone(a["measured_value"])

    def test_outlier_alerts_always_carry_rule_and_measured_value(self):
        events = generate_events()
        agg = aggregate_events(events, period_type="day", dimension="category")
        outliers = check_trailing_outliers(agg, metric="count", group="purchase")
        self.assertTrue(outliers, "expected at least one outlier in the synthetic dataset")
        for a in outliers:
            self.assertIn("rule", a)
            self.assertIn("measured_value", a)
            self.assertTrue(a["rule"].startswith("trailing_outlier_"))


class ScheduleWindowTests(unittest.TestCase):
    def test_daily_window_boundaries(self):
        d = _dt.date(2024, 1, 15)
        window = daily_window(d)
        self.assertTrue(window.contains(d))
        self.assertFalse(window.contains(d - _dt.timedelta(days=1)))
        self.assertFalse(window.contains(d + _dt.timedelta(days=1)))

    def test_weekly_window_boundaries_inclusive_monday_to_sunday(self):
        window = weekly_window(_dt.date(2024, 1, 17))  # a Wednesday
        self.assertEqual(window.start, _dt.date(2024, 1, 15))  # Monday
        self.assertEqual(window.end, _dt.date(2024, 1, 21))  # Sunday
        self.assertTrue(window.contains(_dt.date(2024, 1, 15)))
        self.assertTrue(window.contains(_dt.date(2024, 1, 21)))
        self.assertFalse(window.contains(_dt.date(2024, 1, 14)))
        self.assertFalse(window.contains(_dt.date(2024, 1, 22)))

    def test_events_in_window_is_idempotent(self):
        window = daily_window(SPIKE_DATE)
        first = events_in_window(FIXTURE_EVENTS + [
            {"event_id": "espike", "timestamp": "2024-01-15T00:00:00", "date": "2024-01-15",
             "category": "purchase", "channel": "web", "user_id": "u9", "amount": 5.0},
        ], window)
        second = events_in_window(FIXTURE_EVENTS + [
            {"event_id": "espike", "timestamp": "2024-01-15T00:00:00", "date": "2024-01-15",
             "category": "purchase", "channel": "web", "user_id": "u9", "amount": 5.0},
        ], window)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 1)

    def test_events_in_window_matches_full_history_partition(self):
        # Filtering to a window and then aggregating should give the exact
        # same numbers as aggregating everything and reading off that
        # period -- i.e. windowing does not drop or double count events.
        events = generate_events()
        window = daily_window(SPIKE_DATE)
        windowed = events_in_window(events, window)
        agg_from_window = aggregate_events(windowed, period_type="day", dimension="category")
        agg_from_all = aggregate_events(events, period_type="day", dimension="category")
        spike_from_window = sorted(agg_from_window, key=lambda r: (r["period"], r["group"]))
        spike_from_all = [r for r in agg_from_all if r["period"] == format_date(SPIKE_DATE)]
        spike_from_all_sorted = sorted(spike_from_all, key=lambda r: (r["period"], r["group"]))
        self.assertEqual(spike_from_window, spike_from_all_sorted)


class GeneratorDeterminismTests(unittest.TestCase):
    def test_generate_events_is_deterministic(self):
        first = generate_events()
        second = generate_events()
        self.assertEqual(first, second)

    def test_generate_events_nonempty_and_schema_shaped(self):
        events = generate_events()
        self.assertGreater(len(events), 1000)
        required = {"event_id", "timestamp", "date", "category", "channel", "user_id", "amount"}
        self.assertTrue(required.issubset(events[0].keys()))


class IngestTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_csv(self, rows, fieldnames):
        path = os.path.join(self.tmpdir, "events.csv")
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        return path

    def test_read_valid_csv_roundtrip(self):
        fieldnames = ["event_id", "timestamp", "date", "category", "channel", "user_id", "amount"]
        rows = [{"event_id": "e1", "timestamp": "2024-01-01T00:00:00", "date": "2024-01-01",
                 "category": "purchase", "channel": "web", "user_id": "u1", "amount": "12.50"}]
        path = self._write_csv(rows, fieldnames)
        events = read_events_csv(path)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["amount"], 12.5)
        self.assertIsInstance(events[0]["amount"], float)

    def test_missing_field_raises(self):
        fieldnames = ["event_id", "timestamp", "date", "category", "channel", "user_id"]  # amount missing
        rows = [{"event_id": "e1", "timestamp": "2024-01-01T00:00:00", "date": "2024-01-01",
                 "category": "purchase", "channel": "web", "user_id": "u1"}]
        path = self._write_csv(rows, fieldnames)
        with self.assertRaises(PipelineError):
            read_events_csv(path)

    def test_invalid_category_raises(self):
        fieldnames = ["event_id", "timestamp", "date", "category", "channel", "user_id", "amount"]
        rows = [{"event_id": "e1", "timestamp": "2024-01-01T00:00:00", "date": "2024-01-01",
                 "category": "not_a_category", "channel": "web", "user_id": "u1", "amount": "1.0"}]
        path = self._write_csv(rows, fieldnames)
        with self.assertRaises(PipelineError):
            read_events_csv(path)

    def test_invalid_amount_raises(self):
        fieldnames = ["event_id", "timestamp", "date", "category", "channel", "user_id", "amount"]
        rows = [{"event_id": "e1", "timestamp": "2024-01-01T00:00:00", "date": "2024-01-01",
                 "category": "purchase", "channel": "web", "user_id": "u1", "amount": "not-a-number"}]
        path = self._write_csv(rows, fieldnames)
        with self.assertRaises(PipelineError):
            read_events_csv(path)

    def test_missing_file_raises(self):
        with self.assertRaises(PipelineError):
            read_events_csv(os.path.join(self.tmpdir, "does_not_exist.csv"))


class ReportBuildIdempotenceTests(unittest.TestCase):
    def test_build_report_same_window_twice_is_identical(self):
        events = generate_events()
        window = daily_window(SPIKE_DATE)
        md1, alerts1 = run_demo.build_report(events, window)
        md2, alerts2 = run_demo.build_report(events, window)
        self.assertEqual(md1, md2)
        self.assertEqual(alerts1, alerts2)

    def test_markdown_report_has_no_alerts_placeholder_when_empty(self):
        md = build_markdown_report("Test Report", "2024-01-01", [], [])
        self.assertIn("No threshold or statistical-outlier rules fired", md)

    def test_write_alerts_csv_header_matches_fields(self):
        path = os.path.join(tempfile.mkdtemp(), "alerts.csv")
        write_alerts_csv([], path)
        with open(path, newline="", encoding="utf-8") as fh:
            header = next(csv.reader(fh))
        self.assertIn("rule", header)
        self.assertIn("measured_value", header)


class DemoDeterminismTests(unittest.TestCase):
    """Runs the real run_demo.py entrypoint twice and checks the four
    output files are byte-identical (md5-stable) across runs."""

    def test_sample_output_is_md5_stable_across_two_runs(self):
        run_demo.main()
        first_hashes = self._hash_outputs()
        run_demo.main()
        second_hashes = self._hash_outputs()
        self.assertEqual(first_hashes, second_hashes)

    @staticmethod
    def _hash_outputs():
        names = ["daily_report.md", "weekly_report.md", "alerts.csv", "run_summary.txt"]
        hashes = {}
        for name in names:
            path = os.path.join(run_demo.SAMPLE_OUTPUT_DIR, name)
            with open(path, "rb") as fh:
                hashes[name] = hashlib.md5(fh.read()).hexdigest()
        return hashes


if __name__ == "__main__":
    unittest.main()
