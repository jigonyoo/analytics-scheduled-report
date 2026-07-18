# Event/Transaction Log Schema

The pipeline ingests a flat event/transaction log, one row per event. Both
CSV and JSON (a top-level JSON array of objects with these same keys) are
supported by `pipeline/ingest.py`.

## Required fields

| field       | type              | notes |
|-------------|-------------------|-------|
| `event_id`  | string            | Expected unique per event. Uniqueness is **not** enforced by `ingest.py` (see Limitations). |
| `timestamp` | string, ISO 8601  | `YYYY-MM-DDTHH:MM:SS`, naive (no timezone). Informational only — aggregation buckets on `date`, not `timestamp`. |
| `date`      | string, `YYYY-MM-DD` | The calendar date the event is attributed to. This is what `aggregate.py` and `schedule.py` bucket on. |
| `category`  | string, one of `purchase`, `refund`, `signup`, `support_ticket` | The event type. Used as the default aggregation dimension. |
| `channel`   | string, e.g. `web`, `mobile`, `api`, `partner` | Alternate aggregation dimension. |
| `user_id`   | string            | Used for the `distinct_users` metric. |
| `amount`    | numeric (string in CSV, number in JSON) | Monetary value. `purchase` amounts are positive, `refund` amounts are negative, `signup`/`support_ticket` amounts are `0.0` in the synthetic generator (a real feed might have non-zero amounts for other categories — the schema does not forbid it). |

## Validation performed by `ingest.py`

- All required fields must be present and non-empty.
- `date` must parse as `YYYY-MM-DD`.
- `category` must be one of the four known values.
- `amount` must parse as a float.

Anything else (well-formed `channel`/`user_id`/`event_id` values, exact
`timestamp` format, event ordering) is **not** validated — see the
README's Limitations section.

## Aggregated row shape (`aggregate.py` output)

```
{
  "period": "2024-01-15",       # day (YYYY-MM-DD) or week (Monday YYYY-MM-DD)
  "period_type": "day" | "week",
  "group": "purchase",           # the dimension value (category or channel)
  "count": 812,
  "sum_amount": 33021.44,
  "avg_amount": 40.67,
  "distinct_users": 501
}
```

## Delta row shape (`deltas.py` output)

Adds, for each metric in `count`, `sum_amount`, `avg_amount`, `distinct_users`:
`<metric>_delta`, `<metric>_pct_change`, `<metric>_direction` (`"up"` /
`"down"` / `"flat"` / `"n/a"` for a group's first observed period).

## Alert row shape (`alerts.py` output)

```
{
  "rule": "high_daily_purchase_volume",
  "kind": "threshold" | "statistical_outlier",
  "metric": "count",
  "group": "purchase",
  "period": "2024-01-15",
  "measured_value": 927,
  "threshold_value": 700,        # threshold alerts only
  "comparator": "gt",            # threshold alerts only
  "trailing_mean": 301.4,        # statistical_outlier alerts only
  "trailing_stdev": 22.1,        # statistical_outlier alerts only
  "z_score": 28.3,               # statistical_outlier alerts only
  "message": "human-readable summary"
}
```

Every alert always has `rule` and `measured_value` populated — there is no
"something's wrong" alert without a named rule and a number attached to it.
