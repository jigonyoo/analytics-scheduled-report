# Weekly Analytics Report

Window: `2024-01-15_to_2024-01-21`

This report is generated from event/transaction data only. Figures below are measured directly from the input log; anything under 'Alerts' is a rule- or statistics-based flag for a human to review, not a diagnosis of cause.

## Metrics by group

| period | group | count | sum_amount | avg_amount | distinct_users | count_delta | count_pct_change | count_direction |
|---|---|---|---|---|---|---|---|---|
| 2024-01-15 | purchase | 2558 | 133883.2 | 52.34 | 495 | 642 | 33.51 | up |
| 2024-01-15 | refund | 271 | -9749.39 | -35.98 | 204 | 62 | 29.67 | up |
| 2024-01-15 | signup | 1012 | 0.0 | 0.0 | 425 | 262 | 34.93 | up |
| 2024-01-15 | support_ticket | 1281 | 0.0 | 0.0 | 459 | 358 | 38.79 | up |

## Alerts

| rule | kind | metric | group | period | measured_value | detail |
|---|---|---|---|---|---|---|
| high_weekly_purchase_volume | threshold | count | purchase | 2024-01-15 | 2558 | threshold=2200 (gt) |

_Alerts are threshold/statistical flags for human review, not root-cause diagnoses. Outlier detection is a trailing-window mean/stdev heuristic, not a trained model._
