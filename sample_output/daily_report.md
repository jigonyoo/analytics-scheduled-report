# Daily Analytics Report

Window: `2024-01-15`

This report is generated from event/transaction data only. Figures below are measured directly from the input log; anything under 'Alerts' is a rule- or statistics-based flag for a human to review, not a diagnosis of cause.

## Metrics by group

| period | group | count | sum_amount | avg_amount | distinct_users | count_delta | count_pct_change | count_direction |
|---|---|---|---|---|---|---|---|---|
| 2024-01-15 | purchase | 949 | 49642.44 | 52.31 | 417 | 783 | 471.69 | up |
| 2024-01-15 | refund | 107 | -3631.57 | -33.94 | 99 | 84 | 365.22 | up |
| 2024-01-15 | signup | 382 | 0.0 | 0.0 | 260 | 304 | 389.74 | up |
| 2024-01-15 | support_ticket | 500 | 0.0 | 0.0 | 324 | 405 | 426.32 | up |

## Alerts

| rule | kind | metric | group | period | measured_value | detail |
|---|---|---|---|---|---|---|
| high_daily_purchase_volume | threshold | count | purchase | 2024-01-15 | 949 | threshold=700 (gt) |
| trailing_outlier_count | statistical_outlier | count | purchase | 2024-01-15 | 949 | trailing_mean=273.71 trailing_stdev=66.14 z=10.21 |

_Alerts are threshold/statistical flags for human review, not root-cause diagnoses. Outlier detection is a trailing-window mean/stdev heuristic, not a trained model._
