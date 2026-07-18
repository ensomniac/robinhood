# Independent Early Item 2.02 Reversal Confirmation

Result ID: `strategy-confirmation-bce91ee77020-7d4989dc02e6`

## Evidence Boundary

- Preregistered manifest: `bce91ee770205ee7b6c25e976fb70547f64fe6fdbde16ae1722c3cbf400f627a`
- Requested dates: 100
- Validation-grade dates: 31
- Missing dates retained as blockers: 69
- Prior inspected-date overlap: 0
- Policies executed: 1
- Provider and broker actions during evaluation: 0
- Production strategy changes: 0

## Primary 5 bps / 2R Result

- Trades: 22
- Mean R: -0.457
- Total R: -10.056
- Profit factor: 0.370
- Maximum drawdown: 10.492R
- One-sided 90% bootstrap lower mean R: -0.715

## Acceptance

Overall: `insufficient_independent_evidence`

| Gate | Pass | Actual | Required |
|---|---|---|---|
| requested_dates | yes | 100 | >=100 |
| validation_grade_dates | no | 31 | >=80 |
| executed_signals | no | 22 | >=50 |
| primary_expectancy | no | -0.4570774 | >0 |
| primary_profit_factor | no | 0.36994848 | >=1.3 |
| primary_drawdown | no | 10.49178513 | <=6.0 |
| bootstrap_lower_mean_r | no | -0.71528591 | >0 |
| chronological_halves | no | see JSON | positive total R in both halves |
| without_best_five | no | -15.53015271 | >0 total R |
| cost_stress_10bps | no | see JSON | positive total R, PF>=1.20, drawdown<=6R |
| cost_stress_20bps | no | see JSON | positive total R, PF>=1.20, drawdown<=6R |
| all_frozen_targets | no | see JSON | positive total R at 1R, 1.5R, 2R, and 3R |

## Risk-Sized Deployment Geometry

- Structural-arm trades: 22
- Compounded account return: -2.3%
- Expected log growth per trade: -0.001
- Peak-to-trough account drawdown: 2.4%
- Median allocation: 14.8%
- P90 allocation: 33.7%
- Structural-stop compressions: 0
- Risk-cap violations: 0
- Naturally <=0.8% cohort: 6 trades; inference sufficient: no

Decision: `stop_without_threshold_tuning`.

A failed gate stops advancement and does not authorize tuning on this sample. Passing advances only the unchanged structural-stop, risk-sized arm to prospective shadow qualification.
